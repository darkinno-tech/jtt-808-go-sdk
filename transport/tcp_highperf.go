package transport

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/im10furry/jtt-808-go-sdk/core"
	"github.com/im10furry/jtt-808-go-sdk/protocol"
)

const (
	// 默认Worker Pool大小
	defaultMinWorkers = 1000
	defaultMaxWorkers = 50000
	defaultQueueSize  = 1000000

	// 连接池分片数
	defaultShardCount = 256

	// 读取缓冲区大小
	defaultReadBufferSize = 8192

	// 消息缓冲池大小
	msgBufferPoolSize = 32768
)

// HighPerfTCPServer 高性能TCP服务器
type HighPerfTCPServer struct {
	listener   net.Listener
	codec      *protocol.Codec
	connPool   *ShardedConnectionPool
	handlerMap *HandlerMap
	middleware []core.Middleware
	hooks      *Hooks
	stats      *HighPerfStats
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	config     *HighPerfConfig
	workerPool *WorkerPool
	msgPool    sync.Pool
	bufferPool sync.Pool
}

// HighPerfConfig 高性能服务器配置
type HighPerfConfig struct {
	// ListenAddr 监听地址
	ListenAddr string
	// MaxConnections 最大连接数
	MaxConnections int
	// ReadTimeout 读超时
	ReadTimeout time.Duration
	// WriteTimeout 写超时
	WriteTimeout time.Duration
	// IdleTimeout 空闲超时
	IdleTimeout time.Duration
	// ReadBufferSize 读缓冲区大小
	ReadBufferSize int
	// WriteBufferSize 写缓冲区大小
	WriteBufferSize int
	// MaxPacketSize 最大包长
	MaxPacketSize int
	// MinWorkers 最小Worker数
	MinWorkers int
	// MaxWorkers 最大Worker数
	MaxWorkers int
	// WorkerQueueSize Worker队列大小
	WorkerQueueSize int
	// ConnPoolShardCount 连接池分片数
	ConnPoolShardCount int
	// AcceptParallel 并行Accept数量
	AcceptParallel int
	// EnableTCPNoDelay 启用TCP_NODELAY
	EnableTCPNoDelay bool
	// EnableTCPKeepAlive 启用TCP KeepAlive
	EnableTCPKeepAlive bool
	// TCPKeepAliveInterval TCP KeepAlive间隔
	TCPKeepAliveInterval time.Duration
}

// HighPerfStats 高性能统计信息
type HighPerfStats struct {
	ActiveConnections int64
	TotalConnections  int64
	ReceivedMessages  int64
	SentMessages      int64
	ErrorCount        int64
	WorkerPoolStats   WorkerPoolStats
	StartTime         time.Time
}

// ShardedConnectionPool 分片连接池
type ShardedConnectionPool struct {
	shards     []*connPoolShard
	shardCount int
	totalCount int64
}

type connPoolShard struct {
	mu    sync.RWMutex
	conns map[string]*HighPerfTCPConnection
	count int
}

// HandlerMap 消息处理器映射
type HandlerMap struct {
	mu       sync.RWMutex
	handlers map[uint16]core.MessageHandler
}

// WorkerPool Worker池
type WorkerPool struct {
	taskQueue  chan func()
	workers    int32
	minWorkers int32
	maxWorkers int32
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	active     int64
}

// WorkerPoolStats Worker池统计
type WorkerPoolStats struct {
	ActiveWorkers int64
	TotalWorkers  int32
	QueueLength   int
	QueueCapacity int
}

// DefaultHighPerfConfig 默认高性能配置
func DefaultHighPerfConfig() *HighPerfConfig {
	return &HighPerfConfig{
		ListenAddr:           ":8080",
		MaxConnections:       200000,
		ReadTimeout:          60 * time.Second,
		WriteTimeout:         60 * time.Second,
		IdleTimeout:          300 * time.Second,
		ReadBufferSize:       defaultReadBufferSize,
		WriteBufferSize:      8192,
		MaxPacketSize:        4096,
		MinWorkers:           defaultMinWorkers,
		MaxWorkers:           defaultMaxWorkers,
		WorkerQueueSize:      defaultQueueSize,
		ConnPoolShardCount:   defaultShardCount,
		AcceptParallel:       runtime.NumCPU(),
		EnableTCPNoDelay:     true,
		EnableTCPKeepAlive:   true,
		TCPKeepAliveInterval: 30 * time.Second,
	}
}

// NewHighPerfTCPServer 创建高性能TCP服务器
func NewHighPerfTCPServer(config *HighPerfConfig) *HighPerfTCPServer {
	if config == nil {
		config = DefaultHighPerfConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	server := &HighPerfTCPServer{
		codec:    protocol.NewCodec(),
		connPool: NewShardedConnectionPool(config.ConnPoolShardCount),
		handlerMap: &HandlerMap{
			handlers: make(map[uint16]core.MessageHandler),
		},
		hooks:  &Hooks{},
		stats:  &HighPerfStats{StartTime: time.Now()},
		ctx:    ctx,
		cancel: cancel,
		config: config,
	}

	// 创建Worker Pool
	server.workerPool = NewWorkerPool(ctx, config.MinWorkers, config.MaxWorkers, config.WorkerQueueSize)

	// 初始化消息缓冲池
	server.msgPool = sync.Pool{
		New: func() interface{} {
			return &protocol.Message{
				Header: &protocol.MessageHeader{},
			}
		},
	}

	// 初始化字节缓冲池
	server.bufferPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 0, config.ReadBufferSize)
			return &buf
		},
	}

	return server
}

// NewShardedConnectionPool 创建分片连接池
func NewShardedConnectionPool(shardCount int) *ShardedConnectionPool {
	if shardCount <= 0 {
		shardCount = defaultShardCount
	}

	pool := &ShardedConnectionPool{
		shards:     make([]*connPoolShard, shardCount),
		shardCount: shardCount,
	}

	for i := 0; i < shardCount; i++ {
		pool.shards[i] = &connPoolShard{
			conns: make(map[string]*HighPerfTCPConnection),
		}
	}

	return pool
}

// getShard 获取分片
func (p *ShardedConnectionPool) getShard(key string) *connPoolShard {
	hash := fnvHash(key)
	return p.shards[uint32(hash)%uint32(p.shardCount)]
}

// Put 添加连接
func (p *ShardedConnectionPool) Put(conn *HighPerfTCPConnection) {
	shard := p.getShard(conn.DeviceID())
	shard.mu.Lock()
	if _, exists := shard.conns[conn.DeviceID()]; !exists {
		shard.count++
		atomic.AddInt64(&p.totalCount, 1)
	}
	shard.conns[conn.DeviceID()] = conn
	shard.mu.Unlock()
}

// Get 获取连接
func (p *ShardedConnectionPool) Get(deviceID string) (*HighPerfTCPConnection, bool) {
	shard := p.getShard(deviceID)
	shard.mu.RLock()
	conn, exists := shard.conns[deviceID]
	shard.mu.RUnlock()
	return conn, exists
}

// Delete 删除连接
func (p *ShardedConnectionPool) Delete(deviceID string) {
	shard := p.getShard(deviceID)
	shard.mu.Lock()
	if _, exists := shard.conns[deviceID]; exists {
		delete(shard.conns, deviceID)
		shard.count--
		atomic.AddInt64(&p.totalCount, -1)
	}
	shard.mu.Unlock()
}

// DeleteConn 删除指定连接，避免旧连接断开时误删同设备的新连接。
func (p *ShardedConnectionPool) DeleteConn(deviceID string, conn *HighPerfTCPConnection) {
	shard := p.getShard(deviceID)
	shard.mu.Lock()
	if current, exists := shard.conns[deviceID]; exists && current == conn {
		delete(shard.conns, deviceID)
		shard.count--
		atomic.AddInt64(&p.totalCount, -1)
	}
	shard.mu.Unlock()
}

// Size 获取连接数
func (p *ShardedConnectionPool) Size() int64 {
	return atomic.LoadInt64(&p.totalCount)
}

// Range 遍历连接
func (p *ShardedConnectionPool) Range(fn func(deviceID string, conn *HighPerfTCPConnection) bool) {
	for _, shard := range p.shards {
		shard.mu.RLock()
		for deviceID, conn := range shard.conns {
			if !fn(deviceID, conn) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

// NewWorkerPool 创建Worker池
func NewWorkerPool(ctx context.Context, minWorkers, maxWorkers, queueSize int) *WorkerPool {
	pool := &WorkerPool{
		taskQueue:  make(chan func(), queueSize),
		minWorkers: int32(minWorkers),
		maxWorkers: int32(maxWorkers),
		ctx:        ctx,
	}

	poolCtx, poolCancel := context.WithCancel(ctx)
	pool.ctx = poolCtx
	pool.cancel = poolCancel

	// 启动最小数量的Worker
	for i := 0; i < minWorkers; i++ {
		pool.spawnWorker()
	}

	// 启动动态调整协程
	go pool.adjustLoop()

	return pool
}

// spawnWorker 创建新Worker
func (p *WorkerPool) spawnWorker() {
	atomic.AddInt32(&p.workers, 1)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer atomic.AddInt32(&p.workers, -1)

		for {
			select {
			case <-p.ctx.Done():
				return
			case task, ok := <-p.taskQueue:
				if !ok {
					return
				}
				if task == nil {
					return // nil任务是缩减信号
				}
				atomic.AddInt64(&p.active, 1)
				task()
				atomic.AddInt64(&p.active, -1)
			}
		}
	}()
}

// Submit 提交任务
func (p *WorkerPool) Submit(task func()) bool {
	select {
	case <-p.ctx.Done():
		return false
	default:
	}

	select {
	case p.taskQueue <- task:
		// 任务已提交
		return true
	case <-p.ctx.Done():
		return false
	default:
		// 队列满，创建新Worker
		if atomic.LoadInt32(&p.workers) < p.maxWorkers {
			p.spawnWorker()
		}
		// 阻塞等待
		select {
		case p.taskQueue <- task:
			return true
		case <-p.ctx.Done():
			return false
		}
	}
}

// adjustLoop 动态调整Worker数量
func (p *WorkerPool) adjustLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.adjust()
		}
	}
}

// adjust 调整Worker数量
func (p *WorkerPool) adjust() {
	queueLen := len(p.taskQueue)
	queueCap := cap(p.taskQueue)
	currentWorkers := atomic.LoadInt32(&p.workers)
	activeWorkers := atomic.LoadInt64(&p.active)

	// 队列使用率超过80%，增加Worker
	usage := float64(queueLen) / float64(queueCap)
	if usage > 0.8 && currentWorkers < p.maxWorkers {
		newWorkers := min(currentWorkers*2, p.maxWorkers)
		for i := currentWorkers; i < newWorkers; i++ {
			p.spawnWorker()
		}
	}

	// 队列使用率低于20%且活跃Worker少，减少Worker
	if usage < 0.2 && currentWorkers > p.minWorkers && activeWorkers < int64(currentWorkers/2) {
		// 通过发送nil任务来停止多余Worker
		for i := currentWorkers; i > p.minWorkers; i-- {
			select {
			case p.taskQueue <- nil:
			default:
				return
			}
		}
	}
}

// Stats 获取统计信息
func (p *WorkerPool) Stats() WorkerPoolStats {
	return WorkerPoolStats{
		ActiveWorkers: atomic.LoadInt64(&p.active),
		TotalWorkers:  atomic.LoadInt32(&p.workers),
		QueueLength:   len(p.taskQueue),
		QueueCapacity: cap(p.taskQueue),
	}
}

// Stop 停止Worker池
func (p *WorkerPool) Stop() {
	p.cancel()
	p.wg.Wait()
}

// GetHandler 获取消息处理器
func (m *HandlerMap) Get(msgID uint16) (core.MessageHandler, bool) {
	m.mu.RLock()
	handler, exists := m.handlers[msgID]
	m.mu.RUnlock()
	return handler, exists
}

// Set 设置消息处理器
func (m *HandlerMap) Set(msgID uint16, handler core.MessageHandler) {
	m.mu.Lock()
	m.handlers[msgID] = handler
	m.mu.Unlock()
}

// Start 启动服务器
func (s *HighPerfTCPServer) Start() error {
	listener, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = listener

	// 启动多个Accept协程
	for i := 0; i < s.config.AcceptParallel; i++ {
		s.wg.Add(1)
		go s.accept()
	}

	return nil
}

// Stop 停止服务器
func (s *HighPerfTCPServer) Stop() error {
	s.cancel()
	if s.listener != nil {
		s.listener.Close()
	}

	// 关闭所有连接
	s.connPool.Range(func(deviceID string, conn *HighPerfTCPConnection) bool {
		conn.Close()
		return true
	})

	// 停止Worker池
	s.workerPool.Stop()

	s.wg.Wait()
	return nil
}

// RegisterHandler 注册消息处理器
func (s *HighPerfTCPServer) RegisterHandler(msgID uint16, handler core.MessageHandler) {
	s.handlerMap.Set(msgID, handler)
}

// GetConnection 获取连接
func (s *HighPerfTCPServer) GetConnection(deviceID string) (core.Connection, error) {
	conn, exists := s.connPool.Get(deviceID)
	if !exists {
		return nil, fmt.Errorf("connection not found: %s", deviceID)
	}
	return conn, nil
}

// GetStats 获取统计信息
func (s *HighPerfTCPServer) GetStats() core.ServerStats {
	return core.ServerStats{
		ActiveConnections: atomic.LoadInt64(&s.stats.ActiveConnections),
		TotalConnections:  atomic.LoadInt64(&s.stats.TotalConnections),
		ReceivedMessages:  atomic.LoadInt64(&s.stats.ReceivedMessages),
		SentMessages:      atomic.LoadInt64(&s.stats.SentMessages),
		ErrorCount:        atomic.LoadInt64(&s.stats.ErrorCount),
		StartTime:         s.stats.StartTime,
		Uptime:            time.Since(s.stats.StartTime),
	}
}

// GetHighPerfStats 获取高性能统计信息
func (s *HighPerfTCPServer) GetHighPerfStats() HighPerfStats {
	stats := *s.stats
	stats.WorkerPoolStats = s.workerPool.Stats()
	return stats
}

// Use 添加中间件
func (s *HighPerfTCPServer) Use(middleware core.Middleware) {
	s.middleware = append(s.middleware, middleware)
}

// OnConnect 注册连接建立钩子
func (s *HighPerfTCPServer) OnConnect(hook func(conn core.Connection) error) {
	s.hooks.OnConnect = append(s.hooks.OnConnect, hook)
}

// OnDisconnect 注册连接断开钩子
func (s *HighPerfTCPServer) OnDisconnect(hook func(conn core.Connection) error) {
	s.hooks.OnDisconnect = append(s.hooks.OnDisconnect, hook)
}

// OnError 注册错误处理钩子
func (s *HighPerfTCPServer) OnError(hook func(conn core.Connection, err error)) {
	s.hooks.OnError = append(s.hooks.OnError, hook)
}

// accept 接受连接
func (s *HighPerfTCPServer) accept() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.ctx.Done():
					return
				default:
					atomic.AddInt64(&s.stats.ErrorCount, 1)
					continue
				}
			}

			// 检查连接数限制
			if s.config.MaxConnections > 0 &&
				atomic.LoadInt64(&s.stats.ActiveConnections) >= int64(s.config.MaxConnections) {
				conn.Close()
				continue
			}

			// 设置TCP参数
			if tcpConn, ok := conn.(*net.TCPConn); ok {
				if s.config.EnableTCPNoDelay {
					tcpConn.SetNoDelay(true)
				}
				if s.config.EnableTCPKeepAlive {
					tcpConn.SetKeepAlive(true)
					tcpConn.SetKeepAlivePeriod(s.config.TCPKeepAliveInterval)
				}
				// 设置读写缓冲区
				tcpConn.SetReadBuffer(s.config.ReadBufferSize)
				tcpConn.SetWriteBuffer(s.config.WriteBufferSize)
			}

			s.wg.Add(1)
			go s.handleConn(conn)
		}
	}
}

// handleConn 处理连接
func (s *HighPerfTCPServer) handleConn(netConn net.Conn) {
	defer s.wg.Done()

	conn := NewHighPerfTCPConnection(netConn, s)
	atomic.AddInt64(&s.stats.ActiveConnections, 1)
	atomic.AddInt64(&s.stats.TotalConnections, 1)

	// 执行连接建立钩子
	for _, hook := range s.hooks.OnConnect {
		if err := hook(conn); err != nil {
			atomic.AddInt64(&s.stats.ErrorCount, 1)
			conn.Close()
			atomic.AddInt64(&s.stats.ActiveConnections, -1)
			return
		}
	}

	defer func() {
		// 执行连接断开钩子
		for _, hook := range s.hooks.OnDisconnect {
			hook(conn)
		}
		if deviceID := conn.DeviceID(); deviceID != "" {
			s.connPool.DeleteConn(deviceID, conn)
		}
		conn.Close()
		atomic.AddInt64(&s.stats.ActiveConnections, -1)
	}()

	// 读取消息
	reader := bufio.NewReaderSize(netConn, s.config.ReadBufferSize)
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			// 设置读超时
			if s.config.ReadTimeout > 0 {
				netConn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
			}

			// 读取消息
			msg, err := s.readMessage(reader)
			if err != nil {
				if err != io.EOF {
					atomic.AddInt64(&s.stats.ErrorCount, 1)
					// 执行错误钩子
					for _, hook := range s.hooks.OnError {
						hook(conn, err)
					}
				}
				return
			}

			atomic.AddInt64(&s.stats.ReceivedMessages, 1)

			// 使用Worker池处理消息
			if ok := s.workerPool.Submit(func() {
				s.processMessage(conn, msg)
			}); !ok {
				return
			}
		}
	}
}

// processMessage 处理消息
func (s *HighPerfTCPServer) processMessage(conn *HighPerfTCPConnection, msg *protocol.Message) {
	// 根据消息ID路由到对应处理器
	handler, exists := s.handlerMap.Get(msg.Header.MsgID)
	if !exists {
		return
	}

	// 应用中间件
	for i := len(s.middleware) - 1; i >= 0; i-- {
		handler = s.middleware[i](handler)
	}

	ctx := context.Background()
	if err := handler(ctx, conn, msg); err != nil {
		atomic.AddInt64(&s.stats.ErrorCount, 1)
		// 执行错误钩子
		for _, hook := range s.hooks.OnError {
			hook(conn, err)
		}
	}
}

// readMessage 读取消息（优化版本）
func (s *HighPerfTCPServer) readMessage(reader *bufio.Reader) (*protocol.Message, error) {
	// 读取标志位
	flag, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if flag != 0x7E {
		return nil, fmt.Errorf("invalid flag: %x", flag)
	}

	// 使用预分配缓冲区
	bufPtr := s.bufferPool.Get().(*[]byte)
	buf := *bufPtr
	buf = buf[:0]
	defer func() {
		*bufPtr = buf
		s.bufferPool.Put(bufPtr)
	}()

	// 读取消息内容直到结束标志位
	maxSize := s.config.MaxPacketSize
	if maxSize <= 0 {
		maxSize = 4096
	}

	for {
		b, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == 0x7E {
			break
		}
		if len(buf) >= maxSize {
			return nil, fmt.Errorf("packet exceeds max size %d bytes", maxSize)
		}
		buf = append(buf, b)
	}

	// 构造完整消息
	fullData := make([]byte, 0, len(buf)+2)
	fullData = append(fullData, 0x7E)
	fullData = append(fullData, buf...)
	fullData = append(fullData, 0x7E)

	// 解码消息
	return s.codec.Decode(fullData)
}

// HighPerfTCPConnection 高性能TCP连接
type HighPerfTCPConnection struct {
	netConn    net.Conn
	server     *HighPerfTCPServer
	deviceID   string
	connected  int32
	lastActive int64
	mu         sync.RWMutex
	closeMu    sync.RWMutex
	attributes sync.Map
	ctx        context.Context
	cancel     context.CancelFunc
	writeChan  chan []byte
}

// NewHighPerfTCPConnection 创建高性能TCP连接
func NewHighPerfTCPConnection(netConn net.Conn, server *HighPerfTCPServer) *HighPerfTCPConnection {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &HighPerfTCPConnection{
		netConn:    netConn,
		server:     server,
		connected:  1,
		lastActive: time.Now().UnixNano(),
		ctx:        ctx,
		cancel:     cancel,
		writeChan:  make(chan []byte, 100),
	}

	// 启动写协程
	go conn.writeLoop()

	return conn
}

// writeLoop 写循环
func (c *HighPerfTCPConnection) writeLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case data, ok := <-c.writeChan:
			if !ok {
				return
			}
			c.mu.Lock()
			if c.server.config.WriteTimeout > 0 {
				c.netConn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
			}
			_, err := c.netConn.Write(data)
			c.mu.Unlock()
			if err != nil {
				if atomic.CompareAndSwapInt32(&c.connected, 1, 0) {
					c.closeMu.Lock()
					close(c.writeChan)
					c.closeMu.Unlock()
					c.cancel()
					c.netConn.Close()
				}
				atomic.AddInt64(&c.server.stats.ErrorCount, 1)
				return
			}
			atomic.AddInt64(&c.server.stats.SentMessages, 1)
			atomic.StoreInt64(&c.lastActive, time.Now().UnixNano())
		}
	}
}

// Send 发送消息
func (c *HighPerfTCPConnection) Send(msg *core.Message) error {
	if atomic.LoadInt32(&c.connected) == 0 {
		return fmt.Errorf("connection closed")
	}

	data, err := c.server.codec.Encode(msg)
	if err != nil {
		return err
	}

	c.closeMu.RLock()
	if atomic.LoadInt32(&c.connected) == 0 {
		c.closeMu.RUnlock()
		return fmt.Errorf("connection closed")
	}

	// 异步写入
	select {
	case c.writeChan <- data:
		c.closeMu.RUnlock()
		return nil
	default:
		c.closeMu.RUnlock()
		// 写入通道满，同步写入
		c.mu.Lock()
		defer c.mu.Unlock()

		if c.server.config.WriteTimeout > 0 {
			c.netConn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
		}

		_, err = c.netConn.Write(data)
		if err != nil {
			return err
		}

		atomic.AddInt64(&c.server.stats.SentMessages, 1)
		atomic.StoreInt64(&c.lastActive, time.Now().UnixNano())
		return nil
	}
}

// Close 关闭连接
func (c *HighPerfTCPConnection) Close() error {
	if atomic.CompareAndSwapInt32(&c.connected, 1, 0) {
		c.closeMu.Lock()
		close(c.writeChan)
		c.closeMu.Unlock()
		c.cancel()
		return c.netConn.Close()
	}
	return nil
}

// DeviceID 获取设备ID
func (c *HighPerfTCPConnection) DeviceID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.deviceID
}

// IsConnected 是否已连接
func (c *HighPerfTCPConnection) IsConnected() bool {
	return atomic.LoadInt32(&c.connected) == 1
}

// RemoteAddr 获取远程地址
func (c *HighPerfTCPConnection) RemoteAddr() net.Addr {
	return c.netConn.RemoteAddr()
}

// Set 设置连接属性
func (c *HighPerfTCPConnection) Set(key string, value interface{}) {
	c.attributes.Store(key, value)
}

// Get 获取连接属性
func (c *HighPerfTCPConnection) Get(key string) (interface{}, bool) {
	return c.attributes.Load(key)
}

// LastActiveTime 获取最后活跃时间
func (c *HighPerfTCPConnection) LastActiveTime() time.Time {
	nanos := atomic.LoadInt64(&c.lastActive)
	return time.Unix(0, nanos)
}

// Context 获取连接上下文
func (c *HighPerfTCPConnection) Context() context.Context {
	return c.ctx
}

// SetDeviceID 设置设备ID
func (c *HighPerfTCPConnection) SetDeviceID(deviceID string) {
	c.mu.Lock()
	oldDeviceID := c.deviceID
	c.deviceID = deviceID
	c.mu.Unlock()

	if oldDeviceID != "" && oldDeviceID != deviceID {
		c.server.connPool.DeleteConn(oldDeviceID, c)
	}
	if deviceID != "" {
		c.server.connPool.Put(c)
	}
}

// fnvHash FNV哈希
func fnvHash(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= 16777619
		hash ^= uint32(key[i])
	}
	return hash
}

// min 获取最小值
func min(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}
