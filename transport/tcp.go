package transport

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/darkinno-tech/jtt-808-go-sdk/core"
	"github.com/darkinno-tech/jtt-808-go-sdk/protocol"
)

// TCPServer TCP服务器
type TCPServer struct {
	listener    net.Listener
	codec       *protocol.Codec
	connections sync.Map
	rawConns    sync.Map // 追踪所有原始TCP连接，用于Stop()时强制关闭
	handlerMu   sync.RWMutex
	handlers    map[uint16]core.MessageHandler
	middleware  []core.Middleware
	hooks       *Hooks
	stats       *Stats
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	config      *Config
}

// Config 服务器配置
type Config struct {
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
	// MaxPacketSize 最大包长（字节），防止异常包导致内存增长
	MaxPacketSize int
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		ListenAddr:      ":8080",
		MaxConnections:  1000000,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		IdleTimeout:     300 * time.Second,
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		MaxPacketSize:   4096,
	}
}

// Hooks 钩子函数
type Hooks struct {
	OnConnect    []func(conn core.Connection) error
	OnDisconnect []func(conn core.Connection) error
	OnMessage    []func(conn core.Connection, msg *protocol.Message) error
	OnError      []func(conn core.Connection, err error)
}

// Stats 统计信息
type Stats struct {
	ActiveConnections int64
	TotalConnections  int64
	ReceivedMessages  int64
	SentMessages      int64
	ErrorCount        int64
	StartTime         time.Time
}

// NewTCPServer 创建TCP服务器
func NewTCPServer(config *Config) *TCPServer {
	if config == nil {
		config = DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &TCPServer{
		codec:    protocol.NewCodec(),
		handlers: make(map[uint16]core.MessageHandler),
		hooks:    &Hooks{},
		stats:    &Stats{StartTime: time.Now()},
		ctx:      ctx,
		cancel:   cancel,
		config:   config,
	}
}

// Start 启动服务器
func (s *TCPServer) Start() error {
	listener, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = listener

	s.wg.Add(1)
	go s.accept()

	return nil
}

// Stop 停止服务器
func (s *TCPServer) Stop() error {
	s.cancel()
	if s.listener != nil {
		s.listener.Close()
	}

	// 先关闭所有原始TCP连接（包括未注册的），让handleConn goroutine退出
	s.rawConns.Range(func(key, value interface{}) bool {
		if conn, ok := value.(net.Conn); ok {
			conn.Close()
		}
		return true
	})

	// 关闭已注册的连接
	s.connections.Range(func(key, value interface{}) bool {
		if conn, ok := value.(*TCPConnection); ok {
			conn.Close()
		}
		return true
	})

	s.wg.Wait()
	return nil
}

// RegisterHandler 注册消息处理器
func (s *TCPServer) RegisterHandler(msgID uint16, handler core.MessageHandler) {
	s.handlerMu.Lock()
	s.handlers[msgID] = handler
	s.handlerMu.Unlock()
}

// GetConnection 获取连接
func (s *TCPServer) GetConnection(deviceID string) (core.Connection, error) {
	if conn, ok := s.connections.Load(deviceID); ok {
		return conn.(*TCPConnection), nil
	}
	return nil, fmt.Errorf("connection not found: %s", deviceID)
}

// GetStats 获取统计信息
func (s *TCPServer) GetStats() core.ServerStats {
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

// Use 添加中间件
func (s *TCPServer) Use(middleware core.Middleware) {
	s.middleware = append(s.middleware, middleware)
}

// OnConnect 注册连接建立钩子
func (s *TCPServer) OnConnect(hook func(conn core.Connection) error) {
	s.hooks.OnConnect = append(s.hooks.OnConnect, hook)
}

// OnDisconnect 注册连接断开钩子
func (s *TCPServer) OnDisconnect(hook func(conn core.Connection) error) {
	s.hooks.OnDisconnect = append(s.hooks.OnDisconnect, hook)
}

// OnError 注册错误处理钩子
func (s *TCPServer) OnError(hook func(conn core.Connection, err error)) {
	s.hooks.OnError = append(s.hooks.OnError, hook)
}

// accept 接受连接
func (s *TCPServer) accept() {
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

			s.wg.Add(1)
			go s.handleConn(conn)
		}
	}
}

// handleConn 处理连接
func (s *TCPServer) handleConn(netConn net.Conn) {
	defer s.wg.Done()

	// 追踪原始连接，用于Stop()时强制关闭
	connID := fmt.Sprintf("%p", netConn)
	s.rawConns.Store(connID, netConn)
	defer s.rawConns.Delete(connID)

	conn := NewTCPConnection(netConn, s)
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
			s.connections.CompareAndDelete(deviceID, conn)
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

			// 根据消息ID路由到对应处理器
			s.handlerMu.RLock()
			handler, exists := s.handlers[msg.Header.MsgID]
			if !exists {
				// 如果没有注册处理器，使用默认处理器（如果有）
				for _, h := range s.handlers {
					handler = h
					break
				}
			}
			s.handlerMu.RUnlock()

			if handler != nil {
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
		}
	}
}

// readMessage 读取消息
func (s *TCPServer) readMessage(reader *bufio.Reader) (*protocol.Message, error) {
	// 读取标志位
	flag, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if flag != 0x7E {
		return nil, fmt.Errorf("invalid flag: %x", flag)
	}

	// 读取消息内容直到结束标志位（带最大包长保护）
	var data []byte
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
		if len(data) >= maxSize {
			return nil, fmt.Errorf("packet exceeds max size %d bytes", maxSize)
		}
		data = append(data, b)
	}

	// 构造完整消息
	fullData := make([]byte, 0, len(data)+2)
	fullData = append(fullData, 0x7E)
	fullData = append(fullData, data...)
	fullData = append(fullData, 0x7E)

	// 解码消息
	return s.codec.Decode(fullData)
}

// TCPConnection TCP连接
type TCPConnection struct {
	netConn    net.Conn
	server     *TCPServer
	deviceID   string
	connected  int32
	lastActive time.Time
	mu         sync.RWMutex
	attributes sync.Map
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewTCPConnection 创建TCP连接
func NewTCPConnection(netConn net.Conn, server *TCPServer) *TCPConnection {
	ctx, cancel := context.WithCancel(context.Background())
	return &TCPConnection{
		netConn:    netConn,
		server:     server,
		connected:  1,
		lastActive: time.Now(),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Send 发送消息
func (c *TCPConnection) Send(msg *core.Message) error {
	if atomic.LoadInt32(&c.connected) == 0 {
		return fmt.Errorf("connection closed")
	}

	data, err := c.server.codec.Encode(msg)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 设置写超时
	if c.server.config.WriteTimeout > 0 {
		c.netConn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}

	_, err = c.netConn.Write(data)
	if err != nil {
		return err
	}

	atomic.AddInt64(&c.server.stats.SentMessages, 1)
	c.lastActive = time.Now()
	return nil
}

// Close 关闭连接
func (c *TCPConnection) Close() error {
	if atomic.CompareAndSwapInt32(&c.connected, 1, 0) {
		c.cancel()
		return c.netConn.Close()
	}
	return nil
}

// DeviceID 获取设备ID
func (c *TCPConnection) DeviceID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.deviceID
}

// IsConnected 是否已连接
func (c *TCPConnection) IsConnected() bool {
	return atomic.LoadInt32(&c.connected) == 1
}

// RemoteAddr 获取远程地址
func (c *TCPConnection) RemoteAddr() net.Addr {
	return c.netConn.RemoteAddr()
}

// Set 设置连接属性
func (c *TCPConnection) Set(key string, value interface{}) {
	c.attributes.Store(key, value)
}

// Get 获取连接属性
func (c *TCPConnection) Get(key string) (interface{}, bool) {
	return c.attributes.Load(key)
}

// LastActiveTime 获取最后活跃时间
func (c *TCPConnection) LastActiveTime() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastActive
}

// Context 获取连接上下文
func (c *TCPConnection) Context() context.Context {
	return c.ctx
}

// SetDeviceID 设置设备ID
func (c *TCPConnection) SetDeviceID(deviceID string) {
	c.mu.Lock()
	oldDeviceID := c.deviceID
	c.deviceID = deviceID
	c.mu.Unlock()

	if oldDeviceID != "" && oldDeviceID != deviceID {
		c.server.connections.CompareAndDelete(oldDeviceID, c)
	}
	if deviceID != "" {
		c.server.connections.Store(deviceID, c)
	}
}
