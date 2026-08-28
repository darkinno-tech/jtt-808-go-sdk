package unit

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/darkinno-tech/jtt-808-go-sdk/core"
	"github.com/darkinno-tech/jtt-808-go-sdk/protocol"
	"github.com/darkinno-tech/jtt-808-go-sdk/transport"
)

// TestTCPServerCreation 测试TCP服务器创建
func TestTCPServerCreation(t *testing.T) {
	t.Run("DefaultConfig", func(t *testing.T) {
		server := transport.NewTCPServer(nil)
		if server == nil {
			t.Fatal("Failed to create server with default config")
		}
	})

	t.Run("CustomConfig", func(t *testing.T) {
		config := &transport.Config{
			ListenAddr:     ":9090",
			MaxConnections: 100,
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
		}
		server := transport.NewTCPServer(config)
		if server == nil {
			t.Fatal("Failed to create server with custom config")
		}
	})
}

// TestTCPServerStartStop 测试服务器启动和停止
func TestTCPServerStartStop(t *testing.T) {
	t.Run("StartAndStop", func(t *testing.T) {
		config := &transport.Config{
			ListenAddr: ":0", // 使用随机端口
		}
		server := transport.NewTCPServer(config)

		// 启动服务器
		err := server.Start()
		if err != nil {
			t.Fatalf("Failed to start server: %v", err)
		}

		// 等待服务器启动
		time.Sleep(100 * time.Millisecond)

		// 停止服务器
		err = server.Stop()
		if err != nil {
			t.Fatalf("Failed to stop server: %v", err)
		}
	})

	t.Run("MultipleStartStop", func(t *testing.T) {
		config := &transport.Config{
			ListenAddr: ":0",
		}
		server := transport.NewTCPServer(config)

		// 多次启动停止
		for i := 0; i < 3; i++ {
			err := server.Start()
			if err != nil {
				t.Fatalf("Failed to start server on iteration %d: %v", i, err)
			}

			time.Sleep(50 * time.Millisecond)

			err = server.Stop()
			if err != nil {
				t.Fatalf("Failed to stop server on iteration %d: %v", i, err)
			}
		}
	})
}

// TestTCPServerHandler 测试消息处理器注册
func TestTCPServerHandler(t *testing.T) {
	server := transport.NewTCPServer(nil)

	// 测试注册处理器
	handlerCalled := false
	server.RegisterHandler(0x0001, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		handlerCalled = true
		return nil
	})

	// 测试注册多个处理器
	server.RegisterHandler(0x0002, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		return nil
	})

	// 测试覆盖处理器
	server.RegisterHandler(0x0001, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		return nil
	})

	_ = handlerCalled // 避免未使用变量警告
}

// TestTCPServerHooks 测试钩子函数
func TestTCPServerHooks(t *testing.T) {
	server := transport.NewTCPServer(nil)

	// 测试连接钩子
	server.OnConnect(func(conn core.Connection) error {
		return nil
	})

	// 测试断开连接钩子
	server.OnDisconnect(func(conn core.Connection) error {
		return nil
	})

	// 测试错误钩子
	server.OnError(func(conn core.Connection, err error) {
		// 错误处理
	})
}

// TestTCPServerMiddleware 测试中间件
func TestTCPServerMiddleware(t *testing.T) {
	server := transport.NewTCPServer(nil)

	// 测试添加中间件
	middleware := func(next core.MessageHandler) core.MessageHandler {
		return func(ctx context.Context, conn core.Connection, msg *core.Message) error {
			return next(ctx, conn, msg)
		}
	}

	server.Use(middleware)
	server.Use(middleware)
}

// TestTCPServerStats 测试统计信息
func TestTCPServerStats(t *testing.T) {
	server := transport.NewTCPServer(nil)

	stats := server.GetStats()
	if stats.ActiveConnections != 0 {
		t.Errorf("Expected 0 active connections, got %d", stats.ActiveConnections)
	}

	if stats.TotalConnections != 0 {
		t.Errorf("Expected 0 total connections, got %d", stats.TotalConnections)
	}

	if stats.ReceivedMessages != 0 {
		t.Errorf("Expected 0 received messages, got %d", stats.ReceivedMessages)
	}

	if stats.SentMessages != 0 {
		t.Errorf("Expected 0 sent messages, got %d", stats.SentMessages)
	}

	if stats.ErrorCount != 0 {
		t.Errorf("Expected 0 errors, got %d", stats.ErrorCount)
	}
}

// TestTCPServerConnection 测试连接管理
func TestTCPServerConnection(t *testing.T) {
	t.Run("GetNonExistentConnection", func(t *testing.T) {
		server := transport.NewTCPServer(nil)

		_, err := server.GetConnection("nonexistent")
		if err == nil {
			t.Error("Expected error when getting non-existent connection")
		}
	})
}

// TestTCPConnection 测试TCP连接
func TestTCPConnection(t *testing.T) {
	t.Run("NewTCPConnection", func(t *testing.T) {
		// 创建模拟的net.Conn
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		config := &transport.Config{
			ListenAddr: ":0",
		}
		tcpServer := transport.NewTCPServer(config)

		conn := transport.NewTCPConnection(server, tcpServer)
		if conn == nil {
			t.Fatal("Failed to create TCP connection")
		}

		if !conn.IsConnected() {
			t.Error("New connection should be connected")
		}

		if conn.DeviceID() != "" {
			t.Errorf("Expected empty device ID, got %s", conn.DeviceID())
		}
	})

	t.Run("SetDeviceID", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		config := &transport.Config{
			ListenAddr: ":0",
		}
		tcpServer := transport.NewTCPServer(config)

		conn := transport.NewTCPConnection(server, tcpServer)
		conn.SetDeviceID("test-device-001")

		if conn.DeviceID() != "test-device-001" {
			t.Errorf("Expected device ID 'test-device-001', got '%s'", conn.DeviceID())
		}
	})

	t.Run("ConnectionAttributes", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		config := &transport.Config{
			ListenAddr: ":0",
		}
		tcpServer := transport.NewTCPServer(config)

		conn := transport.NewTCPConnection(server, tcpServer)

		// 测试设置和获取属性
		conn.Set("key1", "value1")
		conn.Set("key2", 123)

		val1, ok1 := conn.Get("key1")
		if !ok1 || val1 != "value1" {
			t.Errorf("Expected 'value1', got '%v'", val1)
		}

		val2, ok2 := conn.Get("key2")
		if !ok2 || val2 != 123 {
			t.Errorf("Expected 123, got '%v'", val2)
		}

		// 测试不存在的属性
		_, ok3 := conn.Get("nonexistent")
		if ok3 {
			t.Error("Expected false for non-existent attribute")
		}
	})

	t.Run("RemoteAddr", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		config := &transport.Config{
			ListenAddr: ":0",
		}
		tcpServer := transport.NewTCPServer(config)

		conn := transport.NewTCPConnection(server, tcpServer)

		// 获取远程地址
		addr := conn.RemoteAddr()
		if addr == nil {
			t.Error("Remote address should not be nil")
		}
	})

	t.Run("LastActiveTime", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		config := &transport.Config{
			ListenAddr: ":0",
		}
		tcpServer := transport.NewTCPServer(config)

		before := time.Now()
		conn := transport.NewTCPConnection(server, tcpServer)
		after := time.Now()

		lastActive := conn.LastActiveTime()
		if lastActive.Before(before) || lastActive.After(after) {
			t.Errorf("Last active time %v should be between %v and %v", lastActive, before, after)
		}
	})

	t.Run("Context", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()

		config := &transport.Config{
			ListenAddr: ":0",
		}
		tcpServer := transport.NewTCPServer(config)

		conn := transport.NewTCPConnection(server, tcpServer)

		ctx := conn.Context()
		if ctx == nil {
			t.Error("Context should not be nil")
		}

		// 测试上下文是否被取消
		select {
		case <-ctx.Done():
			t.Error("Context should not be done initially")
		default:
			// 正常
		}
	})

	t.Run("Close", func(t *testing.T) {
		server, client := net.Pipe()
		defer client.Close()

		config := &transport.Config{
			ListenAddr: ":0",
		}
		tcpServer := transport.NewTCPServer(config)

		conn := transport.NewTCPConnection(server, tcpServer)

		// 关闭连接
		err := conn.Close()
		if err != nil {
			t.Fatalf("Failed to close connection: %v", err)
		}

		// 验证连接已关闭
		if conn.IsConnected() {
			t.Error("Connection should be closed")
		}

		// 再次关闭应该成功
		err = conn.Close()
		if err != nil {
			t.Fatalf("Second close should not return error: %v", err)
		}
	})

	t.Run("SendOnClosedConnection", func(t *testing.T) {
		server, client := net.Pipe()
		defer client.Close()

		config := &transport.Config{
			ListenAddr: ":0",
		}
		tcpServer := transport.NewTCPServer(config)

		conn := transport.NewTCPConnection(server, tcpServer)

		// 关闭连接
		conn.Close()

		// 尝试在已关闭的连接上发送消息
		msg := &core.Message{
			Header: &core.MessageHeader{
				MsgID:     0x0001,
				PhoneNo:   "13800138000",
				MsgFlowNo: 1,
			},
			Body: []byte{0x01, 0x02, 0x03},
		}

		err := conn.Send(msg)
		if err == nil {
			t.Error("Expected error when sending on closed connection")
		}
	})
}

// TestTCPServerConcurrency 测试并发安全性
func TestTCPServerConcurrency(t *testing.T) {
	server := transport.NewTCPServer(nil)

	var wg sync.WaitGroup
	numGoroutines := 10

	// 并发注册处理器
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id uint16) {
			defer wg.Done()
			server.RegisterHandler(id, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
				return nil
			})
		}(uint16(i))
	}

	// 并发添加中间件
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			server.Use(func(next core.MessageHandler) core.MessageHandler {
				return next
			})
		}()
	}

	// 并发添加钩子
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			server.OnConnect(func(conn core.Connection) error {
				return nil
			})
		}()
	}

	wg.Wait()
}

// TestDefaultConfig 测试默认配置
func TestDefaultConfig(t *testing.T) {
	config := transport.DefaultConfig()

	if config.ListenAddr != ":8080" {
		t.Errorf("Expected listen addr ':8080', got '%s'", config.ListenAddr)
	}

	if config.MaxConnections != 1000000 {
		t.Errorf("Expected max connections 1000000, got %d", config.MaxConnections)
	}

	if config.ReadTimeout != 30*time.Second {
		t.Errorf("Expected read timeout 30s, got %v", config.ReadTimeout)
	}

	if config.WriteTimeout != 30*time.Second {
		t.Errorf("Expected write timeout 30s, got %v", config.WriteTimeout)
	}

	if config.IdleTimeout != 300*time.Second {
		t.Errorf("Expected idle timeout 300s, got %v", config.IdleTimeout)
	}

	if config.ReadBufferSize != 4096 {
		t.Errorf("Expected read buffer size 4096, got %d", config.ReadBufferSize)
	}

	if config.WriteBufferSize != 4096 {
		t.Errorf("Expected write buffer size 4096, got %d", config.WriteBufferSize)
	}

	if config.MaxPacketSize != 4096 {
		t.Errorf("Expected max packet size 4096, got %d", config.MaxPacketSize)
	}
}

// TestHooksStructure 测试Hooks结构
func TestHooksStructure(t *testing.T) {
	hooks := &transport.Hooks{}

	// 测试添加钩子
	hooks.OnConnect = append(hooks.OnConnect, func(conn core.Connection) error {
		return nil
	})

	hooks.OnDisconnect = append(hooks.OnDisconnect, func(conn core.Connection) error {
		return nil
	})

	hooks.OnMessage = append(hooks.OnMessage, func(conn core.Connection, msg *protocol.Message) error {
		return nil
	})

	hooks.OnError = append(hooks.OnError, func(conn core.Connection, err error) {
		// 错误处理
	})

	if len(hooks.OnConnect) != 1 {
		t.Errorf("Expected 1 OnConnect hook, got %d", len(hooks.OnConnect))
	}

	if len(hooks.OnDisconnect) != 1 {
		t.Errorf("Expected 1 OnDisconnect hook, got %d", len(hooks.OnDisconnect))
	}

	if len(hooks.OnMessage) != 1 {
		t.Errorf("Expected 1 OnMessage hook, got %d", len(hooks.OnMessage))
	}

	if len(hooks.OnError) != 1 {
		t.Errorf("Expected 1 OnError hook, got %d", len(hooks.OnError))
	}
}

// TestStatsStructure 测试Stats结构
func TestStatsStructure(t *testing.T) {
	stats := &transport.Stats{
		ActiveConnections: 10,
		TotalConnections:  100,
		ReceivedMessages:  500,
		SentMessages:      400,
		ErrorCount:        5,
		StartTime:         time.Now(),
	}

	if stats.ActiveConnections != 10 {
		t.Errorf("Expected 10 active connections, got %d", stats.ActiveConnections)
	}

	if stats.TotalConnections != 100 {
		t.Errorf("Expected 100 total connections, got %d", stats.TotalConnections)
	}

	if stats.ReceivedMessages != 500 {
		t.Errorf("Expected 500 received messages, got %d", stats.ReceivedMessages)
	}

	if stats.SentMessages != 400 {
		t.Errorf("Expected 400 sent messages, got %d", stats.SentMessages)
	}

	if stats.ErrorCount != 5 {
		t.Errorf("Expected 5 errors, got %d", stats.ErrorCount)
	}
}
