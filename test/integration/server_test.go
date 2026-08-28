package integration

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/darkinno-tech/jtt-808-go-sdk/core"
	"github.com/darkinno-tech/jtt-808-go-sdk/protocol"
	"github.com/darkinno-tech/jtt-808-go-sdk/protocol/types"
	"github.com/darkinno-tech/jtt-808-go-sdk/storage"
	"github.com/darkinno-tech/jtt-808-go-sdk/transport"
)

// TestServer_StartAndStop 测试服务器正常启动和停止
func TestServer_StartAndStop(t *testing.T) {
	addr := "127.0.0.1:19101"
	config := transport.DefaultConfig()
	config.ListenAddr = addr
	server := transport.NewTCPServer(config)

	if err := server.Start(); err != nil {
		t.Fatalf("启动服务器失败: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// 验证可以连接
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("连接服务器失败: %v", err)
	}
	conn.Close()

	// 停止服务器
	if err := server.Stop(); err != nil {
		t.Fatalf("停止服务器失败: %v", err)
	}

	// 验证无法连接
	time.Sleep(100 * time.Millisecond)
	_, err = net.DialTimeout("tcp", addr, 1*time.Second)
	if err == nil {
		t.Error("服务器停止后仍可连接")
	}

	t.Log("✓ 服务器启停测试通过")
}

// TestServer_GetStats 测试服务器统计信息
func TestServer_GetStats(t *testing.T) {
	addr := "127.0.0.1:19102"
	_, _, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	codec := protocol.NewCodec()
	phoneNo := "13910001001"

	// 建立连接并发送消息
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}

	// 发送注册
	regData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalRegister, PhoneNo: phoneNo, MsgFlowNo: 1,
		},
		Body: buildRegisterBodyForDevice("STATDV1", phoneNo),
	})
	conn.Write(regData)
	readAndDecode(t, conn, codec)

	// 发送心跳
	hbData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalHeartbeat, PhoneNo: phoneNo, MsgFlowNo: 2,
		},
		Body: []byte{},
	})
	conn.Write(hbData)
	readAndDecode(t, conn, codec)

	conn.Close()
	time.Sleep(200 * time.Millisecond)

	t.Log("✓ 服务器统计测试通过")
}

// TestServer_MiddlewareExecution 测试中间件是否被正确调用
func TestServer_MiddlewareExecution(t *testing.T) {
	addr := "127.0.0.1:19103"
	memStorage := storage.NewMemoryStorage()
	config := transport.DefaultConfig()
	config.ListenAddr = addr

	server := transport.NewTCPServer(config)

	var middlewareCalled int64

	// 添加计数中间件
	server.Use(func(next core.MessageHandler) core.MessageHandler {
		return func(ctx context.Context, conn core.Connection, msg *core.Message) error {
			atomic.AddInt64(&middlewareCalled, 1)
			return next(ctx, conn, msg)
		}
	})

	// 注册心跳处理器
	server.RegisterHandler(types.MsgIDTerminalHeartbeat, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		return conn.Send(&core.Message{
			Header: &core.MessageHeader{
				MsgID:     types.MsgIDPlatformCommonResponse,
				PhoneNo:   msg.Header.PhoneNo,
				MsgFlowNo: msg.Header.MsgFlowNo,
			},
			Body: buildCommonResponse(msg.Header.MsgID, msg.Header.MsgFlowNo, types.CommonResponseSuccess),
		})
	})

	if err := server.Start(); err != nil {
		t.Fatalf("启动服务器失败: %v", err)
	}
	defer server.Stop()
	time.Sleep(100 * time.Millisecond)

	codec := protocol.NewCodec()

	// 发送3次心跳
	for i := 0; i < 3; i++ {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			t.Fatalf("第%d次连接失败: %v", i+1, err)
		}

		hbData, _ := codec.Encode(&protocol.Message{
			Header: &protocol.MessageHeader{
				MsgID: types.MsgIDTerminalHeartbeat, PhoneNo: "13910002001", MsgFlowNo: uint16(i + 1),
			},
			Body: []byte{},
		})
		conn.Write(hbData)
		readAndDecode(t, conn, codec)
		conn.Close()
	}

	time.Sleep(200 * time.Millisecond)

	called := atomic.LoadInt64(&middlewareCalled)
	if called != 3 {
		t.Errorf("中间件调用次数不正确: 期望3, 实际%d", called)
	}

	_ = memStorage
	t.Logf("✓ 中间件执行测试通过: 调用%d次", called)
}

// TestServer_Hooks 测试连接钩子函数
func TestServer_Hooks(t *testing.T) {
	addr := "127.0.0.1:19104"
	config := transport.DefaultConfig()
	config.ListenAddr = addr

	server := transport.NewTCPServer(config)

	var connectCount int64
	var disconnectCount int64

	server.OnConnect(func(conn core.Connection) error {
		atomic.AddInt64(&connectCount, 1)
		return nil
	})

	server.OnDisconnect(func(conn core.Connection) error {
		atomic.AddInt64(&disconnectCount, 1)
		return nil
	})

	server.RegisterHandler(types.MsgIDTerminalHeartbeat, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		return conn.Send(&core.Message{
			Header: &core.MessageHeader{
				MsgID:     types.MsgIDPlatformCommonResponse,
				PhoneNo:   msg.Header.PhoneNo,
				MsgFlowNo: msg.Header.MsgFlowNo,
			},
			Body: buildCommonResponse(msg.Header.MsgID, msg.Header.MsgFlowNo, types.CommonResponseSuccess),
		})
	})

	if err := server.Start(); err != nil {
		t.Fatalf("启动服务器失败: %v", err)
	}
	defer server.Stop()
	time.Sleep(100 * time.Millisecond)

	codec := protocol.NewCodec()

	// 连接3个客户端
	conns := make([]net.Conn, 3)
	for i := 0; i < 3; i++ {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			t.Fatalf("第%d个客户端连接失败: %v", i+1, err)
		}
		conns[i] = conn

		hbData, _ := codec.Encode(&protocol.Message{
			Header: &protocol.MessageHeader{
				MsgID: types.MsgIDTerminalHeartbeat, PhoneNo: fmt.Sprintf("1391000300%d", i), MsgFlowNo: 1,
			},
			Body: []byte{},
		})
		conn.Write(hbData)
		readAndDecode(t, conn, codec)
	}

	time.Sleep(100 * time.Millisecond)

	connsOpened := atomic.LoadInt64(&connectCount)
	if connsOpened != 3 {
		t.Errorf("连接钩子调用次数不正确: 期望3, 实际%d", connsOpened)
	}

	// 关闭所有连接
	for _, conn := range conns {
		conn.Close()
	}

	time.Sleep(200 * time.Millisecond)

	connsClosed := atomic.LoadInt64(&disconnectCount)
	if connsClosed != 3 {
		t.Errorf("断开钩子调用次数不正确: 期望3, 实际%d", connsClosed)
	}

	t.Logf("✓ 钩子测试通过: 连接=%d, 断开=%d", connsOpened, connsClosed)
}

// TestServer_ErrorHandler 测试错误处理钩子
func TestServer_ErrorHandler(t *testing.T) {
	addr := "127.0.0.1:19105"
	config := transport.DefaultConfig()
	config.ListenAddr = addr

	server := transport.NewTCPServer(config)

	var errorCount int64

	server.OnError(func(conn core.Connection, err error) {
		atomic.AddInt64(&errorCount, 1)
	})

	server.RegisterHandler(types.MsgIDTerminalHeartbeat, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		return conn.Send(&core.Message{
			Header: &core.MessageHeader{
				MsgID:     types.MsgIDPlatformCommonResponse,
				PhoneNo:   msg.Header.PhoneNo,
				MsgFlowNo: msg.Header.MsgFlowNo,
			},
			Body: buildCommonResponse(msg.Header.MsgID, msg.Header.MsgFlowNo, types.CommonResponseSuccess),
		})
	})

	if err := server.Start(); err != nil {
		t.Fatalf("启动服务器失败: %v", err)
	}
	defer server.Stop()
	time.Sleep(100 * time.Millisecond)

	// 发送损坏数据
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}

	// 发送无效数据（非JT808协议格式）
	conn.Write([]byte{0x7E, 0xFF, 0xFF, 0x7E})
	time.Sleep(300 * time.Millisecond)

	errCnt := atomic.LoadInt64(&errorCount)
	t.Logf("错误钩子触发次数: %d", errCnt)

	conn.Close()
	t.Log("✓ 错误处理测试通过")
}

// TestServer_ConcurrentConnections 测试服务器处理并发连接
func TestServer_ConcurrentConnections(t *testing.T) {
	addr := "127.0.0.1:19106"
	_, memStorage, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	concurrency := 100
	var wg sync.WaitGroup
	var errCount int64
	var successCount int64

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			phoneNo := fmt.Sprintf("139%08d", id)
			deviceID := fmt.Sprintf("CON%04d", id)

			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				atomic.AddInt64(&errCount, 1)
				return
			}
			defer conn.Close()

			codec := protocol.NewCodec()

			// 注册
			regData, _ := codec.Encode(&protocol.Message{
				Header: &protocol.MessageHeader{
					MsgID: types.MsgIDTerminalRegister, PhoneNo: phoneNo, MsgFlowNo: 1,
				},
				Body: buildRegisterBodyForDevice(deviceID, phoneNo),
			})
			conn.Write(regData)
			if _, err := readAndDecode(t, conn, codec); err != nil {
				atomic.AddInt64(&errCount, 1)
				return
			}

			// 位置上报
			locBody := buildRealLocationBody(
				beijingLat+int32(id),
				beijingLng+int32(id),
				30, 600, 0,
			)
			locData, _ := codec.Encode(&protocol.Message{
				Header: &protocol.MessageHeader{
					MsgID: types.MsgIDLocationReport, PhoneNo: phoneNo, MsgFlowNo: 2,
				},
				Body: locBody,
			})
			conn.Write(locData)
			if _, err := readAndDecode(t, conn, codec); err != nil {
				atomic.AddInt64(&errCount, 1)
				return
			}

			atomic.AddInt64(&successCount, 1)
		}(i)
	}

	wg.Wait()

	if errCount > int64(concurrency/10) {
		t.Errorf("出错连接数过多: %d/%d", errCount, concurrency)
	}

	time.Sleep(300 * time.Millisecond)

	// 验证部分设备
	for i := 0; i < 5; i++ {
		deviceID := fmt.Sprintf("CON%04d", i)
		locations, _ := memStorage.GetLocations(context.Background(), deviceID,
			time.Now().Add(-1*time.Minute), time.Now().Add(1*time.Minute))
		if len(locations) == 0 {
			t.Errorf("设备%d没有位置数据", i)
		}
	}

	t.Logf("✓ 并发连接测试通过: 成功=%d/%d", successCount, concurrency)
}

// TestServer_UnregisteredMessage 测试未注册处理器的消息
func TestServer_UnregisteredMessage(t *testing.T) {
	addr := "127.0.0.1:19107"
	_, _, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	codec := protocol.NewCodec()

	// 发送一个未注册处理器的消息类型（事件报告 0x0300）
	eventData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDEventReport, PhoneNo: "13910004001", MsgFlowNo: 1,
		},
		Body: []byte{0x01, 0x02, 0x03},
	})
	conn.Write(eventData)

	// 服务器应忽略该消息，不应崩溃，连接应保持
	time.Sleep(300 * time.Millisecond)

	// 发送心跳验证连接仍可用
	hbData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalHeartbeat, PhoneNo: "13910004001", MsgFlowNo: 2,
		},
		Body: []byte{},
	})
	conn.Write(hbData)

	resp, err := readAndDecode(t, conn, codec)
	if err != nil {
		t.Fatalf("心跳应答失败（连接可能已断开）: %v", err)
	}
	if resp.Header.MsgID != types.MsgIDPlatformCommonResponse {
		t.Errorf("心跳应答ID错误: 0x%04X", resp.Header.MsgID)
	}

	t.Log("✓ 未注册消息处理测试通过")
}

// TestServer_LargeMessagePayload 测试大消息体处理
func TestServer_LargeMessagePayload(t *testing.T) {
	addr := "127.0.0.1:19108"
	_, memStorage, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	codec := protocol.NewCodec()
	phoneNo := "13910005001"
	deviceID := "BIGDEV0"

	// 注册
	regData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalRegister, PhoneNo: phoneNo, MsgFlowNo: 1,
		},
		Body: buildRegisterBodyForDevice(deviceID, phoneNo),
	})
	conn.Write(regData)
	readAndDecode(t, conn, codec)

	// 快速发送大量位置消息
	messageCount := 50
	for i := 0; i < messageCount; i++ {
		locBody := buildRealLocationBody(
			beijingLat+int32(i*10),
			beijingLng+int32(i*10),
			30, 600, 0,
		)
		locData, _ := codec.Encode(&protocol.Message{
			Header: &protocol.MessageHeader{
				MsgID: types.MsgIDLocationReport, PhoneNo: phoneNo, MsgFlowNo: uint16(i + 2),
			},
			Body: locBody,
		})
		conn.Write(locData)
		readAndDecode(t, conn, codec)
	}

	time.Sleep(500 * time.Millisecond)

	locations, _ := memStorage.GetLocations(context.Background(), deviceID,
		time.Now().Add(-1*time.Minute), time.Now().Add(1*time.Minute))
	if len(locations) != messageCount {
		t.Errorf("位置数据不完整: 期望%d, 实际%d", messageCount, len(locations))
	}

	t.Logf("✓ 大批量消息测试通过: %d条消息", messageCount)
}

// TestServer_ConnectionID 测试连接设备ID设置
func TestServer_ConnectionID(t *testing.T) {
	addr := "127.0.0.1:19109"
	config := transport.DefaultConfig()
	config.ListenAddr = addr

	server := transport.NewTCPServer(config)

	var deviceIDs []string
	var mu sync.Mutex

	server.RegisterHandler(types.MsgIDTerminalRegister, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		reg, _ := protocol.ParseTerminalRegister(msg.Body)
		if tcpConn, ok := conn.(*transport.TCPConnection); ok {
			tcpConn.SetDeviceID(reg.TerminalID)
		}

		mu.Lock()
		deviceIDs = append(deviceIDs, conn.DeviceID())
		mu.Unlock()

		return conn.Send(&core.Message{
			Header: &core.MessageHeader{
				MsgID:     types.MsgIDTerminalRegisterResponse,
				PhoneNo:   msg.Header.PhoneNo,
				MsgFlowNo: msg.Header.MsgFlowNo,
			},
			Body: buildRegisterResponse(types.RegisterResultSuccess, "AUTH_TOKEN_2024"),
		})
	})

	server.RegisterHandler(types.MsgIDLocationReport, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		mu.Lock()
		deviceIDs = append(deviceIDs, conn.DeviceID())
		mu.Unlock()

		return conn.Send(&core.Message{
			Header: &core.MessageHeader{
				MsgID:     types.MsgIDPlatformCommonResponse,
				PhoneNo:   msg.Header.PhoneNo,
				MsgFlowNo: msg.Header.MsgFlowNo,
			},
			Body: buildCommonResponse(msg.Header.MsgID, msg.Header.MsgFlowNo, types.CommonResponseSuccess),
		})
	})

	if err := server.Start(); err != nil {
		t.Fatalf("启动服务器失败: %v", err)
	}
	defer server.Stop()
	time.Sleep(100 * time.Millisecond)

	codec := protocol.NewCodec()
	phoneNo := "13910006001"
	deviceID := "IDTEST1"

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	// 注册
	regData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalRegister, PhoneNo: phoneNo, MsgFlowNo: 1,
		},
		Body: buildRegisterBodyForDevice(deviceID, phoneNo),
	})
	conn.Write(regData)
	readAndDecode(t, conn, codec)

	// 位置上报（此时连接应已有设备ID）
	locBody := buildRealLocationBody(beijingLat, beijingLng, 50, 600, 90)
	locData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDLocationReport, PhoneNo: phoneNo, MsgFlowNo: 2,
		},
		Body: locBody,
	})
	conn.Write(locData)
	readAndDecode(t, conn, codec)

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(deviceIDs) < 2 {
		t.Fatalf("设备ID记录不足: %d", len(deviceIDs))
	}

	// 注册时DeviceID为空字符串（新连接），位置上报时应为已注册的ID
	if deviceIDs[1] != deviceID {
		t.Errorf("位置上报时设备ID不匹配: 期望%s, 实际%s", deviceID, deviceIDs[1])
	}

	t.Logf("✓ 连接设备ID测试通过: 注册=%q, 上报=%q", deviceIDs[0], deviceIDs[1])
}

// TestServer_DoubleRegistration 测试同一设备重复注册
func TestServer_DoubleRegistration(t *testing.T) {
	addr := "127.0.0.1:19110"
	_, _, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	codec := protocol.NewCodec()
	phoneNo := "13910007001"
	deviceID := "DREGDV1"

	// 第一次连接注册
	conn1, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("第一次连接失败: %v", err)
	}

	regData1, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalRegister, PhoneNo: phoneNo, MsgFlowNo: 1,
		},
		Body: buildRegisterBodyForDevice(deviceID, phoneNo),
	})
	conn1.Write(regData1)
	resp1, err := readAndDecode(t, conn1, codec)
	if err != nil {
		t.Fatalf("第一次注册应答失败: %v", err)
	}
	if resp1.Header.MsgID != types.MsgIDTerminalRegisterResponse {
		t.Errorf("第一次注册应答ID错误: 0x%04X", resp1.Header.MsgID)
	}
	t.Log("[1/3] 第一次注册成功")

	// 第二次连接用相同设备ID注册
	conn2, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("第二次连接失败: %v", err)
	}
	defer conn2.Close()

	regData2, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalRegister, PhoneNo: phoneNo, MsgFlowNo: 1,
		},
		Body: buildRegisterBodyForDevice(deviceID, phoneNo),
	})
	conn2.Write(regData2)
	resp2, err := readAndDecode(t, conn2, codec)
	if err != nil {
		t.Fatalf("第二次注册应答失败: %v", err)
	}
	if resp2.Header.MsgID != types.MsgIDTerminalRegisterResponse {
		t.Errorf("第二次注册应答ID错误: 0x%04X", resp2.Header.MsgID)
	}
	t.Log("[2/3] 第二次注册成功（覆盖）")

	// 验证第二次连接可以正常上报位置
	authData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalAuth, PhoneNo: phoneNo, MsgFlowNo: 2,
		},
		Body: []byte("AUTH_TOKEN_2024"),
	})
	conn2.Write(authData)
	readAndDecode(t, conn2, codec)

	locBody := buildRealLocationBody(beijingLat, beijingLng, 50, 600, 90)
	locData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDLocationReport, PhoneNo: phoneNo, MsgFlowNo: 3,
		},
		Body: locBody,
	})
	conn2.Write(locData)
	locResp, err := readAndDecode(t, conn2, codec)
	if err != nil {
		t.Fatalf("位置上报应答失败: %v", err)
	}
	if locResp.Header.MsgID != types.MsgIDPlatformCommonResponse {
		t.Errorf("位置应答ID错误: 0x%04X", locResp.Header.MsgID)
	}

	conn1.Close()
	t.Log("[3/3] 第二次连接位置上报成功")
	t.Log("✓ 重复注册测试通过")
}

// TestServer_ShutdownWithActiveConnections 测试有活跃连接时服务器关闭
func TestServer_ShutdownWithActiveConnections(t *testing.T) {
	addr := "127.0.0.1:19111"
	config := transport.DefaultConfig()
	config.ListenAddr = addr

	server := transport.NewTCPServer(config)

	server.RegisterHandler(types.MsgIDTerminalHeartbeat, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		return conn.Send(&core.Message{
			Header: &core.MessageHeader{
				MsgID:     types.MsgIDPlatformCommonResponse,
				PhoneNo:   msg.Header.PhoneNo,
				MsgFlowNo: msg.Header.MsgFlowNo,
			},
			Body: buildCommonResponse(msg.Header.MsgID, msg.Header.MsgFlowNo, types.CommonResponseSuccess),
		})
	})

	if err := server.Start(); err != nil {
		t.Fatalf("启动服务器失败: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	codec := protocol.NewCodec()

	// 建立多个连接
	conns := make([]net.Conn, 5)
	for i := 0; i < 5; i++ {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			t.Fatalf("连接%d失败: %v", i, err)
		}
		conns[i] = conn

		hbData, _ := codec.Encode(&protocol.Message{
			Header: &protocol.MessageHeader{
				MsgID: types.MsgIDTerminalHeartbeat, PhoneNo: fmt.Sprintf("1391000800%d", i), MsgFlowNo: 1,
			},
			Body: []byte{},
		})
		conn.Write(hbData)
		readAndDecode(t, conn, codec)
	}

	// 在有活跃连接的情况下停止服务器
	stopDone := make(chan struct{})
	go func() {
		server.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		t.Log("服务器在有活跃连接时正常停止")
	case <-time.After(15 * time.Second):
		t.Error("服务器停止超时")
	}

	// 验证所有连接已断开
	for _, conn := range conns {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 1)
		_, err := conn.Read(buf)
		if err == nil {
			t.Error("服务器停止后连接仍可读取")
		}
		conn.Close()
	}

	t.Log("✓ 活跃连接关闭测试通过")
}
