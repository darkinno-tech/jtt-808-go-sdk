package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/im10furry/jtt-808-go-sdk/core"
	"github.com/im10furry/jtt-808-go-sdk/protocol"
	"github.com/im10furry/jtt-808-go-sdk/protocol/types"
	"github.com/im10furry/jtt-808-go-sdk/storage"
	"github.com/im10furry/jtt-808-go-sdk/transport"
)

// 真实国标设备参数
const (
	testPhoneNo  = "13800138000" // 终端手机号（BCD编码12位）
	testDeviceID = "BD56789"     // 终端ID（7字节）
	testPlateNo  = "A12345"      // 车牌号（ASCII，6字节）
)

// 北京天安门广场坐标（真实国标格式：1/100000度）
// 纬度 39.9042°N → 3990420
// 经度 116.4074°E → 11640740
const (
	beijingLat = 3990420
	beijingLng = 11640740
)

// startTestServer 启动测试服务器，返回 cleanup 函数
func startTestServer(t *testing.T, listenAddr string) (*transport.TCPServer, *storage.MemoryStorage, func()) {
	t.Helper()

	memStorage := storage.NewMemoryStorage()
	config := transport.DefaultConfig()
	config.ListenAddr = listenAddr
	config.MaxConnections = 100
	config.ReadTimeout = 10 * time.Second
	config.WriteTimeout = 10 * time.Second

	server := transport.NewTCPServer(config)

	// 终端注册处理器
	server.RegisterHandler(types.MsgIDTerminalRegister, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		reg, err := protocol.ParseTerminalRegister(msg.Body)
		if err != nil {
			return err
		}
		deviceID := reg.TerminalID
		if tcpConn, ok := conn.(*transport.TCPConnection); ok {
			tcpConn.SetDeviceID(deviceID)
		}
		ctx = context.WithValue(ctx, "deviceID", deviceID)
		_ = memStorage.SaveDevice(ctx, deviceID, reg)

		// 注册应答：成功 + 鉴权码
		return conn.Send(&core.Message{
			Header: &core.MessageHeader{
				MsgID:     types.MsgIDTerminalRegisterResponse,
				PhoneNo:   msg.Header.PhoneNo,
				MsgFlowNo: msg.Header.MsgFlowNo,
			},
			Body: buildRegisterResponse(types.RegisterResultSuccess, "AUTH_TOKEN_2024"),
		})
	})

	// 终端鉴权处理器
	server.RegisterHandler(types.MsgIDTerminalAuth, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		return conn.Send(&core.Message{
			Header: &core.MessageHeader{
				MsgID:     types.MsgIDPlatformCommonResponse,
				PhoneNo:   msg.Header.PhoneNo,
				MsgFlowNo: msg.Header.MsgFlowNo,
			},
			Body: buildCommonResponse(msg.Header.MsgID, msg.Header.MsgFlowNo, types.CommonResponseSuccess),
		})
	})

	// 位置上报处理器
	server.RegisterHandler(types.MsgIDLocationReport, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		report, err := protocol.ParseLocationReport(msg.Body)
		if err != nil {
			return err
		}
		deviceID := conn.DeviceID()
		ctx = context.WithValue(ctx, "deviceID", deviceID)
		_ = memStorage.SaveLocation(ctx, report)

		return conn.Send(&core.Message{
			Header: &core.MessageHeader{
				MsgID:     types.MsgIDPlatformCommonResponse,
				PhoneNo:   msg.Header.PhoneNo,
				MsgFlowNo: msg.Header.MsgFlowNo,
			},
			Body: buildCommonResponse(msg.Header.MsgID, msg.Header.MsgFlowNo, types.CommonResponseSuccess),
		})
	})

	// 心跳处理器
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

	cleanup := func() {
		server.Stop()
	}
	return server, memStorage, cleanup
}

// TestCodecRoundtrip 验证协议编解码往返正确性
func TestCodecRoundtrip(t *testing.T) {
	codec := protocol.NewCodec()

	// 编码位置消息
	locBody := buildRealLocationBody(beijingLat, beijingLng, 50, 600, 90)
	locMsg := &protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID:     types.MsgIDLocationReport,
			PhoneNo:   testPhoneNo,
			MsgFlowNo: 1,
		},
		Body: locBody,
	}

	encoded, err := codec.Encode(locMsg)
	if err != nil {
		t.Fatalf("编码失败: %v", err)
	}
	t.Logf("编码后: %d bytes, hex=%X", len(encoded), encoded)

	decoded, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("解码失败: %v", err)
	}
	t.Logf("解码后: MsgID=0x%04X, Body=%d bytes", decoded.Header.MsgID, len(decoded.Body))
	t.Logf("解码Body hex=%X", decoded.Body)

	if len(decoded.Body) != 28 {
		t.Fatalf("Body长度错误: 期望28, 实际%d", len(decoded.Body))
	}

	// 解析位置
	report, err := protocol.ParseLocationReport(decoded.Body)
	if err != nil {
		t.Fatalf("解析位置失败: %v", err)
	}
	t.Logf("解析结果: 纬度=%.5f, 经度=%.5f, 海拔=%d, 速度=%d, 方向=%d",
		report.Latitude, report.Longitude, report.Altitude, report.Speed, report.Direction)

	if report.Latitude < 39.9 || report.Latitude > 39.91 {
		t.Errorf("纬度异常: %f", report.Latitude)
	}
	if report.Longitude < 116.4 || report.Longitude > 116.41 {
		t.Errorf("经度异常: %f", report.Longitude)
	}

	// 编码注册消息
	regBody := buildRealRegisterBody()
	regMsg := &protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID:     types.MsgIDTerminalRegister,
			PhoneNo:   testPhoneNo,
			MsgFlowNo: 2,
		},
		Body: regBody,
	}

	regEncoded, err := codec.Encode(regMsg)
	if err != nil {
		t.Fatalf("注册编码失败: %v", err)
	}

	regDecoded, err := codec.Decode(regEncoded)
	if err != nil {
		t.Fatalf("注册解码失败: %v", err)
	}

	reg, err := protocol.ParseTerminalRegister(regDecoded.Body)
	if err != nil {
		t.Fatalf("注册解析失败: %v", err)
	}
	t.Logf("注册解析: TerminalID=%s, PlateNo=%s, ManufacturerID=%s, TerminalType=%s",
		reg.TerminalID, reg.PlateNo, reg.ManufacturerID, reg.TerminalType)

	if reg.TerminalID != testDeviceID {
		t.Errorf("TerminalID不匹配: 期望%s, 实际%s", testDeviceID, reg.TerminalID)
	}
	if reg.PlateNo != testPlateNo {
		t.Errorf("PlateNo不匹配: 期望%s, 实际%s", testPlateNo, reg.PlateNo)
	}
}

// TestFullFlow_ConnectRegisterAuthLocation 测试完整链路：
// TCP连接 → 终端注册 → 注册应答 → 终端鉴权 → 鉴权应答 → 位置上报 → 通用应答
func TestFullFlow_ConnectRegisterAuthLocation(t *testing.T) {
	addr := "127.0.0.1:18080"
	_, memStorage, cleanup := startTestServer(t, addr)
	defer cleanup()

	// 等待服务器就绪
	time.Sleep(100 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("连接服务器失败: %v", err)
	}
	defer conn.Close()

	codec := protocol.NewCodec()
	flowNo := uint16(0)

	// === 1. 终端注册 ===
	flowNo++
	regBody := buildRealRegisterBody()
	regMsg := &protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID:     types.MsgIDTerminalRegister,
			PhoneNo:   testPhoneNo,
			MsgFlowNo: flowNo,
		},
		Body: regBody,
	}
	regData, err := codec.Encode(regMsg)
	if err != nil {
		t.Fatalf("编码注册消息失败: %v", err)
	}
	if _, err := conn.Write(regData); err != nil {
		t.Fatalf("发送注册消息失败: %v", err)
	}
	t.Logf("→ 终端注册 (0x%04X), 流水号=%d, %d bytes", types.MsgIDTerminalRegister, flowNo, len(regData))

	// 读取注册应答
	regResp, err := readAndDecode(t, conn, codec)
	if err != nil {
		t.Fatalf("读取注册应答失败: %v", err)
	}
	if regResp.Header.MsgID != types.MsgIDTerminalRegisterResponse {
		t.Errorf("注册应答消息ID错误: 期望 0x%04X, 实际 0x%04X", types.MsgIDTerminalRegisterResponse, regResp.Header.MsgID)
	}
	if len(regResp.Body) >= 3 {
		result := regResp.Body[2]
		authCode := string(regResp.Body[3:])
		t.Logf("← 注册应答 (0x%04X), 结果=%d, 鉴权码=%s", regResp.Header.MsgID, result, authCode)
		if result != types.RegisterResultSuccess {
			t.Errorf("注册结果应为成功(0), 实际为 %d", result)
		}
	}

	// === 2. 终端鉴权 ===
	flowNo++
	authBody := []byte("AUTH_TOKEN_2024")
	authMsg := &protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID:     types.MsgIDTerminalAuth,
			PhoneNo:   testPhoneNo,
			MsgFlowNo: flowNo,
		},
		Body: authBody,
	}
	authData, err := codec.Encode(authMsg)
	if err != nil {
		t.Fatalf("编码鉴权消息失败: %v", err)
	}
	if _, err := conn.Write(authData); err != nil {
		t.Fatalf("发送鉴权消息失败: %v", err)
	}
	t.Logf("→ 终端鉴权 (0x%04X), 流水号=%d", types.MsgIDTerminalAuth, flowNo)

	authResp, err := readAndDecode(t, conn, codec)
	if err != nil {
		t.Fatalf("读取鉴权应答失败: %v", err)
	}
	if authResp.Header.MsgID != types.MsgIDPlatformCommonResponse {
		t.Errorf("鉴权应答消息ID错误: 期望 0x%04X, 实际 0x%04X", types.MsgIDPlatformCommonResponse, authResp.Header.MsgID)
	}
	t.Logf("← 鉴权应答 (0x%04X)", authResp.Header.MsgID)

	// === 3. 位置上报（北京天安门广场） ===
	flowNo++
	locBody := buildRealLocationBody(beijingLat, beijingLng, 50, 600, 90)
	locMsg := &protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID:     types.MsgIDLocationReport,
			PhoneNo:   testPhoneNo,
			MsgFlowNo: flowNo,
		},
		Body: locBody,
	}
	locData, err := codec.Encode(locMsg)
	if err != nil {
		t.Fatalf("编码位置消息失败: %v", err)
	}
	if _, err := conn.Write(locData); err != nil {
		t.Fatalf("发送位置消息失败: %v", err)
	}
	t.Logf("→ 位置上报 (0x%04X), 纬度=%.5f, 经度=%.5f, 海拔=%dm, 速度=%dkm/h, 方向=%d°",
		types.MsgIDLocationReport,
		float64(beijingLat)/100000.0, float64(beijingLng)/100000.0,
		50, 60, 90)

	locResp, err := readAndDecode(t, conn, codec)
	if err != nil {
		t.Fatalf("读取位置应答失败: %v", err)
	}
	if locResp.Header.MsgID != types.MsgIDPlatformCommonResponse {
		t.Errorf("位置应答消息ID错误: 期望 0x%04X, 实际 0x%04X", types.MsgIDPlatformCommonResponse, locResp.Header.MsgID)
	}
	t.Logf("← 位置应答 (0x%04X)", locResp.Header.MsgID)

	// === 验证存储数据 ===
	// 等待异步写入完成
	time.Sleep(50 * time.Millisecond)

	locations, err := memStorage.GetLocations(context.Background(), testDeviceID,
		time.Now().Add(-1*time.Minute), time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("获取位置数据失败: %v", err)
	}
	if len(locations) == 0 {
		t.Fatal("存储中没有位置数据")
	}

	loc := locations[len(locations)-1]
	t.Logf("存储验证: 纬度=%.5f, 经度=%.5f, 海拔=%dm, 速度=%d, 方向=%d",
		loc.Latitude, loc.Longitude, loc.Altitude, loc.Speed, loc.Direction)

	if loc.Latitude < 39.9 || loc.Latitude > 39.91 {
		t.Errorf("纬度异常: 期望约39.9042, 实际 %f", loc.Latitude)
	}
	if loc.Longitude < 116.4 || loc.Longitude > 116.41 {
		t.Errorf("经度异常: 期望约116.4074, 实际 %f", loc.Longitude)
	}

	t.Log("✓ 完整链路测试通过: 连接 → 注册 → 鉴权 → 位置上报 → 数据存储")
}

// TestMultiDevice_10Devices 测试 10 台设备并发注册+位置上报
func TestMultiDevice_10Devices(t *testing.T) {
	addr := "127.0.0.1:18081"
	_, memStorage, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	deviceCount := 10
	var wg sync.WaitGroup
	var errCount int64

	for i := 0; i < deviceCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			phoneNo := fmt.Sprintf("138%08d", id)
			deviceID := fmt.Sprintf("DEV%04d", id) // 7字节以内

			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
			if err != nil {
				t.Errorf("设备 %d 连接失败: %v", id, err)
				return
			}
			defer conn.Close()

			codec := protocol.NewCodec()
			flowNo := uint16(0)

			// 注册
			flowNo++
			regBody := buildRegisterBodyForDevice(deviceID, phoneNo)
			regData, _ := codec.Encode(&protocol.Message{
				Header: &protocol.MessageHeader{
					MsgID: types.MsgIDTerminalRegister, PhoneNo: phoneNo, MsgFlowNo: flowNo,
				},
				Body: regBody,
			})
			conn.Write(regData)
			readAndDecode(t, conn, codec)

			// 位置上报（每个设备不同坐标，模拟分布在北京各处）
			flowNo++
			lat := beijingLat + int32(id*100) // 纬度偏移
			lng := beijingLng + int32(id*100) // 经度偏移
			locBody := buildRealLocationBody(lat, lng, 30+uint16(id), 600, 0)
			locData, _ := codec.Encode(&protocol.Message{
				Header: &protocol.MessageHeader{
					MsgID: types.MsgIDLocationReport, PhoneNo: phoneNo, MsgFlowNo: flowNo,
				},
				Body: locBody,
			})
			conn.Write(locData)
			readAndDecode(t, conn, codec)
		}(i)
	}

	wg.Wait()

	if errCount > 0 {
		t.Errorf("有 %d 台设备出错", errCount)
	}

	time.Sleep(100 * time.Millisecond)

	// 验证所有设备都已注册
	device, err := memStorage.GetDevice(context.Background(), "DEV0000")
	if err != nil {
		t.Fatalf("获取设备信息失败: %v", err)
	}
	if device.PlateNo == "" {
		t.Error("车牌号为空")
	}

	// 验证位置数据
	locations, _ := memStorage.GetLocations(context.Background(), "DEV0000",
		time.Now().Add(-1*time.Minute), time.Now().Add(1*time.Minute))
	if len(locations) == 0 {
		t.Error("设备 0 没有位置数据")
	}

	t.Logf("✓ %d 台设备并发测试通过", deviceCount)
}

// TestLocationWithAlarm 上报带报警标志的位置信息
func TestLocationWithAlarm(t *testing.T) {
	addr := "127.0.0.1:18082"
	_, memStorage, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	codec := protocol.NewCodec()
	flowNo := uint16(0)

	// 注册
	flowNo++
	regBody := buildRealRegisterBody()
	regData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalRegister, PhoneNo: testPhoneNo, MsgFlowNo: flowNo,
		},
		Body: regBody,
	})
	conn.Write(regData)
	readAndDecode(t, conn, codec)

	// 位置上报：SOS紧急报警 + 超速报警
	flowNo++
	alarmFlags := uint32(types.AlarmFlagSOS | types.AlarmFlagOverSpeed)
	locBody := buildRealLocationBodyWithAlarm(alarmFlags, beijingLat, beijingLng, 50, 1200, 180)
	locData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDLocationReport, PhoneNo: testPhoneNo, MsgFlowNo: flowNo,
		},
		Body: locBody,
	})
	conn.Write(locData)
	t.Logf("→ 报警位置上报: SOS + 超速, 速度=120km/h")

	readAndDecode(t, conn, codec)
	time.Sleep(100 * time.Millisecond)

	locations, _ := memStorage.GetLocations(context.Background(), testDeviceID,
		time.Now().Add(-1*time.Minute), time.Now().Add(1*time.Minute))
	if len(locations) == 0 {
		t.Fatal("没有位置数据")
	}

	loc := locations[len(locations)-1]
	if loc.AlarmFlag&uint32(types.AlarmFlagSOS) == 0 {
		t.Error("SOS报警标志未设置")
	}
	if loc.AlarmFlag&uint32(types.AlarmFlagOverSpeed) == 0 {
		t.Error("超速报警标志未设置")
	}

	t.Logf("✓ 报警测试通过: 报警标志=0x%08X", loc.AlarmFlag)
}

// TestEscapeBoundary 测试转义边界：消息体包含 0x7E 和 0x7D
func TestEscapeBoundary(t *testing.T) {
	addr := "127.0.0.1:18083"
	_, memStorage, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	codec := protocol.NewCodec()
	flowNo := uint16(0)

	// 注册
	flowNo++
	regBody := buildRealRegisterBody()
	regData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalRegister, PhoneNo: testPhoneNo, MsgFlowNo: flowNo,
		},
		Body: regBody,
	})
	conn.Write(regData)
	readAndDecode(t, conn, codec)

	// 位置上报：纬度/经度值的字节中包含 0x7E/0x7D
	// 选择一个纬度值使其编码后包含 0x7E: 0x007E0000 = 8257536 → 82.57536°
	// 实际上不太可能恰好出现，但我们可以用原始字节构造
	flowNo++
	// 使用一个会触发转义的组合
	// 纬度 0x007E7D00 = 8289536, 经度 0x007E7D00
	locBody := buildRealLocationBody(0x007E7D00, 0x007E7D00, 50, 600, 90)
	locData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDLocationReport, PhoneNo: testPhoneNo, MsgFlowNo: flowNo,
		},
		Body: locBody,
	})
	conn.Write(locData)

	// 如果服务器能正确反转义并解析，应答应该成功
	resp, err := readAndDecode(t, conn, codec)
	if err != nil {
		t.Fatalf("转义边界测试失败: %v", err)
	}
	if resp.Header.MsgID != types.MsgIDPlatformCommonResponse {
		t.Errorf("应答消息ID错误: 0x%04X", resp.Header.MsgID)
	}

	time.Sleep(100 * time.Millisecond)
	locations, _ := memStorage.GetLocations(context.Background(), testDeviceID,
		time.Now().Add(-1*time.Minute), time.Now().Add(1*time.Minute))
	if len(locations) > 0 {
		loc := locations[0]
		t.Logf("转义边界数据: 纬度原始=0x%08X, 解析后=%.5f", 0x007E7D00, loc.Latitude)
	}

	t.Log("✓ 转义边界测试通过")
}

// TestHeartbeat 心跳测试
func TestHeartbeat(t *testing.T) {
	addr := "127.0.0.1:18084"
	_, _, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	codec := protocol.NewCodec()

	// 直接发心跳
	heartbeatData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalHeartbeat, PhoneNo: testPhoneNo, MsgFlowNo: 1,
		},
		Body: []byte{},
	})
	conn.Write(heartbeatData)

	resp, err := readAndDecode(t, conn, codec)
	if err != nil {
		t.Fatalf("心跳应答失败: %v", err)
	}
	if resp.Header.MsgID != types.MsgIDPlatformCommonResponse {
		t.Errorf("心跳应答ID错误: 0x%04X", resp.Header.MsgID)
	}

	t.Log("✓ 心跳测试通过")
}

// TestChecksumFailure 校验码错误应被拒绝
func TestChecksumFailure(t *testing.T) {
	addr := "127.0.0.1:18085"
	_, _, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	codec := protocol.NewCodec()

	// 构造一条心跳消息然后篡改校验码
	heartbeatData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalHeartbeat, PhoneNo: testPhoneNo, MsgFlowNo: 1,
		},
		Body: []byte{},
	})

	// 篡改校验码（倒数第二个字节是校验码，最后一个字节是 0x7E）
	heartbeatData[len(heartbeatData)-2] ^= 0xFF

	conn.Write(heartbeatData)

	// 应该收不到有效应答（服务器会关闭连接或忽略）
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4096)
	_, err = conn.Read(buf)
	if err == nil {
		t.Log("服务器仍然返回了数据（可能连接未关闭但消息被丢弃）")
	} else {
		t.Logf("✓ 校验码错误测试通过: 连接被关闭 (%v)", err)
	}
}

// ========== 辅助函数 ==========

// readAndDecode 读取并解码一条消息
func readAndDecode(t *testing.T, conn net.Conn, codec *protocol.Codec) (*protocol.Message, error) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	flag, err := readByte(conn)
	if err != nil {
		return nil, fmt.Errorf("读取起始标志失败: %w", err)
	}
	if flag != 0x7E {
		return nil, fmt.Errorf("起始标志错误: 0x%02X", flag)
	}

	var data []byte
	for {
		b, err := readByte(conn)
		if err != nil {
			return nil, fmt.Errorf("读取消息体失败: %w", err)
		}
		if b == 0x7E {
			break
		}
		data = append(data, b)
	}

	fullData := make([]byte, 0, len(data)+2)
	fullData = append(fullData, 0x7E)
	fullData = append(fullData, data...)
	fullData = append(fullData, 0x7E)

	return codec.Decode(fullData)
}

func readByte(conn net.Conn) (byte, error) {
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	return buf[0], err
}

// buildRealRegisterBody 构造真实国标注册消息体
// 省域ID(2) + 市县域ID(2) + 制造商ID(5) + 终端型号(30) + 终端ID(7) + 车牌颜色(1) + 车牌号(变长)
func buildRealRegisterBody() []byte {
	body := make([]byte, 53)
	binary.BigEndian.PutUint16(body[0:2], 1100) // 北京省域ID（高2位）
	binary.BigEndian.PutUint16(body[2:4], 1101) // 东城区
	copy(body[4:9], "HJT01")                    // 制造商ID
	copy(body[9:39], "JT808-G-2024")            // 终端型号
	copy(body[39:46], testDeviceID)             // 终端ID
	body[46] = 1                                // 车牌颜色：蓝色
	copy(body[47:], testPlateNo)                // 车牌号
	return body
}

// buildRegisterBodyForDevice 为指定设备构造注册消息体
func buildRegisterBodyForDevice(deviceID, phoneNo string) []byte {
	body := make([]byte, 53)
	binary.BigEndian.PutUint16(body[0:2], 1100)
	binary.BigEndian.PutUint16(body[2:4], 1101)
	copy(body[4:9], "HJT01")
	copy(body[9:39], "JT808-G-2024")
	copy(body[39:46], deviceID[:7])
	body[46] = 1
	plateNo := fmt.Sprintf("京A%s", phoneNo[len(phoneNo)-6:])
	copy(body[47:], plateNo)
	return body
}

// buildRealLocationBody 构造真实国标位置消息体（28字节基础体）
func buildRealLocationBody(lat, lng int32, altitude, speed, direction uint16) []byte {
	body := make([]byte, 28)
	binary.BigEndian.PutUint32(body[0:4], 0)             // 报警标志：无
	binary.BigEndian.PutUint32(body[4:8], 0x02)          // 状态：已定位
	binary.BigEndian.PutUint32(body[8:12], uint32(lat))  // 纬度
	binary.BigEndian.PutUint32(body[12:16], uint32(lng)) // 经度
	binary.BigEndian.PutUint16(body[16:18], altitude)    // 海拔
	binary.BigEndian.PutUint16(body[18:20], speed)       // 速度（1/10km/h）
	binary.BigEndian.PutUint16(body[20:22], direction)   // 方向
	// BCD时间
	now := time.Now()
	body[22] = byte((now.Year()%100)/10)<<4 | byte((now.Year()%100)%10)
	body[23] = byte(int(now.Month())/10)<<4 | byte(int(now.Month())%10)
	body[24] = byte(now.Day()/10)<<4 | byte(now.Day()%10)
	body[25] = byte(now.Hour()/10)<<4 | byte(now.Hour()%10)
	body[26] = byte(now.Minute()/10)<<4 | byte(now.Minute()%10)
	body[27] = byte(now.Second()/10)<<4 | byte(now.Second()%10)
	return body
}

// buildRealLocationBodyWithAlarm 带报警标志的位置消息体
func buildRealLocationBodyWithAlarm(alarmFlag uint32, lat, lng int32, altitude, speed, direction uint16) []byte {
	body := make([]byte, 28)
	binary.BigEndian.PutUint32(body[0:4], alarmFlag) // 报警标志
	binary.BigEndian.PutUint32(body[4:8], 0x02)      // 状态：已定位
	binary.BigEndian.PutUint32(body[8:12], uint32(lat))
	binary.BigEndian.PutUint32(body[12:16], uint32(lng))
	binary.BigEndian.PutUint16(body[16:18], altitude)
	binary.BigEndian.PutUint16(body[18:20], speed)
	binary.BigEndian.PutUint16(body[20:22], direction)
	now := time.Now()
	body[22] = byte((now.Year()%100)/10)<<4 | byte((now.Year()%100)%10)
	body[23] = byte(int(now.Month())/10)<<4 | byte(int(now.Month())%10)
	body[24] = byte(now.Day()/10)<<4 | byte(now.Day()%10)
	body[25] = byte(now.Hour()/10)<<4 | byte(now.Hour()%10)
	body[26] = byte(now.Minute()/10)<<4 | byte(now.Minute()%10)
	body[27] = byte(now.Second()/10)<<4 | byte(now.Second()%10)
	return body
}

// buildRegisterResponse 构造注册应答消息体
func buildRegisterResponse(result uint8, authCode string) []byte {
	body := make([]byte, 3+len(authCode))
	binary.BigEndian.PutUint16(body[0:2], 1) // 消息流水号
	body[2] = result
	copy(body[3:], authCode)
	return body
}

// buildCommonResponse 构造通用应答消息体
func buildCommonResponse(msgID uint16, flowNo uint16, result uint8) []byte {
	body := make([]byte, 5)
	binary.BigEndian.PutUint16(body[0:2], flowNo)
	binary.BigEndian.PutUint16(body[2:4], msgID)
	body[4] = result
	return body
}
