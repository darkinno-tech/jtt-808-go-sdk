package integration

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/darkinno-tech/jtt-808-go-sdk/protocol"
	"github.com/darkinno-tech/jtt-808-go-sdk/protocol/types"
)

// TestE2E_CompleteTerminalLifecycle 测试终端完整生命周期：
// 连接 → 注册 → 鉴权 → 心跳 → 多次位置上报 → 断开
func TestE2E_CompleteTerminalLifecycle(t *testing.T) {
	addr := "127.0.0.1:19001"
	_, memStorage, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("连接服务器失败: %v", err)
	}
	defer conn.Close()

	codec := protocol.NewCodec()
	flowNo := uint16(0)
	phoneNo := "13900001111"
	deviceID := "E2EDV01"

	// === 1. 终端注册 ===
	flowNo++
	regBody := buildRegisterBodyForDevice(deviceID, phoneNo)
	regData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalRegister, PhoneNo: phoneNo, MsgFlowNo: flowNo,
		},
		Body: regBody,
	})
	if _, err := conn.Write(regData); err != nil {
		t.Fatalf("发送注册消息失败: %v", err)
	}

	regResp, err := readAndDecode(t, conn, codec)
	if err != nil {
		t.Fatalf("读取注册应答失败: %v", err)
	}
	if regResp.Header.MsgID != types.MsgIDTerminalRegisterResponse {
		t.Errorf("注册应答消息ID错误: 0x%04X", regResp.Header.MsgID)
	}
	t.Logf("[1/5] 注册成功")

	// === 2. 终端鉴权 ===
	flowNo++
	authData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalAuth, PhoneNo: phoneNo, MsgFlowNo: flowNo,
		},
		Body: []byte("AUTH_TOKEN_2024"),
	})
	conn.Write(authData)

	authResp, err := readAndDecode(t, conn, codec)
	if err != nil {
		t.Fatalf("读取鉴权应答失败: %v", err)
	}
	if authResp.Header.MsgID != types.MsgIDPlatformCommonResponse {
		t.Errorf("鉴权应答消息ID错误: 0x%04X", authResp.Header.MsgID)
	}
	t.Logf("[2/5] 鉴权成功")

	// === 3. 心跳 ===
	flowNo++
	heartbeatData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalHeartbeat, PhoneNo: phoneNo, MsgFlowNo: flowNo,
		},
		Body: []byte{},
	})
	conn.Write(heartbeatData)

	hbResp, err := readAndDecode(t, conn, codec)
	if err != nil {
		t.Fatalf("读取心跳应答失败: %v", err)
	}
	if hbResp.Header.MsgID != types.MsgIDPlatformCommonResponse {
		t.Errorf("心跳应答消息ID错误: 0x%04X", hbResp.Header.MsgID)
	}
	t.Logf("[3/5] 心跳成功")

	// === 4. 连续位置上报（模拟行驶轨迹） ===
	waypoints := []struct {
		lat, lng int32
		speed    uint16
		dir      uint16
	}{
		{3990420, 11640740, 600, 90}, // 天安门
		{3990520, 11640840, 400, 95}, // 向东北移动
		{3990620, 11640940, 300, 100},
		{3990720, 11641040, 500, 110},
		{3990820, 11641140, 700, 120},
	}

	for i, wp := range waypoints {
		flowNo++
		locBody := buildRealLocationBody(wp.lat, wp.lng, 50, wp.speed, wp.dir)
		locData, _ := codec.Encode(&protocol.Message{
			Header: &protocol.MessageHeader{
				MsgID: types.MsgIDLocationReport, PhoneNo: phoneNo, MsgFlowNo: flowNo,
			},
			Body: locBody,
		})
		conn.Write(locData)
		readAndDecode(t, conn, codec)
		t.Logf("[4/5] 位置上报 #%d: 纬度=%.5f, 经度=%.5f, 速度=%d",
			i+1, float64(wp.lat)/100000.0, float64(wp.lng)/100000.0, wp.speed/10)
	}

	// === 5. 验证存储数据 ===
	time.Sleep(100 * time.Millisecond)

	locations, err := memStorage.GetLocations(context.Background(), deviceID,
		time.Now().Add(-1*time.Minute), time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("获取位置数据失败: %v", err)
	}
	if len(locations) != len(waypoints) {
		t.Errorf("位置数据数量不匹配: 期望%d, 实际%d", len(waypoints), len(locations))
	}

	// 验证最后一条位置
	last := locations[len(locations)-1]
	if last.Latitude < 39.908 || last.Latitude > 39.909 {
		t.Errorf("最后位置纬度异常: %f", last.Latitude)
	}
	t.Logf("[5/5] 存储验证通过: 共%d条位置记录", len(locations))
	t.Log("✓ 完整终端生命周期测试通过")
}

// TestE2E_LocationStreaming 测试连续位置流上报并验证轨迹数据
func TestE2E_LocationStreaming(t *testing.T) {
	addr := "127.0.0.1:19002"
	_, memStorage, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	codec := protocol.NewCodec()
	phoneNo := "13900002222"
	deviceID := "E2EDV02"

	// 注册
	regData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalRegister, PhoneNo: phoneNo, MsgFlowNo: 1,
		},
		Body: buildRegisterBodyForDevice(deviceID, phoneNo),
	})
	conn.Write(regData)
	readAndDecode(t, conn, codec)

	// 模拟从北京到天津的行驶轨迹（100个点）
	totalPoints := 100
	startLat, startLng := int32(3990420), int32(11640740) // 北京
	endLat, endLng := int32(3908420), int32(11720740)     // 天津

	for i := 0; i < totalPoints; i++ {
		lat := startLat + (endLat-startLat)*int32(i)/int32(totalPoints)
		lng := startLng + (endLng-startLng)*int32(i)/int32(totalPoints)
		speed := uint16(800 + (i%5)*100) // 80-120 km/h 波动

		locBody := buildRealLocationBody(lat, lng, 30, speed, uint16(45+i%90))
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
	if len(locations) < totalPoints*9/10 {
		t.Fatalf("位置数据数量不匹配: 期望>=%d, 实际%d", totalPoints*9/10, len(locations))
	}

	// 验证轨迹起终点
	first := locations[0]
	last := locations[len(locations)-1]
	if first.Latitude < 39.9 || first.Latitude > 39.91 {
		t.Errorf("起点纬度异常: %f", first.Latitude)
	}
	if last.Latitude < 39.09 || last.Latitude > 39.10 {
		t.Errorf("终点纬度异常: %f", last.Latitude)
	}

	// 验证轨迹单调递减（北京到天津，纬度递减）
	for i := 1; i < len(locations); i++ {
		if locations[i].Latitude > locations[i-1].Latitude+0.001 {
			t.Errorf("轨迹非单调递减: 点%d=%f > 点%d=%f",
				i, locations[i].Latitude, i-1, locations[i-1].Latitude)
			break
		}
	}

	t.Logf("✓ 位置流测试通过: %d个轨迹点, 起点=(%.5f,%.5f), 终点=(%.5f,%.5f)",
		totalPoints, first.Latitude, first.Longitude, last.Latitude, last.Longitude)
}

// TestE2E_ConcurrentClients_50Devices 测试50台设备并发完成完整生命周期
func TestE2E_ConcurrentClients_50Devices(t *testing.T) {
	addr := "127.0.0.1:19003"
	_, memStorage, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(200 * time.Millisecond)

	deviceCount := 50
	var wg sync.WaitGroup
	var errCount int64
	var successCount int64

	for i := 0; i < deviceCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			phoneNo := fmt.Sprintf("139%08d", id)
			deviceID := fmt.Sprintf("E2E%04d", id)

			conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
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
			if _, err := conn.Write(regData); err != nil {
				atomic.AddInt64(&errCount, 1)
				return
			}
			if _, err := readAndDecode(t, conn, codec); err != nil {
				atomic.AddInt64(&errCount, 1)
				return
			}

			// 鉴权
			authData, _ := codec.Encode(&protocol.Message{
				Header: &protocol.MessageHeader{
					MsgID: types.MsgIDTerminalAuth, PhoneNo: phoneNo, MsgFlowNo: 2,
				},
				Body: []byte("AUTH_TOKEN_2024"),
			})
			conn.Write(authData)
			if _, err := readAndDecode(t, conn, codec); err != nil {
				atomic.AddInt64(&errCount, 1)
				return
			}

			// 位置上报（每个设备3条）
			for j := 0; j < 3; j++ {
				lat := beijingLat + int32(id*100) + int32(j*50)
				lng := beijingLng + int32(id*100) + int32(j*50)
				locBody := buildRealLocationBody(lat, lng, 30+uint16(id), 600, 0)
				locData, _ := codec.Encode(&protocol.Message{
					Header: &protocol.MessageHeader{
						MsgID: types.MsgIDLocationReport, PhoneNo: phoneNo, MsgFlowNo: uint16(j + 3),
					},
					Body: locBody,
				})
				conn.Write(locData)
				if _, err := readAndDecode(t, conn, codec); err != nil {
					atomic.AddInt64(&errCount, 1)
					return
				}
			}

			// 心跳
			hbData, _ := codec.Encode(&protocol.Message{
				Header: &protocol.MessageHeader{
					MsgID: types.MsgIDTerminalHeartbeat, PhoneNo: phoneNo, MsgFlowNo: 6,
				},
				Body: []byte{},
			})
			conn.Write(hbData)
			if _, err := readAndDecode(t, conn, codec); err != nil {
				atomic.AddInt64(&errCount, 1)
				return
			}

			atomic.AddInt64(&successCount, 1)
		}(i)
	}

	wg.Wait()

	if errCount > int64(deviceCount/10) {
		t.Errorf("出错设备数过多: %d/%d", errCount, deviceCount)
	}

	time.Sleep(300 * time.Millisecond)

	// 验证部分设备数据
	for i := 0; i < 5; i++ {
		deviceID := fmt.Sprintf("E2E%04d", i)
		locations, _ := memStorage.GetLocations(context.Background(), deviceID,
			time.Now().Add(-1*time.Minute), time.Now().Add(1*time.Minute))
		if len(locations) == 0 {
			t.Errorf("设备%d没有位置数据", i)
		}
	}

	t.Logf("✓ 50设备并发测试通过: 成功=%d, 失败=%d", successCount, errCount)
}

// TestE2E_Reconnection 测试终端断线重连
func TestE2E_Reconnection(t *testing.T) {
	addr := "127.0.0.1:19004"
	_, memStorage, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	codec := protocol.NewCodec()
	phoneNo := "13900003333"
	deviceID := "E2EDV03"

	// === 第一次连接：注册 + 位置上报 ===
	conn1, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("第一次连接失败: %v", err)
	}

	// 注册
	regData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalRegister, PhoneNo: phoneNo, MsgFlowNo: 1,
		},
		Body: buildRegisterBodyForDevice(deviceID, phoneNo),
	})
	conn1.Write(regData)
	readAndDecode(t, conn1, codec)

	// 上报位置1
	locBody1 := buildRealLocationBody(beijingLat, beijingLng, 50, 600, 90)
	locData1, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDLocationReport, PhoneNo: phoneNo, MsgFlowNo: 2,
		},
		Body: locBody1,
	})
	conn1.Write(locData1)
	readAndDecode(t, conn1, codec)

	conn1.Close()
	t.Log("[1/3] 第一次连接完成: 注册 + 1条位置")
	time.Sleep(200 * time.Millisecond)

	// === 第二次连接：重新注册 + 继续上报 ===
	conn2, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("第二次连接失败: %v", err)
	}
	defer conn2.Close()

	// 重新注册
	regData2, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalRegister, PhoneNo: phoneNo, MsgFlowNo: 1,
		},
		Body: buildRegisterBodyForDevice(deviceID, phoneNo),
	})
	conn2.Write(regData2)
	readAndDecode(t, conn2, codec)

	// 上报位置2
	locBody2 := buildRealLocationBody(beijingLat+200, beijingLng+200, 55, 800, 100)
	locData2, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDLocationReport, PhoneNo: phoneNo, MsgFlowNo: 2,
		},
		Body: locBody2,
	})
	conn2.Write(locData2)
	readAndDecode(t, conn2, codec)
	t.Log("[2/3] 第二次连接完成: 重新注册 + 1条位置")

	// === 验证两次连接的数据都存在 ===
	time.Sleep(100 * time.Millisecond)
	locations, _ := memStorage.GetLocations(context.Background(), deviceID,
		time.Now().Add(-1*time.Minute), time.Now().Add(1*time.Minute))
	if len(locations) < 2 {
		t.Fatalf("位置数据不足: 期望>=2, 实际%d", len(locations))
	}

	t.Logf("[3/3] 存储验证: 共%d条位置记录", len(locations))
	t.Log("✓ 断线重连测试通过")
}

// TestE2E_MixedScenario 混合场景：多个设备同时上报不同类型消息
func TestE2E_MixedScenario(t *testing.T) {
	addr := "127.0.0.1:19005"
	_, memStorage, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	deviceCount := 10
	var wg sync.WaitGroup

	for i := 0; i < deviceCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			phoneNo := fmt.Sprintf("139%08d", id)
			deviceID := fmt.Sprintf("MIX%04d", id)

			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()

			codec := protocol.NewCodec()
			flowNo := uint16(0)

			// 注册
			flowNo++
			regData, _ := codec.Encode(&protocol.Message{
				Header: &protocol.MessageHeader{
					MsgID: types.MsgIDTerminalRegister, PhoneNo: phoneNo, MsgFlowNo: flowNo,
				},
				Body: buildRegisterBodyForDevice(deviceID, phoneNo),
			})
			conn.Write(regData)
			readAndDecode(t, conn, codec)

			// 位置上报 + 心跳交替
			for j := 0; j < 5; j++ {
				if j%2 == 0 {
					// 位置上报
					flowNo++
					lat := beijingLat + int32(id*100) + int32(j*10)
					lng := beijingLng + int32(id*100) + int32(j*10)
					locBody := buildRealLocationBody(lat, lng, 30, 600, 0)
					locData, _ := codec.Encode(&protocol.Message{
						Header: &protocol.MessageHeader{
							MsgID: types.MsgIDLocationReport, PhoneNo: phoneNo, MsgFlowNo: flowNo,
						},
						Body: locBody,
					})
					conn.Write(locData)
				} else {
					// 心跳
					flowNo++
					hbData, _ := codec.Encode(&protocol.Message{
						Header: &protocol.MessageHeader{
							MsgID: types.MsgIDTerminalHeartbeat, PhoneNo: phoneNo, MsgFlowNo: flowNo,
						},
						Body: []byte{},
					})
					conn.Write(hbData)
				}
				readAndDecode(t, conn, codec)
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	// 验证所有设备都有位置数据（应为3条：j=0,2,4）
	for i := 0; i < deviceCount; i++ {
		deviceID := fmt.Sprintf("MIX%04d", i)
		locations, _ := memStorage.GetLocations(context.Background(), deviceID,
			time.Now().Add(-1*time.Minute), time.Now().Add(1*time.Minute))
		if len(locations) != 3 {
			t.Errorf("设备%d位置数据不匹配: 期望3条, 实际%d条", i, len(locations))
		}
	}

	t.Logf("✓ 混合场景测试通过: %d台设备, 每台3条位置+2条心跳", deviceCount)
}

// TestE2E_ContinuousHeartbeat 测试持续心跳保活
func TestE2E_ContinuousHeartbeat(t *testing.T) {
	addr := "127.0.0.1:19006"
	_, _, cleanup := startTestServer(t, addr)
	defer cleanup()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	codec := protocol.NewCodec()
	phoneNo := "13900004444"

	// 连续发送20次心跳
	heartbeatCount := 20
	for i := 0; i < heartbeatCount; i++ {
		hbData, _ := codec.Encode(&protocol.Message{
			Header: &protocol.MessageHeader{
				MsgID: types.MsgIDTerminalHeartbeat, PhoneNo: phoneNo, MsgFlowNo: uint16(i + 1),
			},
			Body: []byte{},
		})
		if _, err := conn.Write(hbData); err != nil {
			t.Fatalf("心跳#%d发送失败: %v", i+1, err)
		}

		resp, err := readAndDecode(t, conn, codec)
		if err != nil {
			t.Fatalf("心跳#%d应答失败: %v", i+1, err)
		}
		if resp.Header.MsgID != types.MsgIDPlatformCommonResponse {
			t.Errorf("心跳#%d应答ID错误: 0x%04X", i+1, resp.Header.MsgID)
		}
	}

	t.Logf("✓ 持续心跳测试通过: 连续%d次心跳应答成功", heartbeatCount)
}

// TestE2E_ServerRestart 测试服务器重启后客户端可重新连接
func TestE2E_ServerRestart(t *testing.T) {
	addr := "127.0.0.1:19007"
	codec := protocol.NewCodec()
	phoneNo := "13900005555"
	deviceID := "E2EDV05"

	// === 第一轮：启动服务器，注册 + 上报 ===
	_, memStorage, cleanup1 := startTestServer(t, addr)
	time.Sleep(100 * time.Millisecond)

	conn1, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("第一轮连接失败: %v", err)
	}

	regData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalRegister, PhoneNo: phoneNo, MsgFlowNo: 1,
		},
		Body: buildRegisterBodyForDevice(deviceID, phoneNo),
	})
	conn1.Write(regData)
	readAndDecode(t, conn1, codec)

	locBody := buildRealLocationBody(beijingLat, beijingLng, 50, 600, 90)
	locData, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDLocationReport, PhoneNo: phoneNo, MsgFlowNo: 2,
		},
		Body: locBody,
	})
	conn1.Write(locData)
	readAndDecode(t, conn1, codec)
	conn1.Close()

	time.Sleep(100 * time.Millisecond)
	locations1, _ := memStorage.GetLocations(context.Background(), deviceID,
		time.Now().Add(-1*time.Minute), time.Now().Add(1*time.Minute))
	if len(locations1) != 1 {
		t.Fatalf("第一轮位置数据异常: %d", len(locations1))
	}
	t.Log("[1/3] 第一轮完成: 注册 + 1条位置")

	// === 停止服务器 ===
	cleanup1()
	time.Sleep(300 * time.Millisecond)
	t.Log("[2/3] 服务器已停止")

	// === 第二轮：重新启动服务器，连接注册 ===
	_, _, cleanup2 := startTestServer(t, addr)
	defer cleanup2()
	time.Sleep(100 * time.Millisecond)

	conn2, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("第二轮连接失败: %v", err)
	}
	defer conn2.Close()

	regData2, _ := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID: types.MsgIDTerminalRegister, PhoneNo: phoneNo, MsgFlowNo: 1,
		},
		Body: buildRegisterBodyForDevice(deviceID, phoneNo),
	})
	conn2.Write(regData2)
	regResp2, err := readAndDecode(t, conn2, codec)
	if err != nil {
		t.Fatalf("第二轮注册应答失败: %v", err)
	}
	if regResp2.Header.MsgID != types.MsgIDTerminalRegisterResponse {
		t.Errorf("第二轮注册应答ID错误: 0x%04X", regResp2.Header.MsgID)
	}

	t.Log("[3/3] 第二轮完成: 重新注册成功")
	t.Log("✓ 服务器重启测试通过")
}
