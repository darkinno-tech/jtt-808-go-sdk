package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/darkinno-tech/jtt-808-go-sdk/core"
	"github.com/darkinno-tech/jtt-808-go-sdk/metrics"
	"github.com/darkinno-tech/jtt-808-go-sdk/protocol"
	"github.com/darkinno-tech/jtt-808-go-sdk/protocol/types"
	"github.com/darkinno-tech/jtt-808-go-sdk/storage"
	"github.com/darkinno-tech/jtt-808-go-sdk/transport"
)

var (
	metricsCollector *metrics.Metrics
	memoryStorage    *storage.MemoryStorage
	server           *transport.TCPServer
	messageCount     int64
	locationCount    int64
	alarmCount       int64
	registerCount    int64
)

func main() {
	// 设置最大CPU核数
	runtime.GOMAXPROCS(runtime.NumCPU())

	// 创建监控指标
	metricsCollector = metrics.NewMetrics()

	// 创建存储
	memoryStorage = storage.NewMemoryStorage()

	// 创建服务器配置
	config := transport.DefaultConfig()
	config.ListenAddr = ":8080"
	config.MaxConnections = 100000
	config.ReadTimeout = 60 * time.Second
	config.WriteTimeout = 60 * time.Second
	config.IdleTimeout = 600 * time.Second

	// 创建TCP服务器
	server = transport.NewTCPServer(config)

	// 注册消息处理器
	registerHandlers()

	// 注册钩子函数
	registerHooks()

	// 启动HTTP监控服务
	go startHTTPMonitor()

	// 启动服务器
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	log.Printf("JT808 Server started on %s", config.ListenAddr)
	log.Printf("HTTP Monitor started on :8081")

	// 定期打印统计信息
	go printStats()

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down server...")
	if err := server.Stop(); err != nil {
		log.Printf("Failed to stop server: %v", err)
	}
	log.Println("Server stopped")
}

// registerHandlers 注册消息处理器
func registerHandlers() {
	// 终端注册处理器
	server.RegisterHandler(types.MsgIDTerminalRegister, handleTerminalRegister)

	// 终端鉴权处理器
	server.RegisterHandler(types.MsgIDTerminalAuth, handleTerminalAuth)

	// 终端心跳处理器
	server.RegisterHandler(types.MsgIDTerminalHeartbeat, handleTerminalHeartbeat)

	// 位置信息汇报处理器
	server.RegisterHandler(types.MsgIDLocationReport, handleLocationReport)

	// 终端通用应答处理器
	server.RegisterHandler(types.MsgIDTerminalCommonResponse, handleTerminalCommonResponse)
}

// registerHooks 注册钩子函数
func registerHooks() {
	// 连接建立钩子
	server.OnConnect(func(conn core.Connection) error {
		metricsCollector.IncrActiveConnections()
		log.Printf("[CONNECT] Device connected: %s", conn.RemoteAddr())
		return nil
	})

	// 连接断开钩子
	server.OnDisconnect(func(conn core.Connection) error {
		metricsCollector.DecrActiveConnections()
		if conn.DeviceID() != "" {
			memoryStorage.UpdateDeviceStatus(context.Background(), conn.DeviceID(), core.DeviceStatusOffline)
		}
		log.Printf("[DISCONNECT] Device disconnected: %s", conn.DeviceID())
		return nil
	})

	// 错误处理钩子
	server.OnError(func(conn core.Connection, err error) {
		metricsCollector.IncrMessageErrors()
		log.Printf("[ERROR] Device %s: %v", conn.DeviceID(), err)
	})
}

// handleTerminalRegister 处理终端注册
func handleTerminalRegister(ctx context.Context, conn core.Connection, msg *core.Message) error {
	reg, err := protocol.ParseTerminalRegister(msg.Body)
	if err != nil {
		return fmt.Errorf("parse terminal register failed: %w", err)
	}

	// 保存设备信息（ParseTerminalRegister 已返回 *core.TerminalRegister）
	deviceID := reg.TerminalID
	ctx = context.WithValue(ctx, "deviceID", deviceID)
	if err := memoryStorage.SaveDevice(ctx, deviceID, reg); err != nil {
		return fmt.Errorf("save device failed: %w", err)
	}

	// 设置设备ID
	if tcpConn, ok := conn.(*transport.TCPConnection); ok {
		tcpConn.SetDeviceID(deviceID)
	}

	atomic.AddInt64(&registerCount, 1)
	metricsCollector.IncrReceivedMessages()

	log.Printf("[REGISTER] Device %s registered, plate: %s", deviceID, reg.PlateNo)

	// 发送注册应答
	response := &core.Message{
		Header: &core.MessageHeader{
			MsgID:     types.MsgIDTerminalRegisterResponse,
			PhoneNo:   msg.Header.PhoneNo,
			MsgFlowNo: msg.Header.MsgFlowNo,
		},
		Body: buildRegisterResponse(0, "AUTH123456"),
	}

	return conn.Send(response)
}

// handleTerminalAuth 处理终端鉴权
func handleTerminalAuth(ctx context.Context, conn core.Connection, msg *core.Message) error {
	metricsCollector.IncrReceivedMessages()

	log.Printf("[AUTH] Device %s authenticated", conn.DeviceID())

	// 发送通用应答
	response := &core.Message{
		Header: &core.MessageHeader{
			MsgID:     types.MsgIDPlatformCommonResponse,
			PhoneNo:   msg.Header.PhoneNo,
			MsgFlowNo: msg.Header.MsgFlowNo,
		},
		Body: buildCommonResponse(msg.Header.MsgID, msg.Header.MsgFlowNo, 0),
	}

	return conn.Send(response)
}

// handleTerminalHeartbeat 处理终端心跳
func handleTerminalHeartbeat(ctx context.Context, conn core.Connection, msg *core.Message) error {
	metricsCollector.IncrReceivedMessages()

	// 更新设备状态
	if conn.DeviceID() != "" {
		memoryStorage.UpdateDeviceStatus(ctx, conn.DeviceID(), core.DeviceStatusOnline)
	}

	// 发送通用应答
	response := &core.Message{
		Header: &core.MessageHeader{
			MsgID:     types.MsgIDPlatformCommonResponse,
			PhoneNo:   msg.Header.PhoneNo,
			MsgFlowNo: msg.Header.MsgFlowNo,
		},
		Body: buildCommonResponse(msg.Header.MsgID, msg.Header.MsgFlowNo, 0),
	}

	return conn.Send(response)
}

// handleLocationReport 处理位置信息汇报
func handleLocationReport(ctx context.Context, conn core.Connection, msg *core.Message) error {
	report, err := protocol.ParseLocationReport(msg.Body)
	if err != nil {
		return fmt.Errorf("parse location report failed: %w", err)
	}

	// 保存位置信息（ParseLocationReport 已返回 *core.LocationReport）
	deviceID := conn.DeviceID()
	ctx = context.WithValue(ctx, "deviceID", deviceID)
	if err := memoryStorage.SaveLocation(ctx, report); err != nil {
		return fmt.Errorf("save location failed: %w", err)
	}

	atomic.AddInt64(&locationCount, 1)
	metricsCollector.IncrReceivedMessages()

	// 检查报警
	if report.AlarmFlag != 0 {
		atomic.AddInt64(&alarmCount, 1)
		log.Printf("[ALARM] Device %s: alarm flag %08x", deviceID, report.AlarmFlag)
	}

	// 发送通用应答
	response := &core.Message{
		Header: &core.MessageHeader{
			MsgID:     types.MsgIDPlatformCommonResponse,
			PhoneNo:   msg.Header.PhoneNo,
			MsgFlowNo: msg.Header.MsgFlowNo,
		},
		Body: buildCommonResponse(msg.Header.MsgID, msg.Header.MsgFlowNo, 0),
	}

	return conn.Send(response)
}

// handleTerminalCommonResponse 处理终端通用应答
func handleTerminalCommonResponse(ctx context.Context, conn core.Connection, msg *core.Message) error {
	metricsCollector.IncrReceivedMessages()
	return nil
}

// buildRegisterResponse 构建注册应答
func buildRegisterResponse(result uint8, authCode string) []byte {
	body := make([]byte, 3+len(authCode))
	// 消息流水号（2字节）
	body[0] = 0x00
	body[1] = 0x01
	// 结果（1字节）
	body[2] = result
	// 鉴权码
	copy(body[3:], authCode)
	return body
}

// buildCommonResponse 构建通用应答
func buildCommonResponse(msgID uint16, flowNo uint16, result uint8) []byte {
	body := make([]byte, 5)
	// 应答消息流水号（2字节）
	body[0] = byte(flowNo >> 8)
	body[1] = byte(flowNo)
	// 应答ID（2字节）
	body[2] = byte(msgID >> 8)
	body[3] = byte(msgID)
	// 结果（1字节）
	body[4] = result
	return body
}

// printStats 定期打印统计信息
func printStats() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		stats := metricsCollector.GetStats()
		storageStats := memoryStorage.GetStats()

		log.Printf("[STATS] Active: %d | Total: %d | Messages: %d | Locations: %d | Alarms: %d | Registers: %d | Errors: %d | Goroutines: %d",
			stats.ActiveConnections,
			stats.TotalConnections,
			stats.ReceivedMessages,
			atomic.LoadInt64(&locationCount),
			atomic.LoadInt64(&alarmCount),
			atomic.LoadInt64(&registerCount),
			stats.MessageErrors,
			runtime.NumGoroutine(),
		)

		log.Printf("[STORAGE] Devices: %d | Locations: %d | Alarms: %d",
			storageStats.DeviceCount,
			storageStats.LocationCount,
			storageStats.AlarmCount,
		)
	}
}

// startHTTPMonitor 启动HTTP监控服务
func startHTTPMonitor() {
	http.HandleFunc("/stats", handleStats)
	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/devices", handleDevices)
	http.HandleFunc("/metrics", handleMetrics)

	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Printf("HTTP monitor failed: %v", err)
	}
}

// handleStats 处理统计信息请求
func handleStats(w http.ResponseWriter, r *http.Request) {
	stats := metricsCollector.GetStats()
	storageStats := memoryStorage.GetStats()

	data := map[string]interface{}{
		"server": map[string]interface{}{
			"active_connections": stats.ActiveConnections,
			"total_connections":  stats.TotalConnections,
			"received_messages":  stats.ReceivedMessages,
			"sent_messages":      stats.SentMessages,
			"message_errors":     stats.MessageErrors,
			"uptime":             stats.Uptime.String(),
		},
		"protocol": map[string]interface{}{
			"locations": atomic.LoadInt64(&locationCount),
			"alarms":    atomic.LoadInt64(&alarmCount),
			"registers": atomic.LoadInt64(&registerCount),
		},
		"storage": map[string]interface{}{
			"devices":   storageStats.DeviceCount,
			"locations": storageStats.LocationCount,
			"alarms":    storageStats.AlarmCount,
		},
		"runtime": map[string]interface{}{
			"goroutines": runtime.NumGoroutine(),
			"cpu_count":  runtime.NumCPU(),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// handleHealth 处理健康检查请求
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleDevices 处理设备列表请求
func handleDevices(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "device list endpoint"})
}

// handleMetrics 处理Prometheus指标请求
func handleMetrics(w http.ResponseWriter, r *http.Request) {
	stats := metricsCollector.GetStats()

	metrics := fmt.Sprintf(`# HELP jt808_active_connections Active connections
# TYPE jt808_active_connections gauge
jt808_active_connections %d
# HELP jt808_total_connections Total connections
# TYPE jt808_total_connections counter
jt808_total_connections %d
# HELP jt808_received_messages Total received messages
# TYPE jt808_received_messages counter
jt808_received_messages %d
# HELP jt808_sent_messages Total sent messages
# TYPE jt808_sent_messages counter
jt808_sent_messages %d
# HELP jt808_message_errors Total message errors
# TYPE jt808_message_errors counter
jt808_message_errors %d
# HELP jt808_locations Total location reports
# TYPE jt808_locations counter
jt808_locations %d
# HELP jt808_alarms Total alarm reports
# TYPE jt808_alarms counter
jt808_alarms %d
# HELP jt808_registers Total register requests
# TYPE jt808_registers counter
jt808_registers %d
`,
		stats.ActiveConnections,
		stats.TotalConnections,
		stats.ReceivedMessages,
		stats.SentMessages,
		stats.MessageErrors,
		atomic.LoadInt64(&locationCount),
		atomic.LoadInt64(&alarmCount),
		atomic.LoadInt64(&registerCount),
	)

	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(metrics))
}
