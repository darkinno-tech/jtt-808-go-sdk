package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/im10furry/jtt-808-go-sdk/core"
	"github.com/im10furry/jtt-808-go-sdk/logger"
	"github.com/im10furry/jtt-808-go-sdk/metrics"
	"github.com/im10furry/jtt-808-go-sdk/protocol"
	"github.com/im10furry/jtt-808-go-sdk/protocol/types"
	"github.com/im10furry/jtt-808-go-sdk/storage"
	"github.com/im10furry/jtt-808-go-sdk/transport"
)

func main() {
	// 设置最大CPU核数
	runtime.GOMAXPROCS(runtime.NumCPU())

	// 创建日志记录器
	log := logger.NewLogger(logger.InfoLevel)

	// 创建监控指标
	metrics := metrics.NewMetrics()

	// 创建存储
	storage := storage.NewMemoryStorage()

	// 创建高性能服务器配置
	config := transport.DefaultHighPerfConfig()
	config.ListenAddr = ":8080"
	config.MaxConnections = 200000
	config.MinWorkers = 1000
	config.MaxWorkers = 50000
	config.WorkerQueueSize = 1000000
	config.ConnPoolShardCount = 256
	config.AcceptParallel = runtime.NumCPU()
	config.ReadBufferSize = 8192
	config.WriteBufferSize = 8192
	config.EnableTCPNoDelay = true
	config.EnableTCPKeepAlive = true

	// 创建高性能TCP服务器
	server := transport.NewHighPerfTCPServer(config)

	// 注册消息处理器
	server.RegisterHandler(types.MsgIDLocationReport, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		// 解析位置信息
		report, err := protocol.ParseLocationReport(msg.Body)
		if err != nil {
			log.Error("Failed to parse location report", logger.Error("error", err))
			metrics.IncrMessageErrors()
			return err
		}

		// 保存位置信息
		ctx = context.WithValue(ctx, "deviceID", conn.DeviceID())
		if err := storage.SaveLocation(ctx, report); err != nil {
			log.Error("Failed to save location", logger.Error("error", err))
			metrics.IncrMessageErrors()
			return err
		}

		metrics.IncrReceivedMessages()

		// 发送通用应答
		return conn.Send(&core.Message{
			Header: &core.MessageHeader{
				MsgID:     types.MsgIDPlatformCommonResponse,
				PhoneNo:   msg.Header.PhoneNo,
				MsgFlowNo: msg.Header.MsgFlowNo,
			},
			Body: buildCommonResponse(msg.Header.MsgID, msg.Header.MsgFlowNo, 0x00),
		})
	})

	// 注册终端注册处理器
	server.RegisterHandler(types.MsgIDTerminalRegister, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		// 解析终端注册信息
		reg, err := protocol.ParseTerminalRegister(msg.Body)
		if err != nil {
			log.Error("Failed to parse terminal register", logger.Error("error", err))
			metrics.IncrMessageErrors()
			return err
		}

		// 先设置设备ID
		if tcpConn, ok := conn.(*transport.HighPerfTCPConnection); ok {
			tcpConn.SetDeviceID(reg.TerminalID)
		}

		// 保存设备信息
		ctx = context.WithValue(ctx, "deviceID", conn.DeviceID())
		if err := storage.SaveDevice(ctx, conn.DeviceID(), reg); err != nil {
			log.Error("Failed to save device", logger.Error("error", err))
			metrics.IncrMessageErrors()
			return err
		}

		metrics.IncrReceivedMessages()

		// 发送注册应答
		return conn.Send(&core.Message{
			Header: &core.MessageHeader{
				MsgID:     types.MsgIDTerminalRegisterResponse,
				PhoneNo:   msg.Header.PhoneNo,
				MsgFlowNo: msg.Header.MsgFlowNo,
			},
			Body: buildRegisterResponse(0x00, "AUTH_TOKEN_2024"),
		})
	})

	// 注册心跳处理器
	server.RegisterHandler(types.MsgIDTerminalHeartbeat, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		metrics.IncrReceivedMessages()

		// 发送通用应答
		return conn.Send(&core.Message{
			Header: &core.MessageHeader{
				MsgID:     types.MsgIDPlatformCommonResponse,
				PhoneNo:   msg.Header.PhoneNo,
				MsgFlowNo: msg.Header.MsgFlowNo,
			},
			Body: buildCommonResponse(msg.Header.MsgID, msg.Header.MsgFlowNo, 0x00),
		})
	})

	// 注册连接建立钩子
	server.OnConnect(func(conn core.Connection) error {
		metrics.IncrActiveConnections()
		return nil
	})

	// 注册连接断开钩子
	server.OnDisconnect(func(conn core.Connection) error {
		metrics.DecrActiveConnections()
		return nil
	})

	// 启动服务器
	if err := server.Start(); err != nil {
		log.Fatal("Failed to start server", logger.Error("error", err))
	}

	log.Info("High performance server started",
		logger.String("addr", config.ListenAddr),
		logger.Int("maxConnections", config.MaxConnections),
		logger.Int("minWorkers", config.MinWorkers),
		logger.Int("maxWorkers", config.MaxWorkers),
		logger.Int("acceptParallel", config.AcceptParallel),
	)

	// 定期打印统计信息
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				stats := server.GetHighPerfStats()
				log.Info("Server stats",
					logger.Int64("activeConnections", stats.ActiveConnections),
					logger.Int64("totalConnections", stats.TotalConnections),
					logger.Int64("receivedMessages", stats.ReceivedMessages),
					logger.Int64("sentMessages", stats.SentMessages),
					logger.Int64("errorCount", stats.ErrorCount),
					logger.Int64("workerPoolActive", stats.WorkerPoolStats.ActiveWorkers),
					logger.Int("workerPoolQueue", stats.WorkerPoolStats.QueueLength),
				)

				// 打印内存统计
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				log.Info("Memory stats",
					logger.Float64("allocMB", float64(m.Alloc)/1024/1024),
					logger.Float64("sysMB", float64(m.Sys)/1024/1024),
					logger.Int("gcCount", int(m.NumGC)),
				)
			}
		}
	}()

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down server...")
	if err := server.Stop(); err != nil {
		log.Error("Failed to stop server", logger.Error("error", err))
	}
	log.Info("Server stopped")
}

// buildCommonResponse 构造通用应答消息体
func buildCommonResponse(msgID uint16, flowNo uint16, result uint8) []byte {
	body := make([]byte, 5)
	binary.BigEndian.PutUint16(body[0:2], flowNo)
	binary.BigEndian.PutUint16(body[2:4], msgID)
	body[4] = result
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
