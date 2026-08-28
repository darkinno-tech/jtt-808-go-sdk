package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/im10furry/jtt-808-go-sdk/core"
	"github.com/im10furry/jtt-808-go-sdk/logger"
	"github.com/im10furry/jtt-808-go-sdk/metrics"
	"github.com/im10furry/jtt-808-go-sdk/middleware"
	"github.com/im10furry/jtt-808-go-sdk/protocol"
	"github.com/im10furry/jtt-808-go-sdk/protocol/types"
	"github.com/im10furry/jtt-808-go-sdk/storage"
	"github.com/im10furry/jtt-808-go-sdk/transport"
)

// CustomStorage 自定义存储实现示例
type CustomStorage struct {
	*storage.MemoryStorage
}

// NewCustomStorage 创建自定义存储
func NewCustomStorage() *CustomStorage {
	return &CustomStorage{
		MemoryStorage: storage.NewMemoryStorage(),
	}
}

// SaveLocation 重写保存位置信息方法，添加自定义逻辑
func (s *CustomStorage) SaveLocation(ctx context.Context, loc *core.LocationReport) error {
	// 在这里可以添加自定义逻辑，比如数据验证、转换等
	deviceID, _ := ctx.Value("deviceID").(string)
	if deviceID == "" {
		return fmt.Errorf("设备ID不能为空")
	}

	// 调用原始存储方法
	return s.MemoryStorage.SaveLocation(ctx, loc)
}

func main() {
	// 创建日志记录器
	log := logger.NewLogger(logger.InfoLevel)

	// 创建监控指标
	metrics := metrics.NewMetrics()

	// 创建自定义存储
	customStorage := NewCustomStorage()

	// 创建服务器配置
	config := transport.DefaultConfig()
	config.ListenAddr = ":8080"
	config.MaxConnections = 50000
	config.ReadTimeout = 60 * time.Second
	config.WriteTimeout = 60 * time.Second

	// 创建TCP服务器
	server := transport.NewTCPServer(config)

	// 添加日志中间件
	server.Use(middleware.Logging(log))

	// 添加自定义中间件：消息统计
	server.Use(func(next core.MessageHandler) core.MessageHandler {
		return func(ctx context.Context, conn core.Connection, msg *core.Message) error {
			// 记录消息开始处理时间
			start := time.Now()

			// 调用下一个处理器
			err := next(ctx, conn, msg)

			// 计算处理时间
			duration := time.Since(start)

			// 记录统计信息
			metrics.IncrReceivedMessages()
			if err != nil {
				metrics.IncrMessageErrors()
				log.Error("消息处理失败",
					logger.String("device_id", conn.DeviceID()),
					logger.Int("msg_id", int(msg.Header.MsgID)),
					logger.Duration("duration", duration),
					logger.Error("error", err),
				)
			} else {
				log.Info("消息处理成功",
					logger.String("device_id", conn.DeviceID()),
					logger.Int("msg_id", int(msg.Header.MsgID)),
					logger.Duration("duration", duration),
				)
			}

			return err
		}
	})

	// 注册终端注册消息处理器
	server.RegisterHandler(types.MsgIDTerminalRegister, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		// 解析终端注册信息
		reg, err := protocol.ParseTerminalRegister(msg.Body)
		if err != nil {
			return fmt.Errorf("解析终端注册信息失败: %w", err)
		}

		// 设置设备ID
		if tcpConn, ok := conn.(*transport.TCPConnection); ok {
			tcpConn.SetDeviceID(reg.TerminalID)
		}

		// 保存设备信息
		ctx = context.WithValue(ctx, "deviceID", conn.DeviceID())
		if err := customStorage.SaveDevice(ctx, conn.DeviceID(), reg); err != nil {
			return fmt.Errorf("保存设备信息失败: %w", err)
		}

		log.Info("终端注册成功",
			logger.String("terminal_id", reg.TerminalID),
			logger.String("plate_no", reg.PlateNo),
			logger.String("manufacturer_id", reg.ManufacturerID),
		)

		return nil
	})

	// 注册位置信息上报处理器
	server.RegisterHandler(types.MsgIDLocationReport, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		// 解析位置信息
		report, err := protocol.ParseLocationReport(msg.Body)
		if err != nil {
			return fmt.Errorf("解析位置信息失败: %w", err)
		}

		// 保存位置信息
		ctx = context.WithValue(ctx, "deviceID", conn.DeviceID())
		if err := customStorage.SaveLocation(ctx, report); err != nil {
			return fmt.Errorf("保存位置信息失败: %w", err)
		}

		// 检查是否超速（示例：超过120km/h）
		if report.Speed > 120 {
			log.Warn("检测到超速",
				logger.String("device_id", conn.DeviceID()),
				logger.Int("speed", int(report.Speed)),
				logger.Float64("latitude", report.Latitude),
				logger.Float64("longitude", report.Longitude),
			)
		}

		log.Info("收到位置上报",
			logger.String("device_id", conn.DeviceID()),
			logger.Float64("latitude", report.Latitude),
			logger.Float64("longitude", report.Longitude),
			logger.Int("speed", int(report.Speed)),
		)

		return nil
	})

	// 注册连接建立钩子
	server.OnConnect(func(conn core.Connection) error {
		metrics.IncrActiveConnections()
		log.Info("新连接建立",
			logger.String("remote_addr", conn.RemoteAddr().String()),
			logger.Int64("active_connections", metrics.GetActiveConnections()),
		)
		return nil
	})

	// 注册连接断开钩子
	server.OnDisconnect(func(conn core.Connection) error {
		metrics.DecrActiveConnections()
		log.Info("连接断开",
			logger.String("device_id", conn.DeviceID()),
			logger.Int64("active_connections", metrics.GetActiveConnections()),
		)
		return nil
	})

	// 注册错误处理钩子
	server.OnError(func(conn core.Connection, err error) {
		metrics.IncrMessageErrors()
		log.Error("连接错误",
			logger.String("device_id", conn.DeviceID()),
			logger.Error("error", err),
		)
	})

	// 启动服务器
	if err := server.Start(); err != nil {
		log.Fatal("启动服务器失败", logger.Error("error", err))
	}
	log.Info("服务器已启动", logger.String("addr", config.ListenAddr))

	// 定期打印统计信息
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				stats := server.GetStats()
				storageStats := customStorage.GetStats()
				log.Info("服务器统计",
					logger.Int64("active_connections", stats.ActiveConnections),
					logger.Int64("total_connections", stats.TotalConnections),
					logger.Int64("received_messages", stats.ReceivedMessages),
					logger.Int64("error_count", stats.ErrorCount),
					logger.Int("device_count", storageStats.DeviceCount),
					logger.Int("location_count", storageStats.LocationCount),
				)
			}
		}
	}()

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Info("正在关闭服务器...")
	if err := server.Stop(); err != nil {
		log.Error("关闭服务器失败", logger.Error("error", err))
	}
	log.Info("服务器已关闭")
}
