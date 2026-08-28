package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/darkinno-tech/jtt-808-go-sdk/core"
	"github.com/darkinno-tech/jtt-808-go-sdk/protocol"
	"github.com/darkinno-tech/jtt-808-go-sdk/protocol/types"
	"github.com/darkinno-tech/jtt-808-go-sdk/storage"
	"github.com/darkinno-tech/jtt-808-go-sdk/transport"
)

// LocationPlugin 位置信息插件
type LocationPlugin struct {
	server core.Server
	name   string
}

// NewLocationPlugin 创建位置信息插件
func NewLocationPlugin() *LocationPlugin {
	return &LocationPlugin{
		name: "LocationPlugin",
	}
}

// Name 插件名称
func (p *LocationPlugin) Name() string {
	return p.name
}

// Version 插件版本
func (p *LocationPlugin) Version() string {
	return "1.0.0"
}

// Initialize 初始化插件
func (p *LocationPlugin) Initialize(server core.Server) error {
	p.server = server

	// 注册位置信息上报处理器
	server.RegisterHandler(types.MsgIDLocationReport, p.handleLocationReport)

	log.Printf("插件 %s v%s 已初始化", p.name, p.Version())
	return nil
}

// Shutdown 关闭插件
func (p *LocationPlugin) Shutdown() error {
	log.Printf("插件 %s 已关闭", p.name)
	return nil
}

// handleLocationReport 处理位置信息上报
func (p *LocationPlugin) handleLocationReport(ctx context.Context, conn core.Connection, msg *core.Message) error {
	// 解析位置信息
	report, err := protocol.ParseLocationReport(msg.Body)
	if err != nil {
		return fmt.Errorf("解析位置信息失败: %w", err)
	}

	// 这里可以添加业务逻辑，比如：
	// 1. 保存到数据库
	// 2. 发送到消息队列
	// 3. 实时推送
	// 4. 数据分析

	log.Printf("位置插件处理: 设备ID=%s, 纬度=%.6f, 经度=%.6f, 速度=%dkm/h",
		conn.DeviceID(), report.Latitude, report.Longitude, report.Speed)

	return nil
}

// AlarmPlugin 报警插件
type AlarmPlugin struct {
	server core.Server
	name   string
}

// NewAlarmPlugin 创建报警插件
func NewAlarmPlugin() *AlarmPlugin {
	return &AlarmPlugin{
		name: "AlarmPlugin",
	}
}

// Name 插件名称
func (p *AlarmPlugin) Name() string {
	return p.name
}

// Version 插件版本
func (p *AlarmPlugin) Version() string {
	return "1.0.0"
}

// Initialize 初始化插件
func (p *AlarmPlugin) Initialize(server core.Server) error {
	p.server = server

	// 添加报警检测中间件
	server.Use(p.alarmDetectionMiddleware())

	log.Printf("插件 %s v%s 已初始化", p.name, p.Version())
	return nil
}

// Shutdown 关闭插件
func (p *AlarmPlugin) Shutdown() error {
	log.Printf("插件 %s 已关闭", p.name)
	return nil
}

// alarmDetectionMiddleware 报警检测中间件
func (p *AlarmPlugin) alarmDetectionMiddleware() core.Middleware {
	return func(next core.MessageHandler) core.MessageHandler {
		return func(ctx context.Context, conn core.Connection, msg *core.Message) error {
			// 只处理位置信息上报
			if msg.Header.MsgID == types.MsgIDLocationReport {
				// 解析位置信息
				report, err := protocol.ParseLocationReport(msg.Body)
				if err == nil {
					// 检查报警条件
					p.checkAlarms(conn, report)
				}
			}

			// 调用下一个处理器
			return next(ctx, conn, msg)
		}
	}
}

// checkAlarms 检查报警条件
func (p *AlarmPlugin) checkAlarms(conn core.Connection, report *core.LocationReport) {
	// 检查超速报警
	if report.Speed > 120 {
		log.Printf("报警：设备 %s 超速，当前速度 %dkm/h", conn.DeviceID(), report.Speed)
		// 这里可以发送报警通知
	}

	// 检查疲劳驾驶（连续驾驶超过4小时）
	// 这里只是示例，实际需要更复杂的逻辑

	// 检查区域报警
	// 这里可以添加地理围栏检测
}

func main() {
	// 创建内存存储
	memStorage := storage.NewMemoryStorage()

	// 创建服务器配置
	config := transport.DefaultConfig()
	config.ListenAddr = ":8080"

	// 创建TCP服务器
	server := transport.NewTCPServer(config)

	// 创建插件
	locationPlugin := NewLocationPlugin()
	alarmPlugin := NewAlarmPlugin()

	// 初始化插件（手动调用，因为可能没有RegisterPlugin方法）
	if err := locationPlugin.Initialize(server); err != nil {
		log.Fatalf("初始化位置插件失败: %v", err)
	}

	if err := alarmPlugin.Initialize(server); err != nil {
		log.Fatalf("初始化报警插件失败: %v", err)
	}

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
		if err := memStorage.SaveDevice(ctx, conn.DeviceID(), reg); err != nil {
			return fmt.Errorf("保存设备信息失败: %w", err)
		}

		log.Printf("终端注册成功: 设备ID=%s, 车牌号=%s", reg.TerminalID, reg.PlateNo)
		return nil
	})

	// 注册连接建立钩子
	server.OnConnect(func(conn core.Connection) error {
		log.Printf("新连接建立: %s", conn.RemoteAddr().String())
		return nil
	})

	// 注册连接断开钩子
	server.OnDisconnect(func(conn core.Connection) error {
		log.Printf("连接断开: 设备ID=%s", conn.DeviceID())
		return nil
	})

	// 启动服务器
	if err := server.Start(); err != nil {
		log.Fatalf("启动服务器失败: %v", err)
	}
	log.Printf("服务器已启动，监听地址: %s", config.ListenAddr)
	log.Printf("已加载插件: %s v%s, %s v%s",
		locationPlugin.Name(), locationPlugin.Version(),
		alarmPlugin.Name(), alarmPlugin.Version())

	// 定期打印统计信息
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				stats := server.GetStats()
				log.Printf("服务器统计: 活跃连接=%d, 总连接=%d, 接收消息=%d, 错误=%d",
					stats.ActiveConnections, stats.TotalConnections,
					stats.ReceivedMessages, stats.ErrorCount)
			}
		}
	}()

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("正在关闭服务器...")

	// 关闭插件
	if err := locationPlugin.Shutdown(); err != nil {
		log.Printf("关闭位置插件失败: %v", err)
	}
	if err := alarmPlugin.Shutdown(); err != nil {
		log.Printf("关闭报警插件失败: %v", err)
	}

	// 关闭服务器
	if err := server.Stop(); err != nil {
		log.Printf("关闭服务器失败: %v", err)
	}
	log.Println("服务器已关闭")
}
