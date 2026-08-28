package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/darkinno-tech/jtt-808-go-sdk/core"
	"github.com/darkinno-tech/jtt-808-go-sdk/protocol"
	"github.com/darkinno-tech/jtt-808-go-sdk/protocol/types"
	"github.com/darkinno-tech/jtt-808-go-sdk/storage"
	"github.com/darkinno-tech/jtt-808-go-sdk/transport"
)

func main() {
	// 创建内存存储
	memStorage := storage.NewMemoryStorage()

	// 创建服务器配置
	config := transport.DefaultConfig()
	config.ListenAddr = ":8080"

	// 创建TCP服务器
	server := transport.NewTCPServer(config)

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

	// 注册位置信息上报处理器
	server.RegisterHandler(types.MsgIDLocationReport, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		// 解析位置信息
		report, err := protocol.ParseLocationReport(msg.Body)
		if err != nil {
			return fmt.Errorf("解析位置信息失败: %w", err)
		}

		// 保存位置信息
		ctx = context.WithValue(ctx, "deviceID", conn.DeviceID())
		if err := memStorage.SaveLocation(ctx, report); err != nil {
			return fmt.Errorf("保存位置信息失败: %w", err)
		}

		log.Printf("收到位置上报: 设备ID=%s, 纬度=%.6f, 经度=%.6f",
			conn.DeviceID(), report.Latitude, report.Longitude)
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

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("正在关闭服务器...")
	if err := server.Stop(); err != nil {
		log.Printf("关闭服务器失败: %v", err)
	}
	log.Println("服务器已关闭")
}
