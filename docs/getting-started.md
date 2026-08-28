# JT/T 808 Go SDK 快速开始指南

本指南帮助您快速上手 JT/T 808 Go SDK，从安装到运行第一个示例程序。

## 目录

- [环境要求](#环境要求)
- [安装](#安装)
- [快速入门](#快速入门)
- [核心概念](#核心概念)
- [完整示例](#完整示例)
- [进阶用法](#进阶用法)
- [常见问题](#常见问题)

## 环境要求

- Go 1.18 或更高版本
- 操作系统：Linux、macOS、Windows

## 安装

### 使用 Go Modules

```bash
go get github.com/im10furry/jtt-808-go-sdk
```

### 在项目中引入

```go
import (
    "github.com/im10furry/jtt-808-go-sdk/core"
    "github.com/im10furry/jtt-808-go-sdk/protocol"
    "github.com/im10furry/jtt-808-go-sdk/protocol/types"
    "github.com/im10furry/jtt-808-go-sdk/storage"
    "github.com/im10furry/jtt-808-go-sdk/transport"
)
```

## 快速入门

### 第一个 JT/T 808 服务器

创建一个简单的 JT/T 808 服务器，处理终端位置上报：

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    "github.com/im10furry/jtt-808-go-sdk/core"
    "github.com/im10furry/jtt-808-go-sdk/protocol"
    "github.com/im10furry/jtt-808-go-sdk/protocol/types"
    "github.com/im10furry/jtt-808-go-sdk/storage"
    "github.com/im10furry/jtt-808-go-sdk/transport"
)

func main() {
    // 1. 创建服务器配置
    config := transport.DefaultConfig()
    config.ListenAddr = ":8080"
    
    // 2. 创建服务器实例
    server := transport.NewTCPServer(config)
    
    // 3. 创建存储（使用内存存储，生产环境请使用数据库）
    memStorage := storage.NewMemoryStorage()
    
    // 4. 注册位置上报处理器
    server.RegisterHandler(types.MsgIDLocationReport, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
        // 解析位置信息
        report, err := protocol.ParseLocationReport(msg.Body)
        if err != nil {
            return fmt.Errorf("解析位置信息失败: %w", err)
        }
        
        // 保存位置信息
        if err := memStorage.SaveLocation(ctx, report); err != nil {
            return fmt.Errorf("保存位置信息失败: %w", err)
        }
        
        log.Printf("收到位置上报: 设备=%s, 纬度=%.6f, 经度=%.6f\n", 
            conn.DeviceID(), report.Latitude, report.Longitude)
        
        return nil
    })
    
    // 5. 注册连接建立钩子
    server.OnConnect(func(conn core.Connection) error {
        log.Printf("新设备连接: %s\n", conn.RemoteAddr())
        return nil
    })
    
    // 6. 启动服务器
    if err := server.Start(); err != nil {
        log.Fatal("启动服务器失败:", err)
    }
    defer server.Stop()
    
    log.Println("JT/T 808 服务器已启动，监听端口 :8080")
    
    // 保持服务器运行
    select {}
}
```

### 运行服务器

```bash
go run main.go
```

## 核心概念

### 1. 消息处理器 (MessageHandler)

消息处理器是处理终端上报消息的核心函数：

```go
type MessageHandler func(ctx context.Context, conn Connection, msg *Message) error
```

- `ctx`: 上下文，可用于传递设备ID等信息
- `conn`: 连接对象，用于发送响应
- `msg`: 消息对象，包含消息头和消息体

### 2. 消息ID (Message ID)

消息ID标识消息类型，定义在 `protocol/types/constants.go`：

```go
// 上行消息（终端到平台）
types.MsgIDTerminalHeartbeat  // 0x0002 - 终端心跳
types.MsgIDTerminalRegister   // 0x0100 - 终端注册
types.MsgIDTerminalAuth       // 0x0102 - 终端鉴权
types.MsgIDLocationReport     // 0x0200 - 位置信息汇报

// 下行消息（平台到终端）
types.MsgIDPlatformCommonResponse   // 0x8001 - 平台通用应答
types.MsgIDTerminalRegisterResponse // 0x8100 - 终端注册应答
```

### 3. 连接管理

每个终端连接都通过 `Connection` 接口管理：

```go
// 获取设备ID
deviceID := conn.DeviceID()

// 发送消息
conn.Send(responseMessage)

// 获取连接属性
value, exists := conn.Get("key")

// 设置连接属性
conn.Set("key", value)
```

### 4. 中间件

中间件用于在消息处理前后执行通用逻辑：

```go
// 添加日志中间件
server.Use(middleware.Logging(logger))

// 添加恢复中间件
server.Use(middleware.Recovery(logger))
```

## 完整示例

### 处理多种消息类型

```go
package main

import (
    "context"
    "log"
    
    "github.com/im10furry/jtt-808-go-sdk/core"
    "github.com/im10furry/jtt-808-go-sdk/protocol"
    "github.com/im10furry/jtt-808-go-sdk/protocol/types"
    "github.com/im10furry/jtt-808-go-sdk/transport"
)

func main() {
    config := transport.DefaultConfig()
    server := transport.NewTCPServer(config)
    
    // 处理终端心跳
    server.RegisterHandler(types.MsgIDTerminalHeartbeat, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
        log.Printf("收到心跳: 设备=%s\n", conn.DeviceID())
        
        // 发送平台通用应答
        return sendCommonResponse(conn, msg, types.CommonResponseSuccess)
    })
    
    // 处理终端注册
    server.RegisterHandler(types.MsgIDTerminalRegister, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
        reg, err := protocol.ParseTerminalRegister(msg.Body)
        if err != nil {
            return err
        }
        
        log.Printf("收到注册请求: 车牌=%s, 终端型号=%s\n", reg.PlateNo, reg.TerminalType)
        
        // 保存设备信息
        // storage.SaveDevice(ctx, conn.DeviceID(), reg)
        
        // 发送注册应答
        return sendRegisterResponse(conn, msg, types.RegisterResultSuccess, "AUTH_CODE_123")
    })
    
    // 处理终端鉴权
    server.RegisterHandler(types.MsgIDTerminalAuth, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
        log.Printf("收到鉴权请求: 设备=%s\n", conn.DeviceID())
        
        // 验证鉴权码
        // authCode := string(msg.Body)
        
        return sendCommonResponse(conn, msg, types.CommonResponseSuccess)
    })
    
    // 处理位置上报
    server.RegisterHandler(types.MsgIDLocationReport, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
        report, err := protocol.ParseLocationReport(msg.Body)
        if err != nil {
            return err
        }
        
        // 检查报警标志
        if report.AlarmFlag&types.AlarmFlagSOS != 0 {
            log.Printf("紧急报警: 设备=%s\n", conn.DeviceID())
        }
        
        if report.AlarmFlag&types.AlarmFlagOverSpeed != 0 {
            log.Printf("超速报警: 设备=%s, 速度=%d\n", conn.DeviceID(), report.Speed)
        }
        
        // 保存位置信息
        // storage.SaveLocation(ctx, report)
        
        return sendCommonResponse(conn, msg, types.CommonResponseSuccess)
    })
    
    if err := server.Start(); err != nil {
        log.Fatal(err)
    }
    defer server.Stop()
    
    select {}
}

// 发送平台通用应答
func sendCommonResponse(conn core.Connection, msg *core.Message, result uint8) error {
    response := &core.Message{
        Header: &core.MessageHeader{
            MsgID:     types.MsgIDPlatformCommonResponse,
            PhoneNo:   msg.Header.PhoneNo,
            MsgFlowNo: msg.Header.MsgFlowNo,
        },
        Body: []byte{
            byte(msg.Header.MsgID >> 8),   // 应答消息ID高字节
            byte(msg.Header.MsgID & 0xFF), // 应答消息ID低字节
            byte(msg.Header.MsgFlowNo >> 8),
            byte(msg.Header.MsgFlowNo & 0xFF),
            result,
        },
    }
    return conn.Send(response)
}

// 发送注册应答
func sendRegisterResponse(conn core.Connection, msg *core.Message, result uint8, authCode string) error {
    body := make([]byte, 3+len(authCode))
    body[0] = byte(msg.Header.MsgFlowNo >> 8)
    body[1] = byte(msg.Header.MsgFlowNo & 0xFF)
    body[2] = result
    copy(body[3:], authCode)
    
    response := &core.Message{
        Header: &core.MessageHeader{
            MsgID:     types.MsgIDTerminalRegisterResponse,
            PhoneNo:   msg.Header.PhoneNo,
            MsgFlowNo: msg.Header.MsgFlowNo,
        },
        Body: body,
    }
    return conn.Send(response)
}
```

## 进阶用法

### 使用中间件

```go
import "github.com/im10furry/jtt-808-go-sdk/middleware"

func main() {
    server := transport.NewTCPServer(nil)
    
    // 创建日志记录器
    logger := logger.NewLogger(logger.InfoLevel)
    
    // 添加中间件
    server.Use(middleware.Recovery(logger))  // 恢复中间件
    server.Use(middleware.Logging(logger))   // 日志中间件
    server.Use(middleware.RateLimit(1000))   // 限流中间件
    
    // ... 注册处理器
}
```

### 使用消息队列

```go
import "github.com/im10furry/jtt-808-go-sdk/publisher/kafka"

func main() {
    // 创建Kafka发布者
    kafkaPub := kafka.NewPublisher(&kafka.Config{
        Brokers: []string{"localhost:9092"},
        Topic:   "jt808-locations",
    })
    defer kafkaPub.Close()
    
    server := transport.NewTCPServer(nil)
    
    // 注册处理器，将位置信息发送到Kafka
    server.RegisterHandler(types.MsgIDLocationReport, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
        report, err := protocol.ParseLocationReport(msg.Body)
        if err != nil {
            return err
        }
        
        // 序列化为JSON
        jsonData, _ := json.Marshal(report)
        
        // 发送到Kafka
        return kafkaPub.Publish(ctx, "locations", jsonData)
    })
}
```

### 使用数据库存储

```go
import "github.com/im10furry/jtt-808-go-sdk/storage/mysql"

func main() {
    // 创建MySQL存储
    mysqlStorage, err := mysql.NewStorage(&mysql.Config{
        Host:     "localhost",
        Port:     3306,
        User:     "root",
        Password: "password",
        Database: "jt808",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer mysqlStorage.Close()
    
    server := transport.NewTCPServer(nil)
    
    // 注册处理器，保存到MySQL
    server.RegisterHandler(types.MsgIDLocationReport, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
        report, err := protocol.ParseLocationReport(msg.Body)
        if err != nil {
            return err
        }
        return mysqlStorage.SaveLocation(ctx, report)
    })
}
```

## 常见问题

### 1. 如何测试服务器？

使用 SDK 提供的测试客户端：

```bash
# 运行测试客户端
go run test/client/main.go -s 127.0.0.1:8080 -t location

# 运行压力测试
go run test/stress/main.go -s 127.0.0.1:8080 -c 100 -d 60 -t location
```

### 2. 如何支持 JT/T 808-2019 版本？

SDK 自动支持 2019 版本，只需在消息头中设置协议版本：

```go
msg.Header.ProtocolVersion = types.Version2019
```

### 3. 如何处理分包消息？

SDK 自动处理分包消息，设置分包标志即可：

```go
msg.Header.SubPackage = true
```

### 4. 如何自定义日志？

使用 logger 包创建自定义日志记录器：

```go
import "github.com/im10furry/jtt-808-go-sdk/logger"

// 创建日志记录器
log := logger.NewLogger(logger.DebugLevel)

// 添加字段
log = log.With(logger.String("module", "jt808"))
```

### 5. 如何监控服务器状态？

使用 `GetStats()` 方法获取服务器统计信息：

```go
stats := server.GetStats()
fmt.Printf("活跃连接数: %d\n", stats.ActiveConnections)
fmt.Printf("总连接数: %d\n", stats.TotalConnections)
fmt.Printf("接收消息数: %d\n", stats.ReceivedMessages)
fmt.Printf("运行时间: %s\n", stats.Uptime)
```

## 下一步

- 阅读 [API 文档](api/README.md) 了解完整的 API 参考
- 阅读 [架构设计文档](architecture.md) 了解系统架构
- 阅读 [部署指南](deployment.md) 了解生产环境部署
