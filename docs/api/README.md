# JT/T 808 Go SDK API 文档

本文档详细介绍 JT/T 808 Go SDK 的核心 API 接口和使用方法。

## 目录

- [核心接口](#核心接口)
  - [Server 接口](#server-接口)
  - [Connection 接口](#connection-接口)
  - [Message 结构](#message-结构)
- [协议编解码](#协议编解码)
  - [Codec 编解码器](#codec-编解码器)
  - [消息解析函数](#消息解析函数)
- [传输层](#传输层)
  - [TCPServer](#tcpserver)
  - [Config 配置](#config-配置)
- [存储接口](#存储接口)
  - [Storage 接口](#storage-接口)
  - [MemoryStorage 内存存储](#memorystorage-内存存储)
- [消息队列](#消息队列)
  - [Publisher 接口](#publisher-接口)
  - [Subscriber 接口](#subscriber-接口)
  - [Kafka 实现](#kafka-实现)
- [中间件](#中间件)
- [日志](#日志)
- [常量定义](#常量定义)

## 核心接口

### Server 接口

服务器接口定义了 JT/T 808 服务器的核心功能。

```go
type Server interface {
    // Start 启动服务器
    Start() error
    // Stop 停止服务器
    Stop() error
    // RegisterHandler 注册消息处理器
    RegisterHandler(msgID uint16, handler MessageHandler)
    // GetConnection 获取连接
    GetConnection(deviceID string) (Connection, error)
    // GetStats 获取统计信息
    GetStats() ServerStats
    // Use 添加中间件
    Use(middleware Middleware)
    // OnConnect 注册连接建立钩子
    OnConnect(hook func(conn Connection) error)
    // OnDisconnect 注册连接断开钩子
    OnDisconnect(hook func(conn Connection) error)
    // OnError 注册错误处理钩子
    OnError(hook func(conn Connection, err error))
}
```

#### 使用示例

```go
server := transport.NewTCPServer(nil)

// 注册消息处理器
server.RegisterHandler(types.MsgIDLocationReport, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
    report, err := protocol.ParseLocationReport(msg.Body)
    if err != nil {
        return err
    }
    fmt.Printf("收到位置上报: 纬度=%f, 经度=%f\n", report.Latitude, report.Longitude)
    return nil
})

// 注册钩子函数
server.OnConnect(func(conn core.Connection) error {
    fmt.Printf("设备 %s 已连接\n", conn.DeviceID())
    return nil
})

// 启动服务器
if err := server.Start(); err != nil {
    log.Fatal(err)
}
```

### Connection 接口

连接接口表示一个终端设备连接。

```go
type Connection interface {
    // Send 发送消息
    Send(msg *Message) error
    // Close 关闭连接
    Close() error
    // DeviceID 获取设备ID
    DeviceID() string
    // IsConnected 是否已连接
    IsConnected() bool
    // RemoteAddr 获取远程地址
    RemoteAddr() net.Addr
    // Set 设置连接属性
    Set(key string, value interface{})
    // Get 获取连接属性
    Get(key string) (interface{}, bool)
    // LastActiveTime 获取最后活跃时间
    LastActiveTime() time.Time
    // Context 获取连接上下文
    Context() context.Context
}
```

#### 使用示例

```go
// 在消息处理器中使用连接
server.RegisterHandler(types.MsgIDTerminalHeartbeat, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
    // 设置连接属性
    conn.Set("lastHeartbeat", time.Now())
    
    // 发送响应消息
    response := &core.Message{
        Header: &core.MessageHeader{
            MsgID:     types.MsgIDPlatformCommonResponse,
            PhoneNo:   msg.Header.PhoneNo,
            MsgFlowNo: msg.Header.MsgFlowNo,
        },
        Body: []byte{0x00}, // 成功
    }
    return conn.Send(response)
})
```

### Message 结构

消息结构定义了 JT/T 808 协议消息的格式。

```go
// Message 消息结构
type Message struct {
    // Header 消息头
    Header *MessageHeader
    // Body 消息体
    Body []byte
    // Raw 原始数据
    Raw []byte
}

// MessageHeader 消息头结构
type MessageHeader struct {
    // MsgID 消息ID
    MsgID uint16
    // MsgBodyProperty 消息体属性
    MsgBodyProperty uint16
    // ProtocolVersion 协议版本
    ProtocolVersion uint8
    // PhoneNo 终端手机号
    PhoneNo string
    // MsgFlowNo 消息流水号
    MsgFlowNo uint16
    // MsgBodyLength 消息体长度
    MsgBodyLength uint16
    // SubPackage 是否分包
    SubPackage bool
    // Encryption 加密方式
    Encryption uint8
}
```

## 协议编解码

### Codec 编解码器

Codec 提供 JT/T 808 协议消息的编码和解码功能。

```go
// 创建编解码器
codec := protocol.NewCodec()

// 编码消息
msg := &protocol.Message{
    Header: &protocol.MessageHeader{
        MsgID:   types.MsgIDPlatformCommonResponse,
        PhoneNo: "13800138000",
    },
    Body: []byte{0x00, 0x00, 0x00, 0x01, 0x00},
}
data, err := codec.Encode(msg)
if err != nil {
    log.Fatal(err)
}

// 解码消息
decoded, err := codec.Decode(data)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("消息ID: 0x%04X\n", decoded.Header.MsgID)
```

### 消息解析函数

SDK 提供了常用消息的解析函数。

```go
// ParseLocationReport 解析位置信息上报
func ParseLocationReport(body []byte) (*core.LocationReport, error)

// ParseTerminalRegister 解析终端注册
func ParseTerminalRegister(body []byte) (*core.TerminalRegister, error)
```

#### 使用示例

```go
server.RegisterHandler(types.MsgIDLocationReport, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
    report, err := protocol.ParseLocationReport(msg.Body)
    if err != nil {
        return err
    }
    
    fmt.Printf("报警标志: 0x%08X\n", report.AlarmFlag)
    fmt.Printf("状态: 0x%08X\n", report.Status)
    fmt.Printf("纬度: %f\n", report.Latitude)
    fmt.Printf("经度: %f\n", report.Longitude)
    fmt.Printf("海拔: %d 米\n", report.Altitude)
    fmt.Printf("速度: %d km/h\n", report.Speed)
    fmt.Printf("方向: %d 度\n", report.Direction)
    fmt.Printf("时间: %s\n", report.Time.Format("2006-01-02 15:04:05"))
    
    return nil
})
```

## 传输层

### TCPServer

TCPServer 是 JT/T 808 协议的 TCP 服务器实现。

```go
// NewTCPServer 创建TCP服务器
func NewTCPServer(config *Config) *TCPServer
```

#### 完整示例

```go
package main

import (
    "context"
    "log"
    
    "github.com/darkinno-tech/jtt-808-go-sdk/core"
    "github.com/darkinno-tech/jtt-808-go-sdk/protocol"
    "github.com/darkinno-tech/jtt-808-go-sdk/protocol/types"
    "github.com/darkinno-tech/jtt-808-go-sdk/storage"
    "github.com/darkinno-tech/jtt-808-go-sdk/transport"
)

func main() {
    // 创建配置
    config := transport.DefaultConfig()
    config.ListenAddr = ":8080"
    config.MaxConnections = 100000
    
    // 创建服务器
    server := transport.NewTCPServer(config)
    
    // 创建存储
    memStorage := storage.NewMemoryStorage()
    
    // 注册处理器
    server.RegisterHandler(types.MsgIDLocationReport, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
        report, err := protocol.ParseLocationReport(msg.Body)
        if err != nil {
            return err
        }
        return memStorage.SaveLocation(ctx, report)
    })
    
    // 注册钩子
    server.OnConnect(func(conn core.Connection) error {
        log.Printf("设备连接: %s", conn.RemoteAddr())
        return nil
    })
    
    // 启动服务器
    if err := server.Start(); err != nil {
        log.Fatal(err)
    }
    defer server.Stop()
    
    // 保持运行
    select {}
}
```

### Config 配置

```go
type Config struct {
    // ListenAddr 监听地址
    ListenAddr string
    // MaxConnections 最大连接数
    MaxConnections int
    // ReadTimeout 读超时
    ReadTimeout time.Duration
    // WriteTimeout 写超时
    WriteTimeout time.Duration
    // IdleTimeout 空闲超时
    IdleTimeout time.Duration
    // ReadBufferSize 读缓冲区大小
    ReadBufferSize int
    // WriteBufferSize 写缓冲区大小
    WriteBufferSize int
    // MaxPacketSize 最大包长（字节）
    MaxPacketSize int
}
```

#### 默认配置

```go
config := transport.DefaultConfig()
// 输出:
// ListenAddr: ":8080"
// MaxConnections: 1000000
// ReadTimeout: 30s
// WriteTimeout: 30s
// IdleTimeout: 300s
// ReadBufferSize: 4096
// WriteBufferSize: 4096
// MaxPacketSize: 4096
```

## 存储接口

### Storage 接口

Storage 接口定义了数据存储的抽象层。

```go
type Storage interface {
    // SaveLocation 保存位置信息
    SaveLocation(ctx context.Context, loc *LocationReport) error
    // GetLocations 获取位置信息
    GetLocations(ctx context.Context, deviceID string, start, end time.Time) ([]*LocationReport, error)
    // SaveAlarm 保存报警信息
    SaveAlarm(ctx context.Context, alarm *AlarmReport) error
    // GetAlarms 获取报警信息
    GetAlarms(ctx context.Context, deviceID string, start, end time.Time) ([]*AlarmReport, error)
    // SaveDevice 保存设备信息
    SaveDevice(ctx context.Context, deviceID string, info *TerminalRegister) error
    // GetDevice 获取设备信息
    GetDevice(ctx context.Context, deviceID string) (*TerminalRegister, error)
    // UpdateDeviceStatus 更新设备状态
    UpdateDeviceStatus(ctx context.Context, deviceID string, status DeviceStatus) error
    // Close 关闭存储连接
    Close() error
    // Ping 检查连接健康
    Ping() error
}
```

### MemoryStorage 内存存储

MemoryStorage 是基于内存的存储实现，适用于开发和测试环境。

```go
// 创建内存存储
memStorage := storage.NewMemoryStorage()

// 保存位置信息
ctx := context.WithValue(context.Background(), "deviceID", "123456789")
loc := &core.LocationReport{
    AlarmFlag:  0,
    Status:     0,
    Latitude:   39.9042,
    Longitude:  116.4074,
    Altitude:   50,
    Speed:      60,
    Direction:  90,
    Time:       time.Now(),
}
err := memStorage.SaveLocation(ctx, loc)

// 获取位置信息
start := time.Now().Add(-1 * time.Hour)
end := time.Now()
locations, err := memStorage.GetLocations(ctx, "123456789", start, end)

// 获取统计信息
stats := memStorage.GetStats()
fmt.Printf("设备数: %d, 位置数: %d, 报警数: %d\n", 
    stats.DeviceCount, stats.LocationCount, stats.AlarmCount)
```

## 消息队列

### Publisher 接口

```go
type Publisher interface {
    // Publish 发布消息
    Publish(ctx context.Context, topic string, message []byte) error
    // PublishAsync 异步发布消息
    PublishAsync(ctx context.Context, topic string, message []byte) error
    // Close 关闭发布者
    Close() error
}
```

### Subscriber 接口

```go
type Subscriber interface {
    // Subscribe 订阅消息
    Subscribe(ctx context.Context, topic string, handler func([]byte)) error
    // Unsubscribe 取消订阅
    Unsubscribe(ctx context.Context, topic string) error
    // Close 关闭订阅者
    Close() error
}
```

### Kafka 实现

```go
import "github.com/darkinno-tech/jtt-808-go-sdk/publisher/kafka"

// 创建Kafka发布者
kafkaPublisher := kafka.NewPublisher(&kafka.Config{
    Brokers: []string{"localhost:9092"},
    Topic:   "jt808-messages",
})

// 发布消息
err := kafkaPublisher.Publish(ctx, "location", jsonData)

// 创建Kafka订阅者
kafkaSubscriber := kafka.NewSubscriber(&kafka.Config{
    Brokers: []string{"localhost:9092"},
    Topic:   "jt808-messages",
    GroupID: "jt808-consumer",
}, func(msg []byte) {
    fmt.Printf("收到消息: %s\n", string(msg))
})

// 订阅消息
err := kafkaSubscriber.Subscribe(ctx, "", nil)
```

## 中间件

SDK 提供了内置的中间件支持。

```go
// 添加日志中间件
server.Use(middleware.Logging(logger))

// 添加恢复中间件
server.Use(middleware.Recovery(logger))

// 添加限流中间件
server.Use(middleware.RateLimit(1000)) // 每秒1000个请求

// 添加超时中间件
server.Use(middleware.Timeout(5 * time.Second))

// 添加认证中间件
server.Use(middleware.Auth(func(deviceID string) bool {
    return isValidDevice(deviceID)
}))
```

## 日志

```go
import "github.com/darkinno-tech/jtt-808-go-sdk/logger"

// 创建日志记录器
log := logger.NewLogger(logger.InfoLevel)

// 添加字段
log = log.With(
    logger.String("service", "jt808-server"),
    logger.Int("version", 1),
)

// 记录日志
log.Info("服务器启动",
    logger.String("addr", ":8080"),
    logger.Int("max_connections", 100000),
)

log.Error("处理消息失败",
    logger.String("device_id", "123456789"),
    logger.Error("error", err),
)
```

## 常量定义

### 消息ID

```go
// 上行消息ID（终端到平台）
const (
    MsgIDTerminalCommonResponse   uint16 = 0x0001 // 终端通用应答
    MsgIDTerminalHeartbeat        uint16 = 0x0002 // 终端心跳
    MsgIDTerminalRegister         uint16 = 0x0100 // 终端注册
    MsgIDTerminalAuth             uint16 = 0x0102 // 终端鉴权
    MsgIDLocationReport           uint16 = 0x0200 // 位置信息汇报
)

// 下行消息ID（平台到终端）
const (
    MsgIDPlatformCommonResponse   uint16 = 0x8001 // 平台通用应答
    MsgIDTerminalRegisterResponse uint16 = 0x8100 // 终端注册应答
)
```

### 报警标志位

```go
const (
    AlarmFlagSOS           uint32 = 0x00000001 // 紧急报警
    AlarmFlagOverSpeed     uint32 = 0x00000002 // 超速报警
    AlarmFlagFatigue       uint32 = 0x00000004 // 疲劳驾驶
    AlarmFlagGNSSFault     uint32 = 0x00000010 // GNSS模块故障
    AlarmFlagPowerLow      uint32 = 0x00000080 // 终端主电源欠压
)
```

### 状态位

```go
const (
    StatusACC            uint32 = 0x00000001 // ACC状态
    StatusPositioning    uint32 = 0x00000002 // 定位状态
    StatusSouthLatitude  uint32 = 0x00000004 // 南纬
    StatusWestLongitude  uint32 = 0x00000008 // 西经
)
```

## 数据结构

### LocationReport 位置信息

```go
type LocationReport struct {
    AlarmFlag  uint32    // 报警标志
    Status     uint32    // 状态
    Latitude   float64   // 纬度
    Longitude  float64   // 经度
    Altitude   uint16    // 海拔高度（米）
    Speed      uint16    // 速度（km/h）
    Direction  uint16    // 方向（0-359度）
    Time       time.Time // 时间
}
```

### TerminalRegister 终端注册

```go
type TerminalRegister struct {
    ProvinceID     uint16 // 省域ID
    CityID         uint16 // 市县域ID
    ManufacturerID string // 制造商ID
    TerminalType   string // 终端型号
    TerminalID     string // 终端ID
    PlateColor     uint8  // 车牌颜色
    PlateNo        string // 车牌号
}
```

### AlarmReport 报警信息

```go
type AlarmReport struct {
    AlarmFlag  uint32    // 报警标志
    Status     uint32    // 状态
    Latitude   float64   // 纬度
    Longitude  float64   // 经度
    Altitude   uint16    // 海拔高度
    Speed      uint16    // 速度
    Direction  uint16    // 方向
    Time       time.Time // 时间
    AlarmType  uint8     // 报警类型
}
```

## ServerStats 服务器统计

```go
type ServerStats struct {
    ActiveConnections int64         // 当前活跃连接数
    TotalConnections  int64         // 总连接数
    ReceivedMessages  int64         // 接收消息数
    SentMessages      int64         // 发送消息数
    ErrorCount        int64         // 错误数
    StartTime         time.Time     // 启动时间
    Uptime            time.Duration // 运行时间
}
```

## 更多资源

- [快速开始指南](../getting-started.md)
- [架构设计文档](../architecture.md)
- [部署指南](../deployment.md)
- [技术设计文档](../technical-design.md)
