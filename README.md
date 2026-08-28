# JT/T 808 Go SDK

> 面向 JT/T 808 车载终端接入场景的 Go SDK，提供协议编解码、TCP 服务端、连接管理、存储接口、消息发布和测试工具。

## 功能特性

- **协议编解码**：支持 JT/T 808 消息帧的编码、解码、转义、反转义、BCD 号码处理和异或校验。
- **版本兼容**：内置 2011、2013、2019 版本常量，并按协议版本处理终端手机号 BCD 长度。
- **消息解析**：提供位置上报、终端注册等常用消息体解析能力。
- **TCP 服务端**：支持按消息 ID 注册处理器、连接钩子、中间件链、连接查询和运行统计。
- **高性能服务端**：提供 Worker Pool、分片连接池、并行 Accept、TCP_NODELAY、KeepAlive 等配置。
- **存储后端**：内置内存存储，并提供 MySQL、PostgreSQL、Redis 存储实现。
- **消息发布**：提供 Kafka、RabbitMQ、Redis Pub/Sub 发布/订阅封装。
- **工程组件**：包含日志、指标、中间件、基础示例、进阶示例、压测工具和集成测试。

## 快速开始

### 环境要求

- Go 1.25.6 或更高版本（以 `go.mod` 为准）
- 可选：MySQL、PostgreSQL、Redis、Kafka、RabbitMQ，按实际启用的存储或发布组件安装

### 安装

```bash
go get github.com/im10furry/jtt-808-go-sdk
```

### 创建 TCP 服务端

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
	memStorage := storage.NewMemoryStorage()

	config := transport.DefaultConfig()
	config.ListenAddr = ":8080"

	server := transport.NewTCPServer(config)

	server.RegisterHandler(types.MsgIDLocationReport, func(ctx context.Context, conn core.Connection, msg *core.Message) error {
		report, err := protocol.ParseLocationReport(msg.Body)
		if err != nil {
			return fmt.Errorf("parse location report: %w", err)
		}

		ctx = context.WithValue(ctx, "deviceID", conn.DeviceID())
		if err := memStorage.SaveLocation(ctx, report); err != nil {
			return fmt.Errorf("save location: %w", err)
		}

		log.Printf("location report: device=%s lat=%.6f lng=%.6f", conn.DeviceID(), report.Latitude, report.Longitude)
		return nil
	})

	server.OnConnect(func(conn core.Connection) error {
		log.Printf("connected: %s", conn.RemoteAddr())
		return nil
	})

	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
	defer server.Stop()

	log.Println("JT/T 808 server listening on :8080")
	select {}
}
```

## 命令示例

```bash
# 运行基础示例
go run ./examples/basic

# 运行示例服务端，监听 :8080
go run ./cmd/server

# 运行高性能服务端，监听 :8080
go run ./cmd/highperf

# 对本地服务端执行压测
go run ./test/stress -s 127.0.0.1:8080 -c 100 -d 60 -t location

# 运行全部测试
go test ./...
```

压测工具参数：

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `-s` | `127.0.0.1:8080` | 服务端地址 |
| `-c` | `1000` | 并发连接数 |
| `-d` | `30` | 测试时长，单位秒 |
| `-r` | `10` | 每个连接每秒发送的消息数 |
| `-t` | `location` | 测试场景：`location`、`register`、`heartbeat`、`mixed` |
| `-b` | `500` | 批次连接大小 |

### 压测实测结果

2026-06-09 在 Windows 本机回环地址 `127.0.0.1` 上，对 `cmd/highperf` 高性能服务端执行 `location` 场景压测：

```bash
go run ./test/stress -s 127.0.0.1:8080 -c 1000 -d 30 -r 10 -t location -b 50
```

实测结果：`1000/1000` 连接成功，`304236` 条消息全部成功，吞吐约 `9809.82 msg/s`，平均延迟约 `436.295us`，成功率 `100%`，服务端 `errorCount=0`，Worker 队列无堆积。

注意：Windows 本机回环压测时，`-b 500` 这类较大的突发建连批次可能受到客户端侧端口、连接队列或 TCP 栈调度影响，导致连接错误和并发表现偏低；这不代表 SDK 服务端并发能力上限。复测建议使用 `-b 50` 或更小批次逐步建连。

## 核心用法

### 协议编解码

```go
codec := protocol.NewCodec()

data, err := codec.Encode(&protocol.Message{
	Header: &protocol.MessageHeader{
		MsgID:     types.MsgIDTerminalHeartbeat,
		PhoneNo:   "13800138000",
		MsgFlowNo: 1,
	},
	Body: []byte{},
})
if err != nil {
	return err
}

msg, err := codec.Decode(data)
if err != nil {
	return err
}
_ = msg
```

### TCP 服务配置

`transport.DefaultConfig()` 默认值：

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `ListenAddr` | `:8080` | TCP 监听地址 |
| `MaxConnections` | `1000000` | 最大连接数 |
| `ReadTimeout` | `30s` | 读超时 |
| `WriteTimeout` | `30s` | 写超时 |
| `IdleTimeout` | `300s` | 空闲连接超时 |
| `ReadBufferSize` | `4096` | 读缓冲大小 |
| `WriteBufferSize` | `4096` | 写缓冲大小 |
| `MaxPacketSize` | `4096` | 最大包长度 |

高性能服务端使用 `transport.DefaultHighPerfConfig()`，可配置最小/最大 Worker 数、队列大小、连接池分片数、并行 Accept 数、TCP_NODELAY 和 KeepAlive。

### 中间件与钩子

```go
log := logger.NewLogger(logger.InfoLevel)
server.Use(middleware.Logging(log))
server.Use(middleware.Timeout(5 * time.Second))
server.Use(middleware.Recovery(log))

server.OnConnect(func(conn core.Connection) error {
	return nil
})

server.OnDisconnect(func(conn core.Connection) error {
	return nil
})

server.OnError(func(conn core.Connection, err error) {
	log.Error("connection error", logger.Error("error", err))
})
```

### 存储实现

| 包 | 构造函数 | 说明 |
|---|---|---|
| `storage` | `storage.NewMemoryStorage()` | 内存存储，适合示例、测试和临时缓存 |
| `storage/mysql` | `mysql.NewMySQLStorage(config)` | MySQL 存储 |
| `storage/postgres` | `postgres.NewPostgresStorage(config)` | PostgreSQL 存储 |
| `storage/redis` | `redis.NewRedisStorage(config)` | Redis 存储 |

### 消息发布

| 包 | 能力 |
|---|---|
| `publisher/kafka` | Kafka 发布与订阅 |
| `publisher/rabbitmq` | RabbitMQ 发布与订阅 |
| `publisher/redis` | Redis Pub/Sub 发布与订阅 |

## 消息类型

项目在 `protocol/types` 中定义了常用 JT/T 808 消息 ID。当前消息体解析能力以 `protocol` 包导出的解析函数为准。

| 消息 ID | 常量 | 方向 |
|---:|---|---|
| `0x0001` | `MsgIDTerminalCommonResponse` | 终端到平台 |
| `0x0002` | `MsgIDTerminalHeartbeat` | 终端到平台 |
| `0x0003` | `MsgIDTerminalDeregister` | 终端到平台 |
| `0x0100` | `MsgIDTerminalRegister` | 终端到平台 |
| `0x0102` | `MsgIDTerminalAuth` | 终端到平台 |
| `0x0200` | `MsgIDLocationReport` | 终端到平台 |
| `0x8001` | `MsgIDPlatformCommonResponse` | 平台到终端 |
| `0x8100` | `MsgIDTerminalRegisterResponse` | 平台到终端 |
| `0x8200` | `MsgIDLocationQuery` | 平台到终端 |
| `0x8500` | `MsgIDVehicleControl` | 平台到终端 |

## 项目结构

```text
jtt-808-go-sdk/
├── cmd/                 # 示例服务端入口
├── config/              # 配置管理
├── core/                # 核心接口、消息模型、连接池、Worker Pool
├── docs/                # 架构、API、部署和入门文档
├── examples/            # basic、advanced、custom 示例
├── logger/              # 日志组件
├── metrics/             # 指标统计
├── middleware/          # logging、auth、rate limit、recovery、timeout
├── protocol/            # JT/T 808 编解码和消息常量
├── publisher/           # Kafka、RabbitMQ、Redis 发布订阅
├── storage/             # Memory、MySQL、PostgreSQL、Redis 存储
├── test/                # 单元测试、集成测试、压测和测试客户端
└── transport/           # TCP 与高性能 TCP 服务端
```

## 文档

- [快速开始](docs/getting-started.md)
- [API 文档](docs/api/README.md)
- [架构说明](docs/architecture.md)
- [技术设计](docs/technical-design.md)
- [部署指南](docs/deployment.md)
- [压测报告](docs/stress-test-report.md)
- [基础示例](examples/basic/README.md)
- [进阶示例](examples/advanced/README.md)
- [自定义扩展示例](examples/custom/README.md)

## 开发

```bash
git clone https://github.com/im10furry/jtt-808-go-sdk.git
cd jtt-808-go-sdk

go mod download
go test ./...
```

构建示例服务端：

```bash
go build -o jt808-server ./cmd/server
go build -o jt808-highperf ./cmd/highperf
```

如果本地 Git ownership 导致 Go VCS stamping 报错，可临时关闭 VCS 信息写入：

```bash
go build -buildvcs=false -o jt808-server ./cmd/server
go build -buildvcs=false -o jt808-highperf ./cmd/highperf
```

## 许可证

[MIT License](LICENSE)
