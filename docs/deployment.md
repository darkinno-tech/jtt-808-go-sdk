# JT/T 808 Go SDK 部署指南

本文档介绍如何将 JT/T 808 Go SDK 部署到生产环境。

## 目录

- [部署方式](#部署方式)
  - [单机部署](#单机部署)
  - [Docker 部署](#docker-部署)
  - [集群部署](#集群部署)
- [环境配置](#环境配置)
- [性能调优](#性能调优)
- [监控告警](#监控告警)
- [日志管理](#日志管理)
- [安全配置](#安全配置)
- [运维建议](#运维建议)

## 部署方式

### 单机部署

#### 1. 编译项目

```bash
# 克隆项目
git clone https://github.com/im10furry/jtt-808-go-sdk.git
cd jt808-go-sdk

# 编译
go build -o jt808-server cmd/server/main.go
```

#### 2. 配置文件

创建配置文件 `config.yaml`：

```yaml
server:
  listen_addr: ":8080"
  max_connections: 100000
  read_timeout: 30s
  write_timeout: 30s
  idle_timeout: 300s
  read_buffer_size: 4096
  write_buffer_size: 4096
  max_packet_size: 4096

storage:
  type: "mysql"
  mysql:
    host: "localhost"
    port: 3306
    user: "jt808"
    password: "password"
    database: "jt808"
    max_open_conns: 100
    max_idle_conns: 10

log:
  level: "info"
  output: "stdout"
  file: "/var/log/jt808/server.log"

metrics:
  enabled: true
  addr: ":9090"
```

#### 3. 启动服务

```bash
# 使用配置文件启动
./jt808-server -config config.yaml

# 或使用环境变量
export JT808_LISTEN_ADDR=":8080"
export JT808_STORAGE_TYPE="mysql"
export JT808_MYSQL_HOST="localhost"
./jt808-server
```

#### 4. Systemd 服务

创建 `/etc/systemd/system/jt808-server.service`：

```ini
[Unit]
Description=JT/T 808 Server
After=network.target mysql.service

[Service]
Type=simple
User=jt808
Group=jt808
WorkingDirectory=/opt/jt808
ExecStart=/opt/jt808/jt808-server -config /etc/jt808/config.yaml
Restart=always
RestartSec=5
LimitNOFILE=1000000

[Install]
WantedBy=multi-user.target
```

启动服务：

```bash
sudo systemctl daemon-reload
sudo systemctl enable jt808-server
sudo systemctl start jt808-server
sudo systemctl status jt808-server
```

### Docker 部署

#### 1. 创建 Dockerfile

```dockerfile
# 构建阶段
FROM golang:1.21-alpine AS builder

WORKDIR /app

# 安装依赖
COPY go.mod go.sum ./
RUN go mod download

# 编译
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o jt808-server cmd/server/main.go

# 运行阶段
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# 复制二进制文件
COPY --from=builder /app/jt808-server .
COPY --from=builder /app/config.docker.yaml ./config.yaml

# 暴露端口
EXPOSE 8080 9090

# 启动命令
CMD ["./jt808-server", "-config", "config.yaml"]
```

#### 2. 创建 Docker Compose

创建 `docker-compose.yml`：

```yaml
version: '3.8'

services:
  jt808-server:
    build: .
    container_name: jt808-server
    ports:
      - "8080:8080"
      - "9090:9090"
    volumes:
      - ./config.yaml:/app/config.yaml
      - ./logs:/app/logs
    depends_on:
      - mysql
      - redis
    restart: unless-stopped

  mysql:
    image: mysql:8.0
    container_name: jt808-mysql
    environment:
      MYSQL_ROOT_PASSWORD: rootpassword
      MYSQL_DATABASE: jt808
      MYSQL_USER: jt808
      MYSQL_PASSWORD: password
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
      - ./scripts/init.sql:/docker-entrypoint-initdb.d/init.sql
    restart: unless-stopped

  redis:
    image: redis:7-alpine
    container_name: jt808-redis
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    restart: unless-stopped

  kafka:
    image: confluentinc/cp-kafka:latest
    container_name: jt808-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
    depends_on:
      - zookeeper
    restart: unless-stopped

  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    container_name: jt808-zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    restart: unless-stopped

volumes:
  mysql_data:
  redis_data:
```

#### 3. 启动服务

```bash
# 构建并启动
docker-compose up -d

# 查看日志
docker-compose logs -f jt808-server

# 停止服务
docker-compose down
```

### 集群部署

#### 1. 架构设计

```
                    ┌─────────────┐
                    │ Load Balancer│
                    │   (Nginx)   │
                    └──────┬──────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
     ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
     │ JT808     │   │ JT808     │   │ JT808     │
     │ Node 1    │   │ Node 2    │   │ Node 3    │
     └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
           │               │               │
           └───────────────┼───────────────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
     ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
     │  MySQL    │   │   Redis   │   │   Kafka   │
     │  Cluster  │   │  Cluster  │   │  Cluster  │
     └───────────┘   └───────────┘   └───────────┘
```

#### 2. Nginx 负载均衡配置

```nginx
upstream jt808_backend {
    least_conn;
    server jt808-node1:8080;
    server jt808-node2:8080;
    server jt808-node3:8080;
}

server {
    listen 8080;
    
    location / {
        proxy_pass jt808_backend;
        proxy_connect_timeout 30s;
        proxy_read_timeout 30s;
        proxy_send_timeout 30s;
    }
}
```

#### 3. Kubernetes 部署

创建 `jt808-deployment.yaml`：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jt808-server
  labels:
    app: jt808-server
spec:
  replicas: 3
  selector:
    matchLabels:
      app: jt808-server
  template:
    metadata:
      labels:
        app: jt808-server
    spec:
      containers:
      - name: jt808-server
        image: jt808-server:latest
        ports:
        - containerPort: 8080
        - containerPort: 9090
        env:
        - name: JT808_LISTEN_ADDR
          value: ":8080"
        - name: JT808_STORAGE_TYPE
          value: "mysql"
        - name: JT808_MYSQL_HOST
          valueFrom:
            secretKeyRef:
              name: jt808-secrets
              key: mysql-host
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        livenessProbe:
          tcpSocket:
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          tcpSocket:
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: jt808-service
spec:
  selector:
    app: jt808-server
  ports:
  - name: jt808
    port: 8080
    targetPort: 8080
  - name: metrics
    port: 9090
    targetPort: 9090
  type: LoadBalancer
```

## 环境配置

### 环境变量

| 环境变量 | 说明 | 默认值 |
|---------|------|--------|
| `JT808_LISTEN_ADDR` | 监听地址 | `:8080` |
| `JT808_MAX_CONNECTIONS` | 最大连接数 | `1000000` |
| `JT808_READ_TIMEOUT` | 读超时 | `30s` |
| `JT808_WRITE_TIMEOUT` | 写超时 | `30s` |
| `JT808_STORAGE_TYPE` | 存储类型 | `memory` |
| `JT808_MYSQL_HOST` | MySQL主机 | `localhost` |
| `JT808_MYSQL_PORT` | MySQL端口 | `3306` |
| `JT808_MYSQL_USER` | MySQL用户 | `root` |
| `JT808_MYSQL_PASSWORD` | MySQL密码 | - |
| `JT808_MYSQL_DATABASE` | MySQL数据库 | `jt808` |
| `JT808_REDIS_ADDR` | Redis地址 | `localhost:6379` |
| `JT808_KAFKA_BROKERS` | Kafka地址 | `localhost:9092` |
| `JT808_LOG_LEVEL` | 日志级别 | `info` |
| `JT808_METRICS_ENABLED` | 启用监控 | `true` |
| `JT808_METRICS_ADDR` | 监控地址 | `:9090` |

### 配置文件

完整的配置文件示例：

```yaml
server:
  listen_addr: ":8080"
  max_connections: 1000000
  read_timeout: 30s
  write_timeout: 30s
  idle_timeout: 300s
  read_buffer_size: 4096
  write_buffer_size: 4096
  max_packet_size: 4096

storage:
  type: "mysql"
  mysql:
    host: "localhost"
    port: 3306
    user: "jt808"
    password: "password"
    database: "jt808"
    max_open_conns: 100
    max_idle_conns: 10
    conn_max_lifetime: 3600s
  redis:
    addr: "localhost:6379"
    password: ""
    db: 0
    pool_size: 100

publisher:
  type: "kafka"
  kafka:
    brokers:
      - "localhost:9092"
    topic: "jt808-messages"
    batch_size: 100
    batch_timeout: 10ms

log:
  level: "info"
  format: "json"
  output: "file"
  file:
    path: "/var/log/jt808/server.log"
    max_size: 100MB
    max_backups: 10
    max_age: 30

metrics:
  enabled: true
  addr: ":9090"
  path: "/metrics"

health:
  enabled: true
  addr: ":8081"
  path: "/health"
```

## 性能调优

### 系统参数调优

#### Linux 内核参数

```bash
# /etc/sysctl.conf

# 最大文件描述符
fs.file-max = 1000000
fs.nr_open = 1000000

# 网络参数
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.tcp_fin_timeout = 10
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_keepalive_time = 600
net.ipv4.tcp_keepalive_intvl = 30
net.ipv4.tcp_keepalive_probes = 10

# TCP 缓冲区
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
```

应用配置：

```bash
sudo sysctl -p
```

#### 文件描述符限制

```bash
# /etc/security/limits.conf
* soft nofile 1000000
* hard nofile 1000000
```

### 应用参数调优

#### 连接数优化

```yaml
server:
  max_connections: 1000000
  read_buffer_size: 4096
  write_buffer_size: 4096
```

#### 内存优化

```yaml
# 使用连接池
pool:
  shard_count: 16
  max_idle_per_shard: 100
```

#### 并发优化

```yaml
worker:
  min_workers: 100
  max_workers: 10000
  queue_size: 100000
```

### 数据库优化

#### MySQL 优化

```sql
-- my.cnf
[mysqld]
max_connections = 1000
innodb_buffer_pool_size = 4G
innodb_log_file_size = 256M
innodb_flush_log_at_trx_commit = 2
innodb_flush_method = O_DIRECT
```

#### 连接池配置

```yaml
storage:
  mysql:
    max_open_conns: 100
    max_idle_conns: 10
    conn_max_lifetime: 3600s
```

## 监控告警

### Prometheus 指标

SDK 暴露以下 Prometheus 指标：

```yaml
# 连接指标
jt808_connections_active      # 当前活跃连接数
jt808_connections_total       # 总连接数
jt808_connections_rate        # 连接建立速率

# 消息指标
jt808_messages_received_total # 接收消息总数
jt808_messages_sent_total     # 发送消息总数
jt808_messages_rate           # 消息处理速率
jt808_messages_duration       # 消息处理延迟

# 系统指标
jt808_goroutines              # goroutine数量
jt808_memory_usage            # 内存使用量
```

### Prometheus 配置

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'jt808'
    static_configs:
      - targets: ['jt808-node1:9090', 'jt808-node2:9090', 'jt808-node3:9090']
    scrape_interval: 15s
```

### Grafana Dashboard

导入 Grafana Dashboard JSON：

```json
{
  "dashboard": {
    "title": "JT/T 808 Server",
    "panels": [
      {
        "title": "Active Connections",
        "targets": [{"expr": "jt808_connections_active"}]
      },
      {
        "title": "Message Rate",
        "targets": [{"expr": "rate(jt808_messages_received_total[5m])"}]
      }
    ]
  }
}
```

### 告警规则

```yaml
# alerts.yml
groups:
  - name: jt808
    rules:
      - alert: HighConnectionCount
        expr: jt808_connections_active > 800000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "连接数过高"
          
      - alert: HighErrorRate
        expr: rate(jt808_messages_errors_total[5m]) > 100
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "错误率过高"
```

## 日志管理

### 日志配置

```yaml
log:
  level: "info"
  format: "json"
  output: "file"
  file:
    path: "/var/log/jt808/server.log"
    max_size: 100MB
    max_backups: 10
    max_age: 30
```

### 日志轮转

使用 logrotate 配置日志轮转：

```bash
# /etc/logrotate.d/jt808
/var/log/jt808/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 0644 jt808 jt808
    postrotate
        systemctl reload jt808-server
    endscript
}
```

### ELK 集成

#### Filebeat 配置

```yaml
# filebeat.yml
filebeat.inputs:
- type: log
  paths:
    - /var/log/jt808/*.log
  json.keys_under_root: true

output.elasticsearch:
  hosts: ["elasticsearch:9200"]
  index: "jt808-%{+yyyy.MM.dd}"
```

## 安全配置

### TLS 配置

```yaml
server:
  tls:
    enabled: true
    cert_file: "/etc/jt808/cert.pem"
    key_file: "/etc/jt808/key.pem"
```

### 防火墙配置

```bash
# 只允许特定IP访问
sudo iptables -A INPUT -p tcp --dport 8080 -s 10.0.0.0/8 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 8080 -j DROP
```

### DDoS 防护

```yaml
middleware:
  ratelimit:
    enabled: true
    rate: 1000
    burst: 2000
```

## 运维建议

### 1. 监控要点

- 连接数：监控活跃连接数，设置告警阈值
- 消息延迟：监控消息处理延迟，确保在可接受范围内
- 错误率：监控错误率，及时发现问题
- 资源使用：监控 CPU、内存、磁盘使用情况

### 2. 备份策略

```bash
# 数据库备份
mysqldump -u root -p jt808 > backup_$(date +%Y%m%d).sql

# 定时备份
0 2 * * * /opt/jt808/scripts/backup.sh
```

### 3. 扩容策略

- **垂直扩容**：增加单机资源（CPU、内存）
- **水平扩容**：增加节点数量，使用负载均衡

### 4. 故障处理

```bash
# 查看服务状态
sudo systemctl status jt808-server

# 查看日志
sudo journalctl -u jt808-server -f

# 重启服务
sudo systemctl restart jt808-server
```

### 5. 性能测试

```bash
# 压力测试
go run test/stress/main.go -s 127.0.0.1:8080 -c 10000 -d 300 -t location
```

## 下一步

- [快速开始指南](getting-started.md) - 快速上手使用
- [API 文档](api/README.md) - 完整的 API 参考
- [架构设计文档](architecture.md) - 了解系统架构
