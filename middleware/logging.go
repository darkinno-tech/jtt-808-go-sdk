package middleware

import (
	"context"
	"time"

	"github.com/darkinno-tech/jtt-808-go-sdk/core"
	"github.com/darkinno-tech/jtt-808-go-sdk/logger"
)

// Logging 请求日志记录中间件
// 记录每个请求的处理时间、消息ID、设备ID等信息
func Logging(log *logger.Logger) core.Middleware {
	return func(next core.MessageHandler) core.MessageHandler {
		return func(ctx context.Context, conn core.Connection, msg *core.Message) error {
			start := time.Now()

			// 记录请求开始
			log.Info("请求开始",
				logger.String("device_id", conn.DeviceID()),
				logger.Int("msg_id", int(msg.Header.MsgID)),
				logger.String("remote_addr", conn.RemoteAddr().String()),
			)

			// 调用下一个处理器
			err := next(ctx, conn, msg)

			// 计算处理时间
			duration := time.Since(start)

			// 记录请求结束
			if err != nil {
				log.Error("请求失败",
					logger.String("device_id", conn.DeviceID()),
					logger.Int("msg_id", int(msg.Header.MsgID)),
					logger.Duration("duration", duration),
					logger.Error("error", err),
				)
			} else {
				log.Info("请求完成",
					logger.String("device_id", conn.DeviceID()),
					logger.Int("msg_id", int(msg.Header.MsgID)),
					logger.Duration("duration", duration),
				)
			}

			return err
		}
	}
}
