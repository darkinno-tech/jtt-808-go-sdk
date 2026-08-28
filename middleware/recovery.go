package middleware

import (
	"context"
	"runtime/debug"

	"github.com/darkinno-tech/jtt-808-go-sdk/core"
	"github.com/darkinno-tech/jtt-808-go-sdk/logger"
)

// Recovery Panic恢复中间件
// 捕获处理器中的panic，记录错误日志并返回错误，防止服务器崩溃
func Recovery(log *logger.Logger) core.Middleware {
	return func(next core.MessageHandler) core.MessageHandler {
		return func(ctx context.Context, conn core.Connection, msg *core.Message) (err error) {
			defer func() {
				if r := recover(); r != nil {
					// 获取堆栈信息
					stack := debug.Stack()

					// 记录panic日志
					log.Error("Panic恢复",
						logger.String("device_id", conn.DeviceID()),
						logger.Int("msg_id", int(msg.Header.MsgID)),
						logger.String("panic", string(debug.Stack())),
					)

					// 返回错误
					err = &PanicError{
						Value: r,
						Stack: stack,
					}
				}
			}()

			// 调用下一个处理器
			return next(ctx, conn, msg)
		}
	}
}

// PanicError panic错误
type PanicError struct {
	// Value panic的值
	Value interface{}
	// Stack 堆栈信息
	Stack []byte
}

// Error 实现error接口
func (e *PanicError) Error() string {
	return "panic recovered"
}
