package middleware

import (
	"context"
	"time"

	"github.com/im10furry/jtt-808-go-sdk/core"
)

// Timeout 超时控制中间件
// 为请求处理设置超时时间，防止处理器长时间阻塞
func Timeout(timeout time.Duration) core.Middleware {
	return func(next core.MessageHandler) core.MessageHandler {
		return func(ctx context.Context, conn core.Connection, msg *core.Message) error {
			// 创建带超时的上下文
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			// 创建错误通道
			errChan := make(chan error, 1)

			// 在goroutine中执行处理器
			go func() {
				errChan <- next(ctx, conn, msg)
			}()

			// 等待处理完成或超时
			select {
			case err := <-errChan:
				return err
			case <-ctx.Done():
				return &TimeoutError{
					Timeout: timeout,
					Message: "请求处理超时",
				}
			}
		}
	}
}

// TimeoutError 超时错误
type TimeoutError struct {
	// Timeout 超时时间
	Timeout time.Duration
	// Message 错误信息
	Message string
}

// Error 实现error接口
func (e *TimeoutError) Error() string {
	return e.Message
}
