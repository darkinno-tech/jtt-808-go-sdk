package middleware

import (
	"context"
	"sync"
	"time"

	"github.com/darkinno-tech/jtt-808-go-sdk/core"
)

// RateLimit 限流中间件
// 使用令牌桶算法限制请求速率
func RateLimit(rate int, burst int) core.Middleware {
	limiter := NewTokenBucket(rate, burst)

	return func(next core.MessageHandler) core.MessageHandler {
		return func(ctx context.Context, conn core.Connection, msg *core.Message) error {
			// 检查是否允许请求
			if !limiter.Allow() {
				return &RateLimitError{
					Message: "请求频率超过限制",
				}
			}

			// 调用下一个处理器
			return next(ctx, conn, msg)
		}
	}
}

// TokenBucket 令牌桶
type TokenBucket struct {
	rate     int        // 每秒生成的令牌数
	burst    int        // 桶容量
	tokens   float64    // 当前令牌数
	lastTime time.Time  // 上次更新时间
	mu       sync.Mutex // 互斥锁
}

// NewTokenBucket 创建令牌桶
func NewTokenBucket(rate int, burst int) *TokenBucket {
	return &TokenBucket{
		rate:     rate,
		burst:    burst,
		tokens:   float64(burst),
		lastTime: time.Now(),
	}
}

// Allow 检查是否允许请求
func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastTime)
	tb.lastTime = now

	// 添加令牌
	tb.tokens += elapsed.Seconds() * float64(tb.rate)
	if tb.tokens > float64(tb.burst) {
		tb.tokens = float64(tb.burst)
	}

	// 检查是否有足够的令牌
	if tb.tokens < 1 {
		return false
	}

	// 消耗一个令牌
	tb.tokens--
	return true
}

// RateLimitError 限流错误
type RateLimitError struct {
	// Message 错误信息
	Message string
}

// Error 实现error接口
func (e *RateLimitError) Error() string {
	return e.Message
}
