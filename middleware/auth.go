package middleware

import (
	"context"
	"errors"

	"github.com/darkinno-tech/jtt-808-go-sdk/core"
	"github.com/darkinno-tech/jtt-808-go-sdk/protocol/types"
)

// Auth 终端鉴权中间件
// 检查终端是否已经通过鉴权，未鉴权的终端请求将被拒绝
// 跳过终端注册和终端鉴权消息的验证
func Auth() core.Middleware {
	return func(next core.MessageHandler) core.MessageHandler {
		return func(ctx context.Context, conn core.Connection, msg *core.Message) error {
			// 跳过终端注册消息
			if msg.Header.MsgID == types.MsgIDTerminalRegister {
				return next(ctx, conn, msg)
			}

			// 跳过终端鉴权消息
			if msg.Header.MsgID == types.MsgIDTerminalAuth {
				return next(ctx, conn, msg)
			}

			// 检查终端是否已鉴权
			authenticated, exists := conn.Get("authenticated")
			if !exists || !authenticated.(bool) {
				return &AuthError{
					DeviceID: conn.DeviceID(),
					Message:  "终端未鉴权",
				}
			}

			// 调用下一个处理器
			return next(ctx, conn, msg)
		}
	}
}

// SetAuthenticated 设置终端已鉴权状态
// 在终端鉴权成功后调用此函数
func SetAuthenticated(conn core.Connection) {
	conn.Set("authenticated", true)
}

// ClearAuthenticated 清除终端鉴权状态
// 在终端注销或连接断开时调用此函数
func ClearAuthenticated(conn core.Connection) {
	conn.Set("authenticated", false)
}

// AuthError 鉴权错误
type AuthError struct {
	// DeviceID 设备ID
	DeviceID string
	// Message 错误信息
	Message string
}

// Error 实现error接口
func (e *AuthError) Error() string {
	return e.Message
}

// AuthWithCode 使用鉴权码验证的鉴权中间件
// 通过验证函数检查鉴权码是否有效
func AuthWithCode(validateFunc func(deviceID string, authCode string) bool) core.Middleware {
	return func(next core.MessageHandler) core.MessageHandler {
		return func(ctx context.Context, conn core.Connection, msg *core.Message) error {
			// 跳过终端注册消息
			if msg.Header.MsgID == types.MsgIDTerminalRegister {
				return next(ctx, conn, msg)
			}

			// 处理终端鉴权消息
			if msg.Header.MsgID == types.MsgIDTerminalAuth {
				// 调用下一个处理器处理鉴权消息
				err := next(ctx, conn, msg)
				if err != nil {
					return err
				}

				// 鉴权成功后设置已鉴权状态
				SetAuthenticated(conn)
				return nil
			}

			// 检查终端是否已鉴权
			authenticated, exists := conn.Get("authenticated")
			if !exists || !authenticated.(bool) {
				return &AuthError{
					DeviceID: conn.DeviceID(),
					Message:  "终端未鉴权",
				}
			}

			// 调用下一个处理器
			return next(ctx, conn, msg)
		}
	}
}

// AuthWithStorage 使用存储接口验证的鉴权中间件
// 通过存储接口验证终端是否已注册
func AuthWithStorage(storage core.Storage) core.Middleware {
	return func(next core.MessageHandler) core.MessageHandler {
		return func(ctx context.Context, conn core.Connection, msg *core.Message) error {
			// 跳过终端注册消息
			if msg.Header.MsgID == types.MsgIDTerminalRegister {
				return next(ctx, conn, msg)
			}

			// 跳过终端鉴权消息
			if msg.Header.MsgID == types.MsgIDTerminalAuth {
				return next(ctx, conn, msg)
			}

			// 检查终端是否已鉴权
			authenticated, exists := conn.Get("authenticated")
			if !exists || !authenticated.(bool) {
				// 尝试从存储中获取设备信息
				deviceID := conn.DeviceID()
				if deviceID == "" {
					return errors.New("设备ID为空")
				}

				// 检查设备是否已注册
				_, err := storage.GetDevice(ctx, deviceID)
				if err != nil {
					return &AuthError{
						DeviceID: deviceID,
						Message:  "终端未注册",
					}
				}

				// 设置已鉴权状态
				SetAuthenticated(conn)
			}

			// 调用下一个处理器
			return next(ctx, conn, msg)
		}
	}
}
