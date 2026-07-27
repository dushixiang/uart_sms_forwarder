package middleware

import (
	"net/http"
	"strings"

	"github.com/dushixiang/uart_sms_forwarder/internal/util"
	"github.com/labstack/echo/v5"
	"go.uber.org/zap"
)

const (
	// ContextKeyUsername Context 中用户名的 key
	ContextKeyUsername = "username"
)

// JWTMiddleware JWT 认证中间件
func JWTMiddleware(secret string, logger *zap.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			// 获取 Authorization header
			authHeader := c.Request().Header.Get("Authorization")
			if authHeader == "" {
				logger.Warn("缺少 Authorization header")
				return c.JSON(http.StatusUnauthorized, map[string]string{
					"error": "缺少认证信息",
				})
			}

			// 提取 Bearer token
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || parts[0] != "Bearer" {
				logger.Warn("Authorization header 格式错误", zap.String("header", authHeader))
				return c.JSON(http.StatusUnauthorized, map[string]string{
					"error": "认证信息格式错误",
				})
			}

			tokenString := parts[1]

			// 验证 token
			claims, err := util.VerifyToken(tokenString, secret)
			if err != nil {
				logger.Warn("token 验证失败", zap.Error(err))
				return c.JSON(http.StatusUnauthorized, map[string]string{
					"error": "认证失败：" + err.Error(),
				})
			}

			// 将用户名存入 context
			c.Set(ContextKeyUsername, claims.Username)

			// 继续处理请求
			return next(c)
		}
	}
}

// GetUsername 从 context 中获取用户名
func GetUsername(c *echo.Context) string {
	if username, ok := c.Get(ContextKeyUsername).(string); ok {
		return username
	}
	return ""
}
