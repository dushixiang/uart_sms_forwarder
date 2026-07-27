package handler

import (
	"net/http"

	"github.com/dushixiang/uart_sms_forwarder/internal/service"
	"github.com/labstack/echo/v5"
	"go.uber.org/zap"
)

// AuthHandler 认证处理器
type AuthHandler struct {
	logger         *zap.Logger
	accountService *service.AccountService
}

// NewAuthHandler 创建认证处理器
func NewAuthHandler(logger *zap.Logger, accountService *service.AccountService) *AuthHandler {
	return &AuthHandler{
		logger:         logger,
		accountService: accountService,
	}
}

// LoginRequest 登录请求
type LoginRequest struct {
	Username string `json:"username" validate:"required"`
	Password string `json:"password" validate:"required"`
}

// LoginResponse 登录响应
type LoginResponse struct {
	Token     string `json:"token"`
	Username  string `json:"username"`
	ExpiresAt int64  `json:"expiresAt"`
}

// Login 处理登录请求
func (h *AuthHandler) Login(c *echo.Context) error {
	// 获取请求参数
	var req LoginRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "请求参数错误",
		})
	}

	// 验证必填字段
	if req.Username == "" || req.Password == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "用户名和密码不能为空",
		})
	}

	// 使用 AccountService 进行登录
	ctx := c.Request().Context()
	loginResp, err := h.accountService.Login(ctx, req.Username, req.Password)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "用户名或密码错误",
		})
	}

	// 返回 token 和用户信息
	return c.JSON(http.StatusOK, LoginResponse{
		Token:     loginResp.Token,
		Username:  loginResp.User.Username,
		ExpiresAt: loginResp.ExpiresAt,
	})
}

// GetAuthConfig 获取认证配置
func (h *AuthHandler) GetAuthConfig(c *echo.Context) error {
	config := h.accountService.GetAuthConfig()
	return c.JSON(http.StatusOK, config)
}

// GetOIDCAuthURL 获取 OIDC 认证 URL
func (h *AuthHandler) GetOIDCAuthURL(c *echo.Context) error {
	authURL, err := h.accountService.GetOIDCAuthURL()
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": err.Error(),
		})
	}
	return c.JSON(http.StatusOK, authURL)
}

// OIDCCallbackRequest OIDC 回调请求
type OIDCCallbackRequest struct {
	Code  string `json:"code" validate:"required"`
	State string `json:"state" validate:"required"`
}

// OIDCCallback 处理 OIDC 回调
func (h *AuthHandler) OIDCCallback(c *echo.Context) error {
	var req OIDCCallbackRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "请求参数错误",
		})
	}

	if req.Code == "" || req.State == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "缺少必要参数",
		})
	}

	// 使用 AccountService 处理 OIDC 登录
	ctx := c.Request().Context()
	loginResp, err := h.accountService.LoginWithOIDC(ctx, req.Code, req.State)
	if err != nil {
		h.logger.Error("OIDC 登录失败", zap.Error(err))
		return c.JSON(http.StatusUnauthorized, map[string]string{
			"error": "OIDC 认证失败",
		})
	}

	// 返回 token 和用户信息
	return c.JSON(http.StatusOK, LoginResponse{
		Token:     loginResp.Token,
		Username:  loginResp.User.Username,
		ExpiresAt: loginResp.ExpiresAt,
	})
}
