package handler

import (
	"net/http"

	"github.com/dushixiang/uart_sms_forwarder/internal/service"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// SerialHandler 串口控制API处理器
type SerialHandler struct {
	logger        *zap.Logger
	serialService *service.SerialService
}

// NewSerialHandler 创建串口Handler实例
func NewSerialHandler(logger *zap.Logger, serialService *service.SerialService) *SerialHandler {
	return &SerialHandler{
		logger:        logger,
		serialService: serialService,
	}
}

// SendSMSRequest 发送短信请求
type SendSMSRequest struct {
	To      string `json:"to"`
	Content string `json:"content"`
}

// SendSMS 发送短信
// POST /api/serial/sms
// Body: {"to": "13800138000", "content": "测试短信"}
func (h *SerialHandler) SendSMS(c echo.Context) error {
	var req SendSMSRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "请求参数错误",
		})
	}

	if req.To == "" || req.Content == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "手机号和内容不能为空",
		})
	}

	if _, err := h.serialService.SendSMS(req.To, req.Content); err != nil {
		h.logger.Error("发送短信失败", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "发送失败",
		})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"message": "发送成功",
	})
}

// GetStatus 获取设备状态（包含移动网络信息）
// GET /api/serial/status
func (h *SerialHandler) GetStatus(c echo.Context) error {
	data, err := h.serialService.GetStatus()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": err.Error(),
		})
	}

	return c.JSON(http.StatusOK, data)
}

// ResetStack 重启协议栈
// POST /api/serial/reset
func (h *SerialHandler) ResetStack(c echo.Context) error {
	err := h.serialService.ResetStack()
	if err != nil {
		h.logger.Error("重启协议栈失败", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": err.Error(),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// RebootMcu 重启模块
// POST /api/serial/reboot
func (h *SerialHandler) RebootMcu(c echo.Context) error {
	err := h.serialService.RebootMcu()
	if err != nil {
		h.logger.Error("重启模块", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": err.Error(),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
