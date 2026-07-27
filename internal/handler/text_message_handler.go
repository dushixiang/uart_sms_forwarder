package handler

import (
	"net/http"
	"net/url"

	"github.com/dushixiang/uart_sms_forwarder/internal/repo"
	"github.com/dushixiang/uart_sms_forwarder/internal/service"

	"github.com/labstack/echo/v5"
	"go.uber.org/zap"
)

// TextMessageHandler 短信API处理器
type TextMessageHandler struct {
	logger  *zap.Logger
	service *service.TextMessageService
	repo    *repo.TextMessageRepo
}

// NewTextMessageHandler 创建短信Handler实例
func NewTextMessageHandler(logger *zap.Logger, service *service.TextMessageService, repo *repo.TextMessageRepo) *TextMessageHandler {
	return &TextMessageHandler{
		logger:  logger,
		service: service,
		repo:    repo,
	}
}

// Delete 删除单条短信
// DELETE /api/messages/:id
func (h *TextMessageHandler) Delete(c *echo.Context) error {
	id := c.Param("id")
	if id == "" {
	}

	if err := h.service.Delete(c.Request().Context(), id); err != nil {
		h.logger.Error("删除短信失败", zap.Error(err), zap.String("id", id))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "删除失败",
		})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"message": "删除成功",
	})
}

// Clear 清空所有短信
// DELETE /api/messages
func (h *TextMessageHandler) Clear(c *echo.Context) error {
	if err := h.service.Clear(c.Request().Context()); err != nil {
		h.logger.Error("清空短信失败", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "清空失败",
		})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"message": "清空成功",
	})
}

// GetStats 获取统计信息
// GET /api/messages/stats
func (h *TextMessageHandler) GetStats(c *echo.Context) error {
	stats, err := h.service.GetStats(c.Request().Context())
	if err != nil {
		h.logger.Error("获取统计信息失败", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "获取统计信息失败",
		})
	}

	return c.JSON(http.StatusOK, stats)
}

// GetConversations 获取会话列表
// GET /api/messages/conversations
func (h *TextMessageHandler) GetConversations(c *echo.Context) error {
	conversations, err := h.service.GetConversations(c.Request().Context())
	if err != nil {
		h.logger.Error("获取会话列表失败", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "获取会话列表失败",
		})
	}

	return c.JSON(http.StatusOK, conversations)
}

// GetConversationMessages 获取指定会话的所有消息
// GET /api/messages/conversations/:peer/messages
func (h *TextMessageHandler) GetConversationMessages(c *echo.Context) error {
	peer := c.Param("peer")
	if peer == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "peer 参数不能为空",
		})
	}

	// 手动 URL 解码以处理特殊字符（如 + 号）
	decodedPeer, err := url.QueryUnescape(peer)
	if err != nil {
		h.logger.Error("URL 解码失败", zap.Error(err), zap.String("peer", peer))
		// 如果解码失败，使用原始值
		decodedPeer = peer
	}

	h.logger.Debug("获取会话消息",
		zap.String("peer_raw", peer),
		zap.String("peer_decoded", decodedPeer))

	messages, err := h.service.GetConversationMessages(c.Request().Context(), decodedPeer)
	if err != nil {
		h.logger.Error("获取会话消息失败", zap.Error(err), zap.String("peer", decodedPeer))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "获取会话消息失败",
		})
	}

	return c.JSON(http.StatusOK, messages)
}

// DeleteConversation 删除整个会话（与某个联系人的所有消息）
// DELETE /api/messages/conversations/:peer
func (h *TextMessageHandler) DeleteConversation(c *echo.Context) error {
	peer := c.Param("peer")
	if peer == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "peer 参数不能为空",
		})
	}

	// 手动 URL 解码以处理特殊字符（如 + 号）
	decodedPeer, err := url.QueryUnescape(peer)
	if err != nil {
		h.logger.Error("URL 解码失败", zap.Error(err), zap.String("peer", peer))
		// 如果解码失败，使用原始值
		decodedPeer = peer
	}

	h.logger.Debug("删除会话",
		zap.String("peer_raw", peer),
		zap.String("peer_decoded", decodedPeer))

	if err := h.service.DeleteConversation(c.Request().Context(), decodedPeer); err != nil {
		h.logger.Error("删除会话失败", zap.Error(err), zap.String("peer", decodedPeer))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "删除会话失败",
		})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"message": "删除成功",
	})
}
