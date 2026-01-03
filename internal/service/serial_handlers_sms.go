package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dushixiang/uart_sms_forwarder/internal/models"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// IncomingSMS 接收的短信消息结构
type IncomingSMS struct {
	Timestamp int64  `json:"timestamp"`
	From      string `json:"from"`
	Content   string `json:"content"`
	Type      string `json:"type"`
}

func (r IncomingSMS) String() string {
	timestamp := time.Unix(r.Timestamp, 0)
	message := fmt.Sprintf(`%s
----
来自: %s
%s
`,
		r.Content,
		r.From,
		timestamp.Format(time.DateTime),
	)
	return message
}

// handleIncomingSMS 处理接收到的短信
func (s *SerialService) handleIncomingSMS(msg *ParsedMessage) {
	var sms IncomingSMS
	if err := json.Unmarshal([]byte(msg.JSON), &sms); err != nil {
		s.logger.Error("短信消息解析失败", zap.Error(err))
		return
	}

	s.logger.Info("收到新短信",
		zap.String("from", sms.From),
		zap.String("content", sms.Content),
		zap.Int64("timestamp", sms.Timestamp))

	// 保存短信记录
	ctx := context.Background()
	record := &models.TextMessage{
		ID:        uuid.NewString(),
		From:      sms.From,
		To:        "", // 接收方是本机
		Content:   sms.Content,
		Type:      models.MessageTypeIncoming,
		Status:    models.MessageStatusReceived,
		CreatedAt: time.Now().UnixMilli(),
	}

	if err := s.textMsgService.Save(ctx, record); err != nil {
		s.logger.Error("保存短信记录失败", zap.Error(err))
	}

	// 异步发送通知
	go s.sendNotification(ctx, sms)
}

// sendNotification 发送通知
func (s *SerialService) sendNotification(ctx context.Context, sms IncomingSMS) {
	// 转换为通用通知消息
	msg := NotificationMessage{
		Type:      "sms",
		From:      sms.From,
		Content:   sms.Content,
		Timestamp: sms.Timestamp,
	}

	s.sendNotificationMessage(ctx, msg)
}

// sendNotificationMessage 发送通用通知消息
func (s *SerialService) sendNotificationMessage(ctx context.Context, msg NotificationMessage) {
	// 获取通知渠道配置
	channels, err := s.propertyService.GetNotificationChannelConfigs(ctx)
	if err != nil {
		s.logger.Error("获取通知渠道配置失败", zap.Error(err))
		return
	}

	// 格式化消息
	message := msg.String()

	// 发送到所有启用的渠道
	for _, channel := range channels {
		if !channel.Enabled {
			continue
		}

		var sendErr error
		switch channel.Type {
		case "dingtalk":
			sendErr = s.notifier.SendDingTalkByConfig(ctx, channel.Config, message)
		case "wecom":
			sendErr = s.notifier.SendWeComByConfig(ctx, channel.Config, message)
		case "feishu":
			sendErr = s.notifier.SendFeishuByConfig(ctx, channel.Config, message)
		case "webhook":
			sendErr = s.notifier.SendWebhookByConfig(ctx, channel.Config, msg)
		case "email":
			sendErr = s.notifier.SendEmail(ctx, channel.Config, msg)
		case "telegram":
			sendErr = s.notifier.sendTelegramByConfig(ctx, channel.Config, message)
		}

		if sendErr != nil {
			s.logger.Error("发送通知失败",
				zap.String("type", channel.Type),
				zap.Error(sendErr))
		} else {
			s.logger.Info("通知发送成功", zap.String("type", channel.Type))
		}
	}
}

// handleSMSSendResult 处理短信发送结果
func (s *SerialService) handleSMSSendResult(msg *ParsedMessage) {
	success, _ := msg.Payload["success"].(bool)
	to, _ := msg.Payload["to"].(string)
	requestID, _ := msg.Payload["request_id"].(string)

	if requestID == "" {
		s.logger.Warn("收到短信发送结果但缺少 request_id", zap.Any("msg", msg.Payload))
		return
	}

	ctx := context.Background()
	var status models.MessageStatus
	var lastRunStatus models.LastRunStatus
	if success {
		status = models.MessageStatusSent
		lastRunStatus = models.LastRunStatusSuccess
		s.logger.Info("短信发送成功",
			zap.String("to", to),
			zap.String("request_id", requestID))
	} else {
		status = models.MessageStatusFailed
		lastRunStatus = models.LastRunStatusFailed
		s.logger.Warn("短信发送失败",
			zap.String("to", to),
			zap.String("request_id", requestID))
		go s.sendNotificationMessage(context.Background(), NotificationMessage{
			Type:      "sms",
			From:      "UART 短信转发器",
			Content:   fmt.Sprintf("短信发送失败: %s", to),
			Timestamp: time.Now().Unix(),
		})
	}

	if err := s.textMsgService.UpdateStatusById(ctx, requestID, status); err != nil {
		s.logger.Error("更新短信状态失败",
			zap.String("request_id", requestID),
			zap.Error(err))
	}

	s.updateScheduledTaskStatus(ctx, requestID, lastRunStatus)
}

func (s *SerialService) updateScheduledTaskStatus(ctx context.Context, msgID string, status models.LastRunStatus) {
	if s.scheduledTaskStatusUpdater == nil {
		return
	}
	if err := s.scheduledTaskStatusUpdater(ctx, msgID, status); err != nil {
		s.logger.Error("更新定时任务状态失败",
			zap.String("request_id", msgID),
			zap.Error(err))
	}
}
