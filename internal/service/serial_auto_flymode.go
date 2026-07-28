package service

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

const (
	autoFlymodeCheckInterval  = 10 * time.Second
	cellularReadyTimeout      = 45 * time.Second
	cellularReadyPollInterval = 2 * time.Second
	manualFlymodeRestoreDelay = 30 * time.Second
)

type flymodeChangeSource string

const (
	flymodeChangeAutomatic flymodeChangeSource = "自动"
	flymodeChangeManual    flymodeChangeSource = "手动"
)

// StartAutoFlymodeMonitor 启动短信空闲监控。服务启动或配置刚启用时，
// 都会从当前时间开始计算完整的空闲周期。
func (s *SerialService) StartAutoFlymodeMonitor(ctx context.Context) {
	ticker := time.NewTicker(autoFlymodeCheckInterval)
	defer ticker.Stop()

	wasEnabled := false
	s.evaluateAutoFlymode(ctx, &wasEnabled)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("自动飞行模式监控已停止")
			return
		case <-ticker.C:
			s.evaluateAutoFlymode(ctx, &wasEnabled)
		}
	}
}

func (s *SerialService) evaluateAutoFlymode(ctx context.Context, wasEnabled *bool) {
	config, err := s.propertyService.GetAutoFlymodeConfig(ctx)
	if err != nil {
		s.logger.Error("读取自动飞行模式配置失败，本轮跳过", zap.Error(err))
		return
	}

	if !config.Enabled {
		if *wasEnabled && s.autoFlymodeActive.Load() && s.FlyMode() {
			if err := s.setFlymode(false, flymodeChangeAutomatic, "自动飞行模式配置已停用"); err != nil {
				s.logger.Error("关闭自动飞行模式后退出飞行模式失败", zap.Error(err))
				return
			}
			s.autoFlymodeActive.Store(false)
			s.recordSMSActivity()
			s.logger.Info("自动飞行模式已关闭，设备已退出自动开启的飞行模式")
		}
		*wasEnabled = false
		return
	}

	if !*wasEnabled {
		*wasEnabled = true
		s.recordSMSActivity()
		s.logger.Info("自动飞行模式已启用",
			zap.Int64("idle_timeout_hours", config.IdleTimeoutHours))
		return
	}

	_, connected := s.getConnectionInfo()
	if !connected || s.FlyMode() || s.smsOperationRunning.Load() {
		return
	}

	lastActivity := time.UnixMilli(s.lastSMSActivityAt.Load())
	if !isAutoFlymodeDue(time.Now(), lastActivity, config.IdleTimeoutHours) {
		return
	}

	if err := s.setFlymode(
		true,
		flymodeChangeAutomatic,
		fmt.Sprintf("短信已空闲 %d 小时", config.IdleTimeoutHours),
	); err != nil {
		s.logger.Error("自动进入飞行模式失败", zap.Error(err))
		return
	}
	s.autoFlymodeActive.Store(true)
	s.logger.Info("短信空闲时间达到阈值，已自动进入飞行模式",
		zap.Int64("idle_timeout_hours", config.IdleTimeoutHours),
		zap.Time("last_sms_activity_at", lastActivity))
}

func isAutoFlymodeDue(now, lastActivity time.Time, idleTimeoutHours int64) bool {
	return !now.Before(lastActivity.Add(time.Duration(idleTimeoutHours) * time.Hour))
}

func (s *SerialService) recordSMSActivity() {
	s.lastSMSActivityAt.Store(time.Now().UnixMilli())
}

func (s *SerialService) notifyFlymodeChanged(source flymodeChangeSource, enabled bool, reason string) {
	if s.propertyService == nil || s.notifier == nil {
		return
	}

	status := "关闭"
	if enabled {
		status = "开启"
	}

	content := fmt.Sprintf("飞行模式已%s", status)
	if reason != "" {
		content += "\n原因: " + reason
	}

	go s.sendNotificationMessage(context.Background(), NotificationMessage{
		Type:      "flymode",
		From:      string(source),
		Content:   content,
		Timestamp: time.Now().Unix(),
	})
}

// prepareNetworkForSMS 在自动或手动飞行模式下临时恢复蜂窝网络。
// 返回 true 表示原状态来自用户手动设置，短信完成后需要恢复飞行模式。
func (s *SerialService) prepareNetworkForSMS(ctx context.Context) (bool, uint64, error) {
	if !s.FlyMode() {
		return false, 0, nil
	}

	wasAutomatic := s.autoFlymodeActive.Load()
	manualFlymodeGen := s.manualFlymodeGen.Load()
	reason := "发送短信前临时恢复蜂窝网络"
	if wasAutomatic {
		reason += "（原飞行模式由自动策略开启）"
	} else {
		reason += "（原飞行模式由用户手动开启）"
	}
	if err := s.setFlymode(false, flymodeChangeAutomatic, reason); err != nil {
		return false, 0, fmt.Errorf("发送短信前退出飞行模式失败: %w", err)
	}
	s.autoFlymodeActive.Store(false)
	s.recordSMSActivity()

	s.logger.Info("发送短信前已退出飞行模式，等待移动网络注册",
		zap.Bool("was_automatic", wasAutomatic))
	if err := s.waitForCellularReady(ctx); err != nil {
		if !wasAutomatic {
			s.restoreManualFlymode(manualFlymodeGen)
		}
		return false, 0, err
	}

	return !wasAutomatic, manualFlymodeGen, nil
}

func (s *SerialService) waitForCellularReady(ctx context.Context) error {
	waitCtx, cancel := context.WithTimeout(ctx, cellularReadyTimeout)
	defer cancel()

	// 丢弃飞行模式前的旧状态，确保等待的是退出飞行模式后的新回包。
	s.deviceCache.Delete(CacheKeyDeviceStatus)
	s.RequestCacheUpdate()

	ticker := time.NewTicker(cellularReadyPollInterval)
	defer ticker.Stop()

	for {
		if status, ok := s.deviceCache.Get(CacheKeyDeviceStatus); ok && status.Mobile.IsRegistered {
			s.logger.Info("移动网络注册成功，可以发送短信")
			return nil
		}

		select {
		case <-waitCtx.Done():
			return fmt.Errorf("退出飞行模式后等待移动网络注册超时")
		case <-ticker.C:
			s.RequestCacheUpdate()
		}
	}
}

func (s *SerialService) restoreManualFlymode(manualFlymodeGen uint64) {
	go func() {
		time.Sleep(manualFlymodeRestoreDelay)
		if s.manualFlymodeGen.Load() != manualFlymodeGen {
			s.logger.Info("用户已重新设置飞行模式，跳过旧状态恢复")
			return
		}
		if err := s.setFlymode(true, flymodeChangeAutomatic, "短信发送完成后恢复用户设置"); err != nil {
			s.logger.Error("恢复用户手动设置的飞行模式失败", zap.Error(err))
			return
		}
		s.autoFlymodeActive.Store(false)
		s.logger.Info("已恢复用户手动设置的飞行模式")
	}()
}

func (s *SerialService) restoreManualFlymodeAfterResult(msgID string) {
	value, ok := s.restoreFlymodeByMsg.LoadAndDelete(msgID)
	if !ok {
		return
	}
	manualFlymodeGen, ok := value.(uint64)
	if !ok {
		s.logger.Error("恢复飞行模式状态无效", zap.String("request_id", msgID))
		return
	}
	s.restoreManualFlymode(manualFlymodeGen)
}
