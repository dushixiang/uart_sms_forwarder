package service

import (
	"strings"
	"testing"
	"time"

	"github.com/dushixiang/uart_sms_forwarder/internal/models"
)

func TestValidateAutoFlymodeConfig(t *testing.T) {
	tests := []struct {
		name    string
		hours   int64
		wantErr bool
	}{
		{name: "minimum", hours: MinAutoFlymodeIdleTimeoutHours},
		{name: "maximum", hours: MaxAutoFlymodeIdleTimeoutHours},
		{name: "zero", hours: 0, wantErr: true},
		{name: "negative", hours: -1, wantErr: true},
		{name: "over maximum", hours: MaxAutoFlymodeIdleTimeoutHours + 1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAutoFlymodeConfig(models.AutoFlymodeConfig{
				Enabled:          true,
				IdleTimeoutHours: tt.hours,
			})
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateAutoFlymodeConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIsAutoFlymodeDue(t *testing.T) {
	lastActivity := time.Date(2026, time.July, 27, 8, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		now  time.Time
		want bool
	}{
		{name: "before timeout", now: lastActivity.Add(59 * time.Minute), want: false},
		{name: "at timeout", now: lastActivity.Add(time.Hour), want: true},
		{name: "after timeout", now: lastActivity.Add(2 * time.Hour), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAutoFlymodeDue(tt.now, lastActivity, 1); got != tt.want {
				t.Fatalf("isAutoFlymodeDue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFlymodeNotificationMessage(t *testing.T) {
	message := NotificationMessage{
		Type:      "flymode",
		From:      string(flymodeChangeAutomatic),
		Content:   "飞行模式已开启\n原因: 短信已空闲 1 小时",
		Timestamp: time.Date(2026, time.July, 28, 20, 30, 0, 0, time.Local).Unix(),
	}.String()

	for _, want := range []string{
		"飞行模式通知",
		"切换方式: 自动",
		"飞行模式已开启",
		"原因: 短信已空闲 1 小时",
		"时间: 2026-07-28 20:30:00",
	} {
		if !strings.Contains(message, want) {
			t.Errorf("NotificationMessage.String() = %q, want it to contain %q", message, want)
		}
	}
}
