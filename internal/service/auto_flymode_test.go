package service

import (
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
