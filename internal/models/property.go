package models

// Property 通用属性配置表
type Property struct {
	ID        string `gorm:"primaryKey" json:"id"`                  // 属性ID (如: notification_channels)
	Name      string `json:"name"`                                  // 可读名称
	Value     string `json:"value" gorm:"type:text"`                // JSON配置
	CreatedAt int64  `json:"createdAt"`                             // 创建时间（时间戳毫秒）
	UpdatedAt int64  `json:"updatedAt" gorm:"autoUpdateTime:milli"` // 更新时间（时间戳毫秒）
}

func (Property) TableName() string {
	return "properties"
}

// NotificationChannelConfig 通知渠道配置（存储在 Property 中）
type NotificationChannelConfig struct {
	Type    string                 `json:"type"`    // 类型: dingtalk, wecom, feishu, webhook
	Enabled bool                   `json:"enabled"` // 是否启用
	Config  map[string]interface{} `json:"config"`  // 配置对象
}

// AutoFlymodeConfig 短信空闲后自动进入飞行模式的配置。
type AutoFlymodeConfig struct {
	Enabled          bool  `json:"enabled"`
	IdleTimeoutHours int64 `json:"idleTimeoutHours"`
}

// 配置格式说明：
// dingtalk: { "secretKey": "xxx", "signSecret": "xxx" }
// wecom:    { "secretKey": "xxx" }
// feishu:   { "secretKey": "xxx", "signSecret": "xxx" }
// webhook:  {
//   "url": "https://...",
//   "method": "POST",  // 可选：GET, POST, PUT, PATCH, DELETE，默认 POST
//   "headers": {"key": "value"},  // 可选：自定义请求头
//   "bodyTemplate": "json"  // 可选：json(默认), form, custom
//   "customBody": ""  // 当 bodyTemplate 为 custom 时使用，支持变量替换
// }

// WebhookConfig 自定义 Webhook 配置结构
type WebhookConfig struct {
	URL          string            `json:"url"`                    // Webhook URL
	Method       string            `json:"method,omitempty"`       // 请求方法，默认 POST
	Headers      map[string]string `json:"headers,omitempty"`      // 自定义请求头
	BodyTemplate string            `json:"bodyTemplate,omitempty"` // 请求体模板：json, form, custom
	CustomBody   string            `json:"customBody,omitempty"`   // 自定义请求体模板（支持变量）
}
