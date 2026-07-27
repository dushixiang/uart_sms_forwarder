package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dushixiang/uart_sms_forwarder/internal/models"
	"github.com/dushixiang/uart_sms_forwarder/internal/repo"
	"github.com/go-orz/cache"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	// PropertyIDNotificationChannels 通知渠道配置的固定 ID
	PropertyIDNotificationChannels = "notification_channels"
	// PropertyIDAutoFlymodeConfig 自动飞行模式配置的固定 ID
	PropertyIDAutoFlymodeConfig = "auto_flymode_config"

	DefaultAutoFlymodeIdleTimeoutHours int64 = 1
	MinAutoFlymodeIdleTimeoutHours     int64 = 1
	MaxAutoFlymodeIdleTimeoutHours     int64 = 30 * 24
)

type PropertyService struct {
	repo   *repo.PropertyRepo
	logger *zap.Logger
	// 内存缓存，使用 go-orz/cache，永不过期
	cache cache.Cache[string, *models.Property]
}

func NewPropertyService(logger *zap.Logger, db *gorm.DB) *PropertyService {
	return &PropertyService{
		repo:   repo.NewPropertyRepo(db),
		logger: logger,
		cache:  cache.New[string, *models.Property](time.Minute), // 0 表示永不过期
	}
}

// Get 获取属性（返回原始 JSON 字符串）
func (s *PropertyService) Get(ctx context.Context, id string) (*models.Property, error) {
	// 先尝试从缓存读取
	if property, ok := s.cache.Get(id); ok {
		return property, nil
	}

	// 缓存未命中，从数据库读取
	property, err := s.repo.FindById(ctx, id)
	if err != nil {
		return nil, err
	}

	// 更新缓存
	s.cache.Set(id, &property, time.Hour)

	return &property, nil
}

// GetValue 获取属性值并反序列化
func (s *PropertyService) GetValue(ctx context.Context, id string, target interface{}) error {
	// 使用 Get 方法，内部已经支持缓存
	property, err := s.Get(ctx, id)
	if err != nil {
		return err
	}

	if property.Value == "" {
		return nil
	}

	return json.Unmarshal([]byte(property.Value), target)
}

// Set 设置属性（接收对象，自动序列化）
func (s *PropertyService) Set(ctx context.Context, id string, name string, value interface{}) error {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return err
	}

	property := &models.Property{
		ID:        id,
		Name:      name,
		Value:     string(jsonValue),
		CreatedAt: time.Now().UnixMilli(),
		UpdatedAt: time.Now().UnixMilli(),
	}

	err = s.repo.Save(ctx, property)
	if err != nil {
		return err
	}

	// 清空缓存中的该项，下次读取时会重新从数据库加载
	s.cache.Delete(id)

	return nil
}

func (s *PropertyService) GetNotificationChannelConfigs(ctx context.Context) ([]models.NotificationChannelConfig, error) {
	var allChannels []models.NotificationChannelConfig
	err := s.GetValue(ctx, PropertyIDNotificationChannels, &allChannels)
	if err != nil {
		return nil, fmt.Errorf("获取通知渠道配置失败: %w", err)
	}
	return allChannels, nil
}

func DefaultAutoFlymodeConfig() models.AutoFlymodeConfig {
	return models.AutoFlymodeConfig{
		Enabled:          false,
		IdleTimeoutHours: DefaultAutoFlymodeIdleTimeoutHours,
	}
}

// ValidateAutoFlymodeConfig 校验自动飞行模式配置。
func ValidateAutoFlymodeConfig(config models.AutoFlymodeConfig) error {
	if config.IdleTimeoutHours < MinAutoFlymodeIdleTimeoutHours ||
		config.IdleTimeoutHours > MaxAutoFlymodeIdleTimeoutHours {
		return fmt.Errorf("空闲时间必须在 %d 到 %d 小时之间",
			MinAutoFlymodeIdleTimeoutHours,
			MaxAutoFlymodeIdleTimeoutHours,
		)
	}
	return nil
}

// GetAutoFlymodeConfig 获取并校验自动飞行模式配置。
func (s *PropertyService) GetAutoFlymodeConfig(ctx context.Context) (models.AutoFlymodeConfig, error) {
	config := DefaultAutoFlymodeConfig()
	if err := s.GetValue(ctx, PropertyIDAutoFlymodeConfig, &config); err != nil {
		return models.AutoFlymodeConfig{}, fmt.Errorf("获取自动飞行模式配置失败: %w", err)
	}
	if err := ValidateAutoFlymodeConfig(config); err != nil {
		return models.AutoFlymodeConfig{}, fmt.Errorf("自动飞行模式配置无效: %w", err)
	}
	return config, nil
}

// SetAutoFlymodeConfig 校验并保存自动飞行模式配置。
func (s *PropertyService) SetAutoFlymodeConfig(ctx context.Context, config models.AutoFlymodeConfig) error {
	if err := ValidateAutoFlymodeConfig(config); err != nil {
		return err
	}
	return s.Set(ctx, PropertyIDAutoFlymodeConfig, "自动飞行模式配置", config)
}

// defaultPropertyConfig 默认配置项定义
type defaultPropertyConfig struct {
	ID    string
	Name  string
	Value interface{}
}

// InitializeDefaultConfigs 初始化默认配置（如果数据库中不存在）
func (s *PropertyService) InitializeDefaultConfigs(ctx context.Context) error {
	// 定义所有需要初始化的默认配置
	defaultConfigs := []defaultPropertyConfig{
		{
			ID:    PropertyIDNotificationChannels,
			Name:  "通知渠道配置",
			Value: []models.NotificationChannelConfig{},
		},
		{
			ID:    PropertyIDAutoFlymodeConfig,
			Name:  "自动飞行模式配置",
			Value: DefaultAutoFlymodeConfig(),
		},
	}

	// 遍历并初始化每个配置
	for _, config := range defaultConfigs {
		if err := s.initializeProperty(ctx, config); err != nil {
			return fmt.Errorf("初始化 %s 失败: %w", config.Name, err)
		}
	}

	s.logger.Info("默认配置初始化完成")
	return nil
}

// initializeProperty 初始化单个配置项
func (s *PropertyService) initializeProperty(ctx context.Context, config defaultPropertyConfig) error {
	// 检查配置是否已存在
	exists, err := s.repo.ExistsById(ctx, config.ID)
	if err != nil {
		return err
	}

	if exists {
		// 配置已存在，无需初始化
		s.logger.Info("配置已存在，跳过初始化", zap.String("name", config.Name))
		return nil
	}

	// 配置不存在，创建默认配置
	if err := s.Set(ctx, config.ID, config.Name, config.Value); err != nil {
		return err
	}
	s.logger.Info("配置默认值已初始化", zap.String("name", config.Name))
	return nil
}
