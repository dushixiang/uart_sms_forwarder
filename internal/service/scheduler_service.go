package service

import (
	"context"
	"fmt"
	"time"

	"github.com/dushixiang/uart_sms_forwarder/internal/models"
	"github.com/dushixiang/uart_sms_forwarder/internal/repo"
	"github.com/go-orz/orz"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// SchedulerService 定时任务调度服务（包含任务管理功能）
type SchedulerService struct {
	logger        *zap.Logger
	cron          *cron.Cron
	repo          *repo.ScheduledTaskRepo
	serialService *SerialService
}

// NewSchedulerService 创建定时任务服务实例
func NewSchedulerService(
	logger *zap.Logger,
	db *gorm.DB,
	serialService *SerialService,
) *SchedulerService {
	return &SchedulerService{
		logger:        logger,
		repo:          repo.NewScheduledTaskRepo(db),
		serialService: serialService,
	}
}

// ==================== 任务管理方法 ====================

// GetAll 获取所有定时任务
func (s *SchedulerService) GetAll(ctx context.Context) ([]models.ScheduledTask, error) {
	return s.repo.FindAll(ctx)
}

// GetAllEnabled 获取所有启用的定时任务
func (s *SchedulerService) GetAllEnabled(ctx context.Context) ([]models.ScheduledTask, error) {
	return s.repo.FindAllEnabled(ctx)
}

// GetById 根据ID获取定时任务
func (s *SchedulerService) GetById(ctx context.Context, id string) (*models.ScheduledTask, error) {
	task, err := s.repo.FindById(ctx, id)
	if err != nil {
		return nil, err
	}
	return &task, nil
}

// Create 创建定时任务
func (s *SchedulerService) Create(ctx context.Context, task *models.ScheduledTask) error {
	now := time.Now().UnixMilli()
	task.ID = uuid.New().String()
	task.CreatedAt = now
	task.UpdatedAt = now
	return s.repo.Create(ctx, task)
}

// Update 更新定时任务
func (s *SchedulerService) Update(ctx context.Context, task *models.ScheduledTask) error {
	existingTask, err := s.GetById(ctx, task.ID)
	if err != nil {
		return err
	}
	existingTask.Name = task.Name
	existingTask.Enabled = task.Enabled
	existingTask.IntervalDays = task.IntervalDays
	existingTask.PhoneNumber = task.PhoneNumber
	existingTask.Content = task.Content

	return s.repo.Save(ctx, existingTask)
}

// Delete 删除定时任务
func (s *SchedulerService) Delete(ctx context.Context, id string) error {
	return s.repo.DeleteById(ctx, id)
}

// TriggerTask 立即触发执行指定的任务
func (s *SchedulerService) TriggerTask(ctx context.Context, id string) error {
	// 获取任务
	task, err := s.GetById(ctx, id)
	if err != nil {
		return fmt.Errorf("获取任务失败: %w", err)
	}

	// 执行任务
	if err := s.executeTask(*task); err != nil {
		return fmt.Errorf("执行任务失败: %w", err)
	}

	return nil
}

// ==================== 调度相关方法 ====================

// Start 启动定时任务服务
func (s *SchedulerService) Start(ctx context.Context) error {
	s.cron = cron.New()

	// 添加每天执行一次的检查任务（每天早上8点执行）
	_, err := s.cron.AddFunc("0 8 * * *", func() {
		s.logger.Info("开始检查定时任务")
		if err := s.checkAndExecuteTasks(); err != nil {
			s.logger.Error("检查并执行定时任务失败", zap.Error(err))
		}
	})
	if err != nil {
		return fmt.Errorf("添加检查任务失败: %w", err)
	}

	// 启动 cron
	s.cron.Start()

	s.logger.Info("定时任务服务启动成功")
	return nil
}

// checkAndExecuteTasks 检查并执行满足条件的任务
func (s *SchedulerService) checkAndExecuteTasks() error {
	ctx := context.Background()

	// 获取所有启用的任务
	tasks, err := s.GetAllEnabled(ctx)
	if err != nil {
		s.logger.Error("获取启用的定时任务失败", zap.Error(err))
		return err
	}

	now := time.Now()
	for _, task := range tasks {
		// 检查是否需要执行
		if s.shouldExecuteTask(task, now) {
			s.logger.Info("任务满足执行条件",
				zap.String("id", task.ID),
				zap.String("name", task.Name),
				zap.Int("intervalDays", task.IntervalDays))

			if err := s.executeTask(task); err != nil {
				s.logger.Error("执行定时任务失败",
					zap.String("id", task.ID),
					zap.String("name", task.Name),
					zap.Error(err))
			}
		}
	}

	return nil
}

// shouldExecuteTask 判断任务是否应该执行
func (s *SchedulerService) shouldExecuteTask(task models.ScheduledTask, now time.Time) bool {
	// 如果从未执行过，则执行
	if task.LastRunAt <= 0 {
		return true
	}

	// 计算距离上次执行的天数
	lastRun := time.UnixMilli(task.LastRunAt)
	daysSinceLastRun := int(now.Sub(lastRun).Hours() / 24)

	// 如果满足间隔天数条件，则执行
	return daysSinceLastRun >= task.IntervalDays
}

// executeTask 执行任务
func (s *SchedulerService) executeTask(task models.ScheduledTask) error {
	s.logger.Info("执行定时任务",
		zap.String("id", task.ID),
		zap.String("name", task.Name),
		zap.String("phone", task.PhoneNumber),
		zap.String("content", task.Content))

	ctx := context.Background()

	// 发送短信
	msgId, err := s.serialService.SendSMS(task.PhoneNumber, task.Content)
	if err != nil {
		s.logger.Error("定时任务发送短信失败",
			zap.String("id", task.ID),
			zap.String("name", task.Name),
			zap.Error(err))
		_ = s.UpdateLastRun(ctx, task.ID, msgId, models.LastRunStatusFailed)
		return err
	}
	s.logger.Info("定时任务执行成功",
		zap.String("id", task.ID),
		zap.String("name", task.Name))

	// 更新任务的 LastRunAt 字段到数据库

	_ = s.UpdateLastRun(ctx, task.ID, msgId, models.LastRunStatusUnknown)

	return nil
}

func (s *SchedulerService) UpdateLastRun(ctx context.Context, id, msgId string, status models.LastRunStatus) error {
	return s.repo.UpdateColumnsById(ctx, id, orz.Map{
		"last_msg_id":     msgId,
		"last_run_at":     time.Now().UnixMilli(),
		"last_run_status": status,
	})
}
