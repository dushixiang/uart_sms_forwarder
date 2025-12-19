package repo

import (
	"context"

	"github.com/dushixiang/uart_sms_forwarder/internal/models"
	"github.com/go-orz/orz"
	"gorm.io/gorm"
)

type ScheduledTaskRepo struct {
	orz.Repository[models.ScheduledTask, string]
	db *gorm.DB
}

func NewScheduledTaskRepo(db *gorm.DB) *ScheduledTaskRepo {
	return &ScheduledTaskRepo{
		Repository: orz.NewRepository[models.ScheduledTask, string](db),
		db:         db,
	}
}

// FindAllEnabled 查询所有启用的任务
func (r *ScheduledTaskRepo) FindAllEnabled(ctx context.Context) ([]models.ScheduledTask, error) {
	var tasks []models.ScheduledTask
	err := r.db.WithContext(ctx).Where("enabled = ?", true).Find(&tasks).Error
	return tasks, err
}

// FindAll 查询所有任务
func (r *ScheduledTaskRepo) FindAll(ctx context.Context) ([]models.ScheduledTask, error) {
	var tasks []models.ScheduledTask
	err := r.db.WithContext(ctx).Find(&tasks).Error
	return tasks, err
}

func (r *ScheduledTaskRepo) UpdateLastRunStatusByMsgId(ctx context.Context, msgId string, status models.LastRunStatus) error {
	return r.db.WithContext(ctx).Model(&models.ScheduledTask{}).
		Where("last_msg_id = ?", msgId).
		Update("last_run_status", status).Error
}
