package handler

import (
	"net/http"

	"github.com/dushixiang/uart_sms_forwarder/internal/models"
	"github.com/dushixiang/uart_sms_forwarder/internal/service"
	"github.com/labstack/echo/v5"
	"go.uber.org/zap"
)

type ScheduledTaskHandler struct {
	logger           *zap.Logger
	schedulerService *service.SchedulerService
}

func NewScheduledTaskHandler(logger *zap.Logger, schedulerService *service.SchedulerService) *ScheduledTaskHandler {
	return &ScheduledTaskHandler{
		logger:           logger,
		schedulerService: schedulerService,
	}
}

// List 获取所有定时任务
func (h *ScheduledTaskHandler) List(c *echo.Context) error {
	ctx := c.Request().Context()

	tasks, err := h.schedulerService.GetAll(ctx)
	if err != nil {
		h.logger.Error("获取定时任务列表失败", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "获取任务列表失败",
		})
	}

	// 如果为空，返回空数组而不是 null
	if tasks == nil {
		tasks = []models.ScheduledTask{}
	}

	return c.JSON(http.StatusOK, tasks)
}

// Get 根据ID获取定时任务
func (h *ScheduledTaskHandler) Get(c *echo.Context) error {
	ctx := c.Request().Context()
	id := c.Param("id")

	task, err := h.schedulerService.GetById(ctx, id)
	if err != nil {
		h.logger.Error("获取定时任务失败", zap.String("id", id), zap.Error(err))
		return c.JSON(http.StatusNotFound, map[string]string{
			"error": "任务不存在",
		})
	}

	return c.JSON(http.StatusOK, task)
}

// Create 创建定时任务
func (h *ScheduledTaskHandler) Create(c *echo.Context) error {
	ctx := c.Request().Context()

	var task models.ScheduledTask
	if err := c.Bind(&task); err != nil {
		h.logger.Error("解析请求失败", zap.Error(err))
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "请求参数错误",
		})
	}

	// 验证必填字段
	if err := h.validateTask(&task); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": err.Error(),
		})
	}

	// 创建任务
	if err := h.schedulerService.Create(ctx, &task); err != nil {
		h.logger.Error("创建定时任务失败", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "创建任务失败",
		})
	}

	h.logger.Info("定时任务创建成功", zap.String("id", task.ID), zap.String("name", task.Name))

	return c.JSON(http.StatusCreated, task)
}

// Update 更新定时任务
func (h *ScheduledTaskHandler) Update(c *echo.Context) error {
	ctx := c.Request().Context()
	id := c.Param("id")

	var task models.ScheduledTask
	if err := c.Bind(&task); err != nil {
		h.logger.Error("解析请求失败", zap.Error(err))
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "请求参数错误",
		})
	}

	// 验证必填字段
	if err := h.validateTask(&task); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": err.Error(),
		})
	}

	// 确保 ID 一致
	task.ID = id

	// 更新任务
	if err := h.schedulerService.Update(ctx, &task); err != nil {
		h.logger.Error("更新定时任务失败", zap.String("id", id), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "更新任务失败",
		})
	}

	h.logger.Info("定时任务更新成功", zap.String("id", id), zap.String("name", task.Name))

	return c.JSON(http.StatusOK, task)
}

// Delete 删除定时任务
func (h *ScheduledTaskHandler) Delete(c *echo.Context) error {
	ctx := c.Request().Context()
	id := c.Param("id")

	if err := h.schedulerService.Delete(ctx, id); err != nil {
		h.logger.Error("删除定时任务失败", zap.String("id", id), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "删除任务失败",
		})
	}

	h.logger.Info("定时任务删除成功", zap.String("id", id))

	return c.JSON(http.StatusOK, map[string]string{
		"message": "任务已删除",
	})
}

// Trigger 立即触发执行定时任务
func (h *ScheduledTaskHandler) Trigger(c *echo.Context) error {
	ctx := c.Request().Context()
	id := c.Param("id")

	if err := h.schedulerService.TriggerTask(ctx, id); err != nil {
		h.logger.Error("触发定时任务失败", zap.String("id", id), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "触发任务失败",
		})
	}

	h.logger.Info("定时任务已触发执行", zap.String("id", id))

	return c.JSON(http.StatusOK, map[string]string{
		"message": "任务已触发执行",
	})
}

// validateTask 验证任务字段
func (h *ScheduledTaskHandler) validateTask(task *models.ScheduledTask) error {
	if task.Name == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "任务名称不能为空")
	}
	if task.IntervalDays <= 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "执行间隔天数必须大于0")
	}
	if task.PhoneNumber == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "目标手机号不能为空")
	}
	if task.Content == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "短信内容不能为空")
	}
	return nil
}
