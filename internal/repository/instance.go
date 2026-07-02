package repository

import (
	"time"

	"gorm.io/gorm"

	"task-schedule/internal/domain"
)

type InstanceStore struct{ db *gorm.DB }

// Create 创建实例(状态/参数等由调用方在 ins 指定)。
func (s InstanceStore) Create(ins *domain.Instance) error {
	return s.db.Create(ins).Error
}

// Get 按主键 id。
func (s InstanceStore) Get(id int64) (*domain.Instance, error) {
	var ins domain.Instance
	if err := s.db.Where("id = ?", id).First(&ins).Error; err != nil {
		return nil, err
	}
	return &ins, nil
}

// MarkDispatched 实例派发成功:置 waiting_receive + 绑定承接 worker + start_time。
//
// 终态守护:若 worker 已在 /run 期间(同步)回报了终态(success/failed),则不覆盖回 waiting_receive。
// worker_address / start_time 仍始终记录(审计),仅 status 受守护。
func (s InstanceStore) MarkDispatched(id int64, workerAddress string) error {
	if err := s.db.Model(&domain.Instance{}).Where("id = ?", id).Updates(map[string]any{
		"worker_address": workerAddress,
		"start_time":     time.Now(),
	}).Error; err != nil {
		return err
	}
	return s.db.Model(&domain.Instance{}).
		Where("id = ? AND status NOT IN ?", id, domain.TerminalStatuses()).
		Update("status", domain.StatusWaitingReceive).Error
}

// InstanceFilter 实例列表过滤。
type InstanceFilter struct {
	AppID  int64
	JobID  int64
	Status string
	RootID int64 // 按归属分组过滤(可选)
	Page   int
	Size   int
}

// List 按过滤条件分页查询(按 created_at DESC)。
func (s InstanceStore) List(f InstanceFilter) ([]domain.Instance, int64, error) {
	var list []domain.Instance
	var total int64
	q := s.db.Model(&domain.Instance{})
	if f.AppID > 0 {
		q = q.Where("app_id = ?", f.AppID)
	}
	if f.JobID > 0 {
		q = q.Where("job_id = ?", f.JobID)
	}
	if f.Status != "" {
		q = q.Where("status = ?", f.Status)
	}
	if f.RootID > 0 {
		q = q.Where("root_instance_id = ?", f.RootID)
	}
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	page, size := f.Page, f.Size
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 20
	}
	if err := q.Order("created_at DESC").Offset((page - 1) * size).Limit(size).Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

// UpdateResult 写入状态/结果;终态写 end_time。
// 终态不可回退守护:仅当前非终态时才更新——worker 乱序/迟到上报不覆盖既有终态。
func (s InstanceStore) UpdateResult(id int64, status, result string) error {
	fields := map[string]any{"status": status}
	if result != "" {
		fields["result"] = result
	}
	if domain.StatusTerminal(status) {
		fields["end_time"] = time.Now()
	}
	return s.db.Model(&domain.Instance{}).
		Where("id = ? AND status NOT IN ?", id, domain.TerminalStatuses()).
		Updates(fields).Error
}

// SetStatus 强制写入状态(管理员纠错,不守护终态,可把终态实例复活或改终态)。
func (s InstanceStore) SetStatus(id int64, status, result string) error {
	fields := map[string]any{"status": status}
	if result != "" {
		fields["result"] = result
	}
	if domain.StatusTerminal(status) {
		fields["end_time"] = time.Now()
	} else {
		fields["end_time"] = nil // 复活:清空 end_time
	}
	return s.db.Model(&domain.Instance{}).Where("id = ?", id).Updates(fields).Error
}

// SetNextRetryTime 设定 DB 驱动重试到点时间。
func (s InstanceStore) SetNextRetryTime(id int64, t time.Time) error {
	return s.db.Model(&domain.Instance{}).Where("id = ?", id).Update("next_retry_time", t).Error
}

// ClearNextRetryTime 原子清重试标记(去重):仅当非空时清,返回是否抢到。
func (s InstanceStore) ClearNextRetryTime(id int64) (bool, error) {
	res := s.db.Model(&domain.Instance{}).Where("id = ? AND next_retry_time IS NOT NULL", id).
		Update("next_retry_time", nil)
	return res.RowsAffected > 0, res.Error
}

// ListRetryDue failed 且 next_retry_time 到期的实例,供 RetryPump 扫描。
func (s InstanceStore) ListRetryDue(now time.Time, limit int) ([]domain.Instance, error) {
	var list []domain.Instance
	if limit <= 0 {
		limit = 500
	}
	err := s.db.Where("status = ? AND next_retry_time IS NOT NULL AND next_retry_time <= ?",
		domain.StatusFailed, now).
		Order("next_retry_time ASC").Limit(limit).Find(&list).Error
	return list, err
}

// ListGeneralizedActive 已派发但未终结(waiting_receive/running),供 reaper 扫描失败转移。
func (s InstanceStore) ListGeneralizedActive() ([]domain.Instance, error) {
	var list []domain.Instance
	err := s.db.Where("status IN ?", []string{domain.StatusWaitingReceive, domain.StatusRunning}).
		Find(&list).Error
	return list, err
}

// ListManualQueued 返回 status=queued 且 trigger_type=manual 的实例,供调度器启动恢复用。
// 手动优先队列是纯内存,重启即丢;queued 实例不被 reaper/RetryPump 捞,需在启动时重新入队,
// 否则会永久滞留(违背 SubmitManual 的持久化承诺)。
func (s InstanceStore) ListManualQueued() ([]domain.Instance, error) {
	var list []domain.Instance
	err := s.db.Where("status = ? AND trigger_type = ?", domain.StatusQueued, "manual").
		Find(&list).Error
	return list, err
}

// MarkStaleActiveAsFailed 启动清理:把重启前未终结的实例标 failed(worker 上下文随进程退出丢失)。
func (s InstanceStore) MarkStaleActiveAsFailed(reason string) (int64, error) {
	res := s.db.Model(&domain.Instance{}).
		Where("status IN ?", []string{domain.StatusWaitingReceive, domain.StatusRunning}).
		Updates(map[string]any{"status": domain.StatusFailed, "end_time": time.Now(), "result": reason})
	return res.RowsAffected, res.Error
}
