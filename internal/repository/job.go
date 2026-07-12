package repository

import (
	"time"

	"gorm.io/gorm"

	"tp-job/internal/domain"
)

type JobStore struct{ db *gorm.DB }

func (s JobStore) Create(j *domain.Job) error { return s.db.Create(j).Error }

// Get 按主键 id;appID 用于越权防护(只能查本 app 的 job)。
func (s JobStore) Get(appID, id int64) (*domain.Job, error) {
	var j domain.Job
	if err := s.db.Where("id = ? AND app_id = ?", id, appID).First(&j).Error; err != nil {
		return nil, err
	}
	return &j, nil
}

func (s JobStore) Update(id int64, fields map[string]any) error {
	if len(fields) == 0 {
		return nil
	}
	return s.db.Model(&domain.Job{}).Where("id = ?", id).Updates(fields).Error
}

func (s JobStore) Delete(appID, id int64) error {
	return s.db.Where("id = ? AND app_id = ?", id, appID).Delete(&domain.Job{}).Error
}

// List 按 app 分页。
func (s JobStore) List(appID int64, page, size int) ([]domain.Job, int64, error) {
	var jobs []domain.Job
	var total int64
	q := s.db.Model(&domain.Job{}).Where("app_id = ?", appID)
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 20
	}
	if err := q.Order("created_at DESC").Offset((page - 1) * size).Limit(size).Find(&jobs).Error; err != nil {
		return nil, 0, err
	}
	return jobs, total, nil
}

// ListDue 返回已启用且 next_run_time 到期(<= now)的 job,供调度器扫描。
func (s JobStore) ListDue(now time.Time, limit int) ([]domain.Job, error) {
	var jobs []domain.Job
	if limit <= 0 {
		limit = 500
	}
	err := s.db.Where("enabled = ? AND next_run_time IS NOT NULL AND next_run_time <= ?", true, now).
		Order("next_run_time ASC").Limit(limit).Find(&jobs).Error
	return jobs, err
}

// AdvanceNextRun 原子推进 next_run_time(调度器"认领",防并发重复触发):
// 仅当当前 next_run_time 仍是 oldNext 时更新为 newNext(nil=置空,停止自动调度)。
func (s JobStore) AdvanceNextRun(jobID int64, oldNext time.Time, newNext *time.Time) (bool, error) {
	q := s.db.Model(&domain.Job{}).Where("id = ? AND next_run_time = ?", jobID, oldNext)
	if newNext == nil {
		res := q.Update("next_run_time", nil)
		return res.RowsAffected > 0, res.Error
	}
	res := q.Update("next_run_time", *newNext)
	return res.RowsAffected > 0, res.Error
}

// ListByIDs 批量按 id 查 job(reaper/retry 预加载用,消除逐实例 Get 的 N+1)。
func (s JobStore) ListByIDs(ids []int64) ([]domain.Job, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var jobs []domain.Job
	err := s.db.Where("id IN ?", ids).Find(&jobs).Error
	return jobs, err
}

// GetByFrom 按 (app_id, from_id, from_type) 查指定 app 内的来源 job(导入 upsert 判重用)。
// 每个 app 独立判重:同源 job 可分存不同 app。返回 gorm.ErrRecordNotFound 表示该 app 尚未导入此来源。
func (s JobStore) GetByFrom(appID int64, fromID, fromType string) (*domain.Job, error) {
	var j domain.Job
	if err := s.db.Where("app_id = ? AND from_id = ? AND from_type = ?", appID, fromID, fromType).First(&j).Error; err != nil {
		return nil, err
	}
	return &j, nil
}

// ListByFrom 批量按 (app_id, from_type, from_id IN ...) 查现有来源 job(导入判重,消除逐条 GetByFrom 的 N+1)。
// 返回 fromID → *Job map;调用方按 job.FromID 查是否冲突。空 fromIDs 直接返回空 map。
func (s JobStore) ListByFrom(appID int64, fromType string, fromIDs []string) (map[string]*domain.Job, error) {
	out := make(map[string]*domain.Job, len(fromIDs))
	if len(fromIDs) == 0 {
		return out, nil
	}
	var jobs []domain.Job
	if err := s.db.Where("app_id = ? AND from_type = ? AND from_id IN ?", appID, fromType, fromIDs).Find(&jobs).Error; err != nil {
		return nil, err
	}
	for i := range jobs {
		out[jobs[i].FromID] = &jobs[i]
	}
	return out, nil
}
