package repository

import (
	"time"

	"gorm.io/gorm"

	"tp-job/internal/domain"
)

// CallbackStore 实例状态变更回调记录的持久化。CallbackPump(dispatch/callback.go)
// 扫 ListDue → POST → MarkSent/MarkRetry/MarkDead。
type CallbackStore struct{ db *gorm.DB }

// Create 插入一条回调记录(同事务内由 InstanceStore.*WithCallback 调用)。
func (s CallbackStore) Create(cb *domain.Callback) error {
	return s.db.Create(cb).Error
}

// ListDue 返回 state=pending 且 next_retry_at<=now 的记录,按 next_retry_at 升序(pump 扫描)。
func (s CallbackStore) ListDue(now time.Time, limit int) ([]domain.Callback, error) {
	if limit <= 0 {
		limit = 500
	}
	var list []domain.Callback
	err := s.db.Where("state = ? AND next_retry_at <= ?", domain.CallbackPending, now).
		Order("next_retry_at ASC").Limit(limit).Find(&list).Error
	return list, err
}

// MarkSent 标记投递成功(2xx)。
func (s CallbackStore) MarkSent(id int64) error {
	return s.db.Model(&domain.Callback{}).Where("id = ?", id).
		Updates(map[string]any{"state": domain.CallbackSent, "next_retry_at": nil, "last_error": ""}).Error
}

// MarkRetry 推进一次重试:attempt、next_retry_at(退避后)、last_error。
func (s CallbackStore) MarkRetry(id int64, attempt int, nextRetry time.Time, lastErr string) error {
	return s.db.Model(&domain.Callback{}).Where("id = ?", id).
		Updates(map[string]any{"attempt": attempt, "next_retry_at": nextRetry, "last_error": lastErr}).Error
}

// MarkDead 达 MaxAttempts 仍未成功,放弃投递。
func (s CallbackStore) MarkDead(id int64, lastErr string) error {
	return s.db.Model(&domain.Callback{}).Where("id = ?", id).
		Updates(map[string]any{"state": domain.CallbackDead, "next_retry_at": nil, "last_error": lastErr}).Error
}

// PurgeOld 删除 sent/dead 且 updated_at 早于 olderThan 的记录(审计保留期)。pending 永不删(未投递保证)。
func (s CallbackStore) PurgeOld(olderThan time.Time) (int64, error) {
	res := s.db.Where("state IN ? AND updated_at < ?",
		[]string{domain.CallbackSent, domain.CallbackDead}, olderThan).
		Delete(&domain.Callback{})
	return res.RowsAffected, res.Error
}
