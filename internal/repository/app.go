package repository

import (
	"gorm.io/gorm"

	"dida/internal/domain"
)

type AppStore struct{ db *gorm.DB }

func (s AppStore) Create(a *domain.App) error { return s.db.Create(a).Error }

// Get 按主键 id。
func (s AppStore) Get(id int64) (*domain.App, error) {
	var a domain.App
	if err := s.db.Where("id = ?", id).First(&a).Error; err != nil {
		return nil, err
	}
	return &a, nil
}

// GetByName 按 AppName(登录标识)查。
func (s AppStore) GetByName(appName string) (*domain.App, error) {
	var a domain.App
	if err := s.db.Where("app_name = ?", appName).First(&a).Error; err != nil {
		return nil, err
	}
	return &a, nil
}

// Update 更新允许修改的字段(app_name / password / status)。
func (s AppStore) Update(id int64, fields map[string]any) error {
	if len(fields) == 0 {
		return nil
	}
	return s.db.Model(&domain.App{}).Where("id = ?", id).Updates(fields).Error
}

func (s AppStore) Delete(id int64) error {
	return s.db.Where("id = ?", id).Delete(&domain.App{}).Error
}

// List 按 AppName 关键字分页(管理端用)。
func (s AppStore) List(keyword string, page, size int) ([]domain.App, int64, error) {
	var apps []domain.App
	var total int64
	q := s.db.Model(&domain.App{})
	if keyword != "" {
		q = q.Where("app_name LIKE ?", "%"+keyword+"%")
	}
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 20
	}
	if err := q.Order("created_at DESC").Offset((page - 1) * size).Limit(size).Find(&apps).Error; err != nil {
		return nil, 0, err
	}
	return apps, total, nil
}
