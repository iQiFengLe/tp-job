// Package repository 是 domain model 的仓储层(App/Job/Instance CRUD)。
//
// 由调用方(main.go / 测试)负责打开 db,经 FromDB 构建——便于 SQLite 单写者模型下
// 统一连接管理。
package repository

import (
	"fmt"

	"gorm.io/gorm"

	"task-schedule/internal/domain"
)

// Store domain 仓储聚合。
type Store struct {
	DB       *gorm.DB
	Driver   string // 数据库驱动类型(sqlite/postgres/mysql),用于 health 接口展示
	App      AppStore
	Job      JobStore
	Instance InstanceStore
	Callback CallbackStore
}

// FromDB 基于已打开的 gorm.DB 构建仓储,并 AutoMigrate domain 表(app/job/job_instance)。
func FromDB(db *gorm.DB) (*Store, error) {
	if err := db.AutoMigrate(&domain.App{}, &domain.Job{}, &domain.Instance{}, &domain.Callback{}); err != nil {
		return nil, fmt.Errorf("domain auto migrate: %w", err)
	}
	s := &Store{DB: db}
	s.App = AppStore{db: db}
	s.Job = JobStore{db: db}
	s.Instance = InstanceStore{db: db}
	s.Callback = CallbackStore{db: db}
	return s, nil
}
