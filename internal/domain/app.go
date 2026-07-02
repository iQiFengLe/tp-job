// Package domain 定义任务调度服务的统一领域模型。
//
// 这是重构后的核心模型(见 docs/refactor-unified-model.md):App / Job / Instance
// 三张表 + 8 态状态机 + Executor/DispatchBody/SystemMetrics 等抽象。与旧 internal/model
// 包并存,旧代码在重构过渡期继续使用旧 model;本包供新调度器/协议层逐步采用。
package domain

import "time"

// App 接入应用:任务归属的命名空间 + 接入凭证。
//
// ID(int 自增)不可用户指定;AppName 全局唯一,兼作登录标识与显示名。
// Password 为 bcrypt 哈希,仅管理端/程序化登录使用——worker 心跳不校验密码。
type App struct {
	ID        int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	AppName   string    `gorm:"type:varchar(128);uniqueIndex;not null" json:"app_name"`
	Password  string    `gorm:"type:varchar(128)" json:"-"`       // bcrypt 哈希
	Status    int8      `gorm:"default:1" json:"status"`          // 1=启用 0=禁用
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"` // 最后更新时间(GORM 自动维护)
}

func (App) TableName() string { return "app" }
