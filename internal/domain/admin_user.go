package domain

import "time"

// AdminUser 管理员账户(入库)。取代旧 config.yaml / env 注入机制:首次启动表空时由
// dservice.AdminUserService.SeedDefault 种入默认 admin/admin123;之后改密/改名只走 Web。
//
// 与 App(应用账户)分离:管理员可操作任意 app 并管理 app 增删;应用账户仅限自家 app。
// 登录时 LoginService 先查本表(命中用户名即不回退 app,防同名 app 凭据通过),否则回退 App。
type AdminUser struct {
	ID        int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	Username  string    `gorm:"type:varchar(128);uniqueIndex;not null" json:"username"`
	Password  string    `gorm:"type:varchar(128);not null" json:"-"` // bcrypt 哈希,永不外泄
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

// TableName 显式指定表名(全小写,与 app/job 一致)。
func (AdminUser) TableName() string { return "admin_user" }
