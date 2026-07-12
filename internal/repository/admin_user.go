package repository

import (
	"gorm.io/gorm"

	"tp-job/internal/domain"
)

// AdminUserStore 管理员账户仓储。与 AppStore 同构:薄封装 gorm,业务规则在 dservice。
type AdminUserStore struct{ db *gorm.DB }

func (s AdminUserStore) Create(u *domain.AdminUser) error { return s.db.Create(u).Error }

// FindByID 按主键。
func (s AdminUserStore) FindByID(id int64) (*domain.AdminUser, error) {
	var u domain.AdminUser
	if err := s.db.Where("id = ?", id).First(&u).Error; err != nil {
		return nil, err
	}
	return &u, nil
}

// FindByUsername 按用户名(登录标识)查。未命中返回 gorm.ErrRecordNotFound。
func (s AdminUserStore) FindByUsername(username string) (*domain.AdminUser, error) {
	var u domain.AdminUser
	if err := s.db.Where("username = ?", username).First(&u).Error; err != nil {
		return nil, err
	}
	return &u, nil
}

// Count 总数(SeedDefault 判空用)。
func (s AdminUserStore) Count() (int64, error) {
	var n int64
	return n, s.db.Model(&domain.AdminUser{}).Count(&n).Error
}

// Update 更新允许修改的字段(username / password)。
func (s AdminUserStore) Update(id int64, fields map[string]any) error {
	if len(fields) == 0 {
		return nil
	}
	return s.db.Model(&domain.AdminUser{}).Where("id = ?", id).Updates(fields).Error
}

// UsernameExists 判断 username 是否已被占用,排除自身 id(改用户名查重)。
func (s AdminUserStore) UsernameExists(username string, excludeID int64) (bool, error) {
	var n int64
	if err := s.db.Model(&domain.AdminUser{}).
		Where("username = ? AND id <> ?", username, excludeID).
		Count(&n).Error; err != nil {
		return false, err
	}
	return n > 0, nil
}
