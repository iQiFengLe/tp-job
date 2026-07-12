package dservice

import (
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"dida/internal/domain"
	"dida/internal/repository"
)

// 管理员账户相关 sentinel。handler 层(badStatus/notFoundStatus)按这些映射 HTTP 码。
var (
	ErrAdminUserNotFound  = errors.New("管理员账户不存在")
	ErrAdminUserValidate  = errors.New("管理员账户参数校验失败")
	ErrAdminUserDuplicate = errors.New("管理员用户名已存在")
	ErrAdminPasswordWrong = errors.New("原密码错误")
)

// 默认 seed 账户(首次启动表空时种入)。取代旧 config 占位 admin/change-me-admin。
// 导出供 auto-login 端点复用(开发便利:debug.auto_login 开启时用这组凭据匿名登入)。
const (
	DefaultAdminUsername = "admin"
	DefaultAdminPassword = "admin123"

	adminUsernameMinLen = 3
	adminUsernameMaxLen = 64
)

// AdminUserService 管理员账户业务。取代旧 config.yaml/env 注入:首次启动 SeedDefault 种
// admin/admin123,之后改密/改名走 Web。登录经 Lookup 供 auth.LoginService 调用
// (命中用户名即不回退 app,防同名 app 凭据通过)。
type AdminUserService struct {
	st *repository.Store
}

func NewAdminUserService(st *repository.Store) *AdminUserService {
	return &AdminUserService{st: st}
}

// SeedDefault 表空时种默认账户 admin/admin123。幂等:已有任意账户则不动(尊重后续 Web 改动,
// 不覆盖)。固定常量、无参数 —— 已彻底废弃 env/配置注入。
func (s *AdminUserService) SeedDefault() error {
	n, err := s.st.AdminUser.Count()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	return s.create(DefaultAdminUsername, DefaultAdminPassword)
}

// create 校验 + bcrypt 哈希 + 入库。仅 SeedDefault 内部用(不接受外部任意创建,账户管理范围
// 本次限定为"改当前账户")。
func (s *AdminUserService) create(username, password string) error {
	username = strings.TrimSpace(username)
	if err := validateUsername(username); err != nil {
		return fmt.Errorf("%w: %v", ErrAdminUserValidate, err)
	}
	if err := validatePassword(password); err != nil { // 复用 app.go 的 validatePassword(bcrypt ≤72)
		return fmt.Errorf("%w: %v", ErrAdminUserValidate, err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	return s.st.AdminUser.Create(&domain.AdminUser{Username: username, Password: string(hash)})
}

// Lookup 登录用:按用户名查;找到 (u,nil)、未找到 (nil,nil)、DB 错 (nil,err)。
// 让 auth 层用 u!=nil 判命中——命中即只校验管理员密码、不回退 app(防同名 app 凭据通过)。
func (s *AdminUserService) Lookup(username string) (*domain.AdminUser, error) {
	u, err := s.st.AdminUser.FindByUsername(username)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return u, nil
}

// Profile 取当前账户(Password 字段 json:"-",view 层不外泄)。
func (s *AdminUserService) Profile(id int64) (*domain.AdminUser, error) {
	u, err := s.st.AdminUser.FindByID(id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrAdminUserNotFound
		}
		return nil, err
	}
	return u, nil
}

// ChangeUsername 改用户名;查重排除自身 id,避免与其它管理员重名(unique 约束兜底)。
func (s *AdminUserService) ChangeUsername(id int64, newUsername string) error {
	newUsername = strings.TrimSpace(newUsername)
	if err := validateUsername(newUsername); err != nil {
		return fmt.Errorf("%w: %v", ErrAdminUserValidate, err)
	}
	exists, err := s.st.AdminUser.UsernameExists(newUsername, id)
	if err != nil {
		return err
	}
	if exists {
		return ErrAdminUserDuplicate
	}
	return s.st.AdminUser.Update(id, map[string]any{"username": newUsername})
}

// ChangePassword 改密码:先 bcrypt 比对旧密(错则 ErrAdminPasswordWrong),再写新密哈希。
func (s *AdminUserService) ChangePassword(id int64, oldPassword, newPassword string) error {
	u, err := s.st.AdminUser.FindByID(id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrAdminUserNotFound
		}
		return err
	}
	if bcrypt.CompareHashAndPassword([]byte(u.Password), []byte(oldPassword)) != nil {
		return ErrAdminPasswordWrong
	}
	if err := validatePassword(newPassword); err != nil {
		return fmt.Errorf("%w: %v", ErrAdminUserValidate, err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	return s.st.AdminUser.Update(id, map[string]any{"password": string(hash)})
}

// validateUsername 用户名长度约束(与密码长度解耦:密码由 validatePassword 管)。
func validateUsername(name string) error {
	if len(name) < adminUsernameMinLen {
		return fmt.Errorf("用户名长度不能小于 %d", adminUsernameMinLen)
	}
	if len(name) > adminUsernameMaxLen {
		return fmt.Errorf("用户名长度不能超过 %d", adminUsernameMaxLen)
	}
	return nil
}
