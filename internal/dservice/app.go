// Package dservice 是 domain model 的应用服务层:封装业务规则(bcrypt、调度推算、终态守护等),
// 供 protocol handler 调用。handler 只做 HTTP↔dto 绑定,业务在此。
package dservice

import (
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"task-schedule/internal/domain"
	"task-schedule/internal/repository"
)

var (
	ErrAppNotFound     = errors.New("app 不存在")
	ErrAppUnauthorized = errors.New("app 认证失败")
	ErrAppValidate     = errors.New("app 参数校验失败")
	ErrAppInUse        = errors.New("app 下仍存在 job,无法删除")
	ErrJobNotFound     = errors.New("job 不存在")
	ErrJobValidate     = errors.New("job 参数校验失败")
)

const bcryptMaxPasswordLength = 72

// AppService app 业务。
type AppService struct {
	st *repository.Store
}

func NewAppService(st *repository.Store) *AppService { return &AppService{st: st} }

func validatePassword(pwd string) error {
	if pwd == "" {
		return errors.New("password 不能为空")
	}
	if len(pwd) > bcryptMaxPasswordLength {
		return fmt.Errorf("password 长度不能超过 %d 字节(bcrypt 限制)", bcryptMaxPasswordLength)
	}
	return nil
}

// Create 创建应用。ID 自增(不可指定);密码 bcrypt 存储。
func (s *AppService) Create(appName, password string, status int8) (*domain.App, error) {
	appName = strings.TrimSpace(appName)
	if appName == "" {
		return nil, fmt.Errorf("%w: app_name 不能为空", ErrAppValidate)
	}
	if err := validatePassword(password); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAppValidate, err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}
	if status == 0 {
		status = 1
	}
	app := &domain.App{AppName: appName, Password: string(hash), Status: status}
	if err := s.st.App.Create(app); err != nil {
		return nil, err
	}
	return app, nil
}

// Update 更新可变字段。
func (s *AppService) Update(id int64, appName *string, password *string, status *int8) error {
	fields := map[string]any{}
	if appName != nil {
		n := strings.TrimSpace(*appName)
		if n == "" {
			return fmt.Errorf("%w: app_name 不能为空", ErrAppValidate)
		}
		fields["app_name"] = n
	}
	if password != nil && *password != "" {
		if err := validatePassword(*password); err != nil {
			return fmt.Errorf("%w: %v", ErrAppValidate, err)
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(*password), bcrypt.DefaultCost)
		if err != nil {
			return err
		}
		fields["password"] = string(hash)
	}
	if status != nil {
		fields["status"] = *status
	}
	if len(fields) == 0 {
		return nil
	}
	return s.st.App.Update(id, fields)
}

// Delete 删除;名下有 job 则拒绝。
func (s *AppService) Delete(id int64) error {
	var n int64
	if err := s.st.DB.Model(&domain.Job{}).Where("app_id = ?", id).Count(&n).Error; err != nil {
		return err
	}
	if n > 0 {
		return ErrAppInUse
	}
	return s.st.App.Delete(id)
}

func (s *AppService) Get(id int64) (*domain.App, error) {
	a, err := s.st.App.Get(id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrAppNotFound
		}
		return nil, err
	}
	return a, nil
}

func (s *AppService) GetByName(appName string) (*domain.App, error) {
	a, err := s.st.App.GetByName(appName)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrAppNotFound
		}
		return nil, err
	}
	return a, nil
}

func (s *AppService) List(keyword string, page, size int) ([]domain.App, int64, error) {
	return s.st.App.List(keyword, page, size)
}

// Verify 校验 AppName + 密码(管理端应用登录用)。
func (s *AppService) Verify(appName, password string) (*domain.App, error) {
	app, err := s.st.App.GetByName(appName)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrAppUnauthorized
		}
		return nil, err
	}
	if app.Status != 1 {
		return nil, errors.New("app 已禁用")
	}
	if bcrypt.CompareHashAndPassword([]byte(app.Password), []byte(password)) != nil {
		return nil, ErrAppUnauthorized
	}
	return app, nil
}

// VerifyOldPassword 校验 app 旧密码(app 角色改密码前验明正身,对齐管理员 changePassword 的旧码校验)。
// 旧码为空或校验失败返回 ErrAppUnauthorized;app 不存在返回 ErrAppNotFound。管理员改任意 app 不调此方法。
func (s *AppService) VerifyOldPassword(id int64, old string) error {
	app, err := s.st.App.Get(id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrAppNotFound
		}
		return err
	}
	if old == "" || bcrypt.CompareHashAndPassword([]byte(app.Password), []byte(old)) != nil {
		return ErrAppUnauthorized
	}
	return nil
}
