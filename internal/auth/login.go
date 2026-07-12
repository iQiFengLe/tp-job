package auth

import (
	"errors"

	"golang.org/x/crypto/bcrypt"

	"tp-job/internal/domain"
)

// ErrLoginFailed 登录失败(用户名/密码不匹配,或应用已禁用)。统一错误避免身份枚举。
var ErrLoginFailed = errors.New("用户名或密码错误")

// AdminUserLookup 管理员账户查找(登录用)。*dservice.AdminUserService 天然满足;用接口解耦,
// auth 不直接依赖 dservice。Lookup 未命中返回 (nil,nil) —— 命中与否由 auth 判定,以保留
// "命中管理员用户名即不回退 app"的防同名 app 凭据语义。
type AdminUserLookup interface {
	Lookup(username string) (*domain.AdminUser, error)
}

// AppVerifier 应用账户校验器。*dservice.AppService 天然满足(Verify 方法)。
// 用接口解耦:auth 不直接依赖 dservice,便于单测注入桩。
type AppVerifier interface {
	Verify(appName, password string) (*domain.App, error)
}

// LoginService 登录服务:先查管理员账户(admin_user 表),否则尝试应用账户;成功经 Store 颁发会话。
type LoginService struct {
	adminUsers AdminUserLookup
	apps       AppVerifier
	store      *Store
}

// NewLoginService 构造登录服务。adminUsers 为空则只能走应用登录;apps 为 nil 则禁用应用登录分支。
func NewLoginService(adminUsers AdminUserLookup, apps AppVerifier, store *Store) *LoginService {
	return &LoginService{adminUsers: adminUsers, apps: apps, store: store}
}

// Login 校验 ident+password 并颁发会话。
//
// 顺序:ident 命中管理员用户名(admin_user 表)→ 仅校验该管理员密码(不回退到应用登录,避免
// 同名应用凭据意外通过);否则走应用账户(apps.Verify,含 bcrypt 比较与禁用判断)。任一阶段失败
// 返回 ErrLoginFailed(统一文案,不区分"用户不存在/密码错/已禁用",防身份枚举)。
//
// Lookup 返回 DB 错误时直接判失败、不回退应用登录——保"命中管理员用户名即不回退 app"的防同名
// 语义:DB 抖动窗口不会被当作"未命中"而落到 app 分支,避免同名 app 凭据意外通过。
func (l *LoginService) Login(ident, password string) (Session, error) {
	if l.adminUsers != nil {
		u, err := l.adminUsers.Lookup(ident)
		if err != nil {
			return Session{}, ErrLoginFailed
		}
		if u != nil {
			if bcrypt.CompareHashAndPassword([]byte(u.Password), []byte(password)) == nil {
				return l.store.Put(Session{Role: RoleAdmin, UserID: u.ID, Username: u.Username})
			}
			return Session{}, ErrLoginFailed
		}
	}
	if l.apps != nil {
		if app, err := l.apps.Verify(ident, password); err == nil {
			return l.store.Put(Session{
				Role: RoleApp, AppID: app.ID, AppName: app.AppName, Username: app.AppName,
			})
		}
	}
	return Session{}, ErrLoginFailed
}
