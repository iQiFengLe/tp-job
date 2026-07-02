package auth

import (
	"errors"

	"golang.org/x/crypto/bcrypt"

	"task-schedule/internal/domain"
)

// ErrLoginFailed 登录失败(用户名/密码不匹配,或应用已禁用)。统一错误避免身份枚举。
var ErrLoginFailed = errors.New("用户名或密码错误")

// AdminCredential 管理员凭据(用户名 + bcrypt 哈希)。由装配层从 config 映射而来,
// 解耦 auth 与 config 包;不入库(配置注入)。
type AdminCredential struct {
	Username     string
	PasswordHash string // bcrypt 哈希
}

// AppVerifier 应用账户校验器。*dservice.AppService 天然满足(Verify 方法)。
// 用接口解耦:auth 不直接依赖 dservice,便于单测注入桩。
type AppVerifier interface {
	Verify(appName, password string) (*domain.App, error)
}

// LoginService 登录服务:先匹配管理员账户,否则尝试应用账户;成功则经 Store 颁发会话。
type LoginService struct {
	admins []AdminCredential
	apps   AppVerifier
	store  *Store
}

// NewLoginService 构造登录服务。admins 为空则只能走应用登录。apps 为 nil 则禁用应用登录分支。
func NewLoginService(admins []AdminCredential, apps AppVerifier, store *Store) *LoginService {
	cp := make([]AdminCredential, len(admins))
	copy(cp, admins)
	return &LoginService{admins: cp, apps: apps, store: store}
}

// Login 校验 ident+password 并颁发会话。
//
// 顺序:若 ident 命中某管理员用户名 → 仅校验该管理员密码(不回退到应用登录,避免同名的应用
// 凭据意外通过);否则走应用账户(apps.Verify,含 bcrypt 比较与禁用判断)。任一阶段失败返回
// ErrLoginFailed(统一文案,不区分"用户不存在/密码错/已禁用",防身份枚举)。
func (l *LoginService) Login(ident, password string) (Session, error) {
	for _, a := range l.admins {
		if a.Username == ident {
			if bcrypt.CompareHashAndPassword([]byte(a.PasswordHash), []byte(password)) == nil {
				return l.store.Put(Session{Role: RoleAdmin, Username: ident})
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
