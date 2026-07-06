// Package auth 提供管理端会话鉴权:登录会话存储 + 登录服务 + gin 中间件。
//
// 取代旧 X-Admin-Token / Basic Auth:管理员账户(配置注入)+ 应用账户(app 表)经
// LoginService 校验 → 颁发带 TTL 的 session token → SessionAuth 中间件解析 Bearer token
// 还原身份 → RequireAdmin/AppScope 按权限矩阵放行。/worker/* /server/* 不走此层
// (靠 appName + 网络隔离,对齐 PowerJob Server 约束)。
package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"sync"
	"time"
)

// Role 会话角色。
type Role string

const (
	RoleAdmin Role = "admin" // 管理员(配置账户):可操作任意 app,可新增/删除 app
	RoleApp   Role = "app"   // 应用账户(app 表登录):仅可操作所属 app
)

// Session 一次登录会话。
type Session struct {
	Token     string    // 随机 token(作 Authorization: Bearer)
	Role      Role      // admin | app
	UserID    int64     // admin=admin_user.id(改密/改名 handler 定位账户用);app=0
	AppID     int64     // app 角色=所属 app id;admin=0
	AppName   string    // app 角色=app 名;admin=""
	Username  string    // 管理员用户名(app 角色同 AppName,便于统一展示)
	CreatedAt time.Time
	ExpiresAt time.Time
}

// IsAdmin 管理员会话。
func (s Session) IsAdmin() bool { return s.Role == RoleAdmin }

// tokenRandBytes token 随机字节数(32B → base64url 约 43 字符,熵足够抗穷举)。
const tokenRandBytes = 32

// Store 内存会话表:token→Session,带 TTL 与后台清理。进程重启即失效(需重新登录),
// 不影响 worker/任务执行(均落 DB 驱动,与登录态解耦)。
type Store struct {
	ttl  time.Duration
	mu   sync.RWMutex
	sess map[string]Session // token → session
}

// NewStore 创建会话表。ttl<=0 时默认 24h。
func NewStore(ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &Store{ttl: ttl, sess: make(map[string]Session)}
}

// TTL 返回会话有效期(供装配层/登录响应回填 ExpiresAt)。
func (s *Store) TTL() time.Duration { return s.ttl }

// Put 写入会话:生成随机 token、回填时间,返回带 token 的会话。
func (s *Store) Put(sess Session) (Session, error) {
	tok, err := newToken()
	if err != nil {
		return Session{}, err
	}
	now := time.Now()
	sess.Token = tok
	sess.CreatedAt = now
	sess.ExpiresAt = now.Add(s.ttl)
	s.mu.Lock()
	s.sess[tok] = sess
	s.mu.Unlock()
	return sess, nil
}

// Get 查询;过期即删除并视为不存在。
func (s *Store) Get(token string) (Session, bool) {
	s.mu.RLock()
	sess, ok := s.sess[token]
	s.mu.RUnlock()
	if !ok {
		return Session{}, false
	}
	if time.Now().After(sess.ExpiresAt) {
		s.Delete(token)
		return Session{}, false
	}
	return sess, true
}

// Delete 删除会话(不存在无副作用)。登出/失效用。
func (s *Store) Delete(token string) {
	s.mu.Lock()
	delete(s.sess, token)
	s.mu.Unlock()
}

// Run 后台周期清理过期会话;随 ctx 取消退出。过期项即便不被 Get 命中也最终被回收。
func (s *Store) Run(ctx context.Context) {
	const interval = time.Minute
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reapExpired()
		}
	}
}

func (s *Store) reapExpired() {
	now := time.Now()
	s.mu.Lock()
	for tok, sess := range s.sess {
		if now.After(sess.ExpiresAt) {
			delete(s.sess, tok)
		}
	}
	s.mu.Unlock()
}

// newToken 生成 base64url(RawURLEncoding,无填充,URL 安全)随机 token。
func newToken() (string, error) {
	b := make([]byte, tokenRandBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
