package own

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"dida/internal/auth"
	"dida/internal/dservice"
)

// ===== 鉴权 DTO / 端点 =====
//
// 登录会话取代旧 X-Admin-Token / Basic Auth。ident 先匹配管理员用户名(配置注入),
// 否则匹配 app 名(app 表);成功颁发 session token,前端存为 Bearer。

// LoginReq 登录请求。ident = 管理员用户名 或 app 名。
type LoginReq struct {
	Ident    string `json:"ident" binding:"required"`
	Password string `json:"password" binding:"required"`
}

// LoginResp 登录成功响应。app_id/app_name 仅 app 角色返回。
type LoginResp struct {
	Token     string    `json:"token"`
	Role      string    `json:"role"` // admin | app
	Username  string    `json:"username"`
	AppID     int64     `json:"app_id,omitempty"`
	AppName   string    `json:"app_name,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
}

// MeResp 当前会话身份(前端用于按 role 显隐 UI)。
type MeResp struct {
	Role     string `json:"role"`
	Username string `json:"username"`
	AppID    int64  `json:"app_id,omitempty"`
	AppName  string `json:"app_name,omitempty"`
}

// LoginHandler 公开登录端点(POST /api/auth/login)。不在 SessionAuth 保护组内——
// 调用方直接挂到 /api 根(与受保护资源路由分离)。
func LoginHandler(login *auth.LoginService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req LoginReq
		if err := c.ShouldBindJSON(&req); err != nil {
			fail(c, http.StatusBadRequest, "参数错误: "+err.Error())
			return
		}
		sess, err := login.Login(req.Ident, req.Password)
		if err != nil {
			fail(c, http.StatusUnauthorized, err.Error())
			return
		}
		ok(c, LoginResp{
			Token: sess.Token, Role: string(sess.Role), Username: sess.Username,
			AppID: sess.AppID, AppName: sess.AppName, ExpiresAt: sess.ExpiresAt,
		})
	}
}

// AutoLoginHandler 公开端点(POST /api/auth/auto-login)。仅本地调试便利:enabled=true 时用默认
// 管理员账户(dservice 导出的 seed 凭据)登录,前端无 token 即可进控制台,免手输。
//
// 安全边界:enabled=false 直接 401(在 bcrypt 之前,零开销,生产 release 模式如此);enabled=true
// 时走 LoginService 真实校验——默认账户密码若已被 Web 改掉,本端点自然 401(登录页兜底),不因
// 开关开启而绕过。⚠ enabled 仅由 config 的 debug.auto_login 决定,生产必须 false,否则任何人
// 可匿名登入。端点复用登录限流(按 ClientIP),防 debug 误暴露时被刷。
func AutoLoginHandler(login *auth.LoginService, enabled bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !enabled {
			fail(c, http.StatusUnauthorized, "自动登录未启用")
			return
		}
		sess, err := login.Login(dservice.DefaultAdminUsername, dservice.DefaultAdminPassword)
		if err != nil {
			fail(c, http.StatusUnauthorized, err.Error())
			return
		}
		ok(c, LoginResp{
			Token: sess.Token, Role: string(sess.Role), Username: sess.Username,
			AppID: sess.AppID, AppName: sess.AppName, ExpiresAt: sess.ExpiresAt,
		})
	}
}

// RegisterAuth 挂载 /auth/me、/auth/logout。两条路由各自前置 SessionAuth(与公开的 login
// 可共用同一 /api group,互不干扰)。logout 幂等。
//
// me 对 admin 角色实时查 AdminUsers 取最新用户名(改名后刷新页面即正确,不依赖登录时的 session
// 快照);AdminUsers 为 nil(未装配)时回退 session 快照。
func RegisterAuth(r *gin.RouterGroup, d Deps) {
	r.GET("/auth/me", auth.SessionAuth(d.Auth), meHandler(d))
	r.POST("/auth/logout", auth.SessionAuth(d.Auth), logoutHandler(d.Auth))
}

func meHandler(d Deps) gin.HandlerFunc {
	return func(c *gin.Context) {
		sess, _ := auth.SessionFrom(c)
		resp := MeResp{
			Role: string(sess.Role), Username: sess.Username,
			AppID: sess.AppID, AppName: sess.AppName,
		}
		// admin 角色取库内最新用户名;查询失败或未装配时回退 session 快照(不影响响应)。
		if sess.Role == auth.RoleAdmin && d.AdminUsers != nil {
			if u, err := d.AdminUsers.Profile(sess.UserID); err == nil {
				resp.Username = u.Username
			}
		}
		ok(c, resp)
	}
}

func logoutHandler(store *auth.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		sess, _ := auth.SessionFrom(c)
		store.Delete(sess.Token)
		ok(c, gin.H{"logged_out": true})
	}
}

// ===== 登录限流 =====
//
// 登录端点公开可达,且每次校验触发 bcrypt(耗时)。无闸门时既可被密码爆破,也可被当作
// 资源放大型 DoS(海量登录请求持续占 CPU)。此处用每 IP 固定窗口限流,超限返 429。
// max<=0 时关闭(向后兼容;生产建议 10~20)。固定窗口对本场景够用:误伤面小、实现无依赖。

// loginRateLimiter 每 IP 固定窗口计数器。window 内最多 max 次尝试。
type loginRateLimiter struct {
	window time.Duration
	max    int
	mu     sync.Mutex
	hits   map[string]*loginWin
}

type loginWin struct {
	n     int
	start time.Time
}

// newLoginRateLimiter max<=0 返回 nil(表示不限流,nil.allow 恒放行)。否则启动后台回收。
func newLoginRateLimiter(maxPerMin int) *loginRateLimiter {
	if maxPerMin <= 0 {
		return nil
	}
	l := &loginRateLimiter{window: time.Minute, max: maxPerMin, hits: map[string]*loginWin{}}
	go l.reapLoop()
	return l
}

// allow nil 接收者恒放行(未启用限流);否则按 IP 固定窗口计数,超限拒绝。
func (l *loginRateLimiter) allow(ip string) bool {
	if l == nil {
		return true
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	w, ok := l.hits[ip]
	if !ok || now.Sub(w.start) >= l.window {
		l.hits[ip] = &loginWin{n: 1, start: now}
		return true
	}
	w.n++
	return w.n <= l.max
}

// reapLoop 周期回收过期窗口条目,避免长期 churn 下 hits map 无限增长。随进程退出终止。
func (l *loginRateLimiter) reapLoop() {
	t := time.NewTicker(l.window)
	defer t.Stop()
	for range t.C {
		cutoff := time.Now().Add(-l.window)
		l.mu.Lock()
		for ip, w := range l.hits {
			if w.start.Before(cutoff) {
				delete(l.hits, ip)
			}
		}
		l.mu.Unlock()
	}
}

// LoginRateLimit 登录端点 IP 限流中间件。maxPerMin<=0 时 no-op(不限流,直接放行)。
func LoginRateLimit(maxPerMin int) gin.HandlerFunc {
	rl := newLoginRateLimiter(maxPerMin)
	return func(c *gin.Context) {
		if !rl.allow(c.ClientIP()) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests,
				gin.H{"code": http.StatusTooManyRequests, "msg": "登录尝试过于频繁,请稍后再试"})
			return
		}
		c.Next()
	}
}
