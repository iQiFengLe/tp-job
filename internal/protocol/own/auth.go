package own

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"task-schedule/internal/auth"
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

// RegisterAuth 挂载 /auth/me、/auth/logout。两条路由各自前置 SessionAuth(与公开的 login
// 可共用同一 /api group,互不干扰)。logout 幂等。
func RegisterAuth(r *gin.RouterGroup, store *auth.Store) {
	r.GET("/auth/me", auth.SessionAuth(store), meHandler)
	r.POST("/auth/logout", auth.SessionAuth(store), logoutHandler(store))
}

func meHandler(c *gin.Context) {
	sess, _ := auth.SessionFrom(c)
	ok(c, MeResp{
		Role: string(sess.Role), Username: sess.Username,
		AppID: sess.AppID, AppName: sess.AppName,
	})
}

func logoutHandler(store *auth.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		sess, _ := auth.SessionFrom(c)
		store.Delete(sess.Token)
		ok(c, gin.H{"logged_out": true})
	}
}
