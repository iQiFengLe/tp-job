package auth

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

const ctxSessionKey = "auth.session"

// SessionAuth 解析 Authorization: Bearer <token>,还原会话写入 context;失败→401。
// 受保护路由组挂此中间件后,下游用 SessionFrom 取身份。
func SessionAuth(store *Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := bearerToken(c)
		if token == "" {
			abortJSON(c, http.StatusUnauthorized, "缺少认证信息")
			return
		}
		sess, ok := store.Get(token)
		if !ok {
			abortJSON(c, http.StatusUnauthorized, "认证已失效,请重新登录")
			return
		}
		c.Set(ctxSessionKey, sess)
		c.Next()
	}
}

// bearerToken 提取 "Bearer <token>"(大小写不敏感);缺失返回空。
func bearerToken(c *gin.Context) string {
	h := c.GetHeader("Authorization")
	const p = "Bearer "
	if len(h) > len(p) && strings.EqualFold(h[:len(p)], p) {
		return strings.TrimSpace(h[len(p):])
	}
	return ""
}

// SessionFrom 取 SessionAuth 写入的会话(无则 ok=false)。
func SessionFrom(c *gin.Context) (Session, bool) {
	v, ok := c.Get(ctxSessionKey)
	if !ok {
		return Session{}, false
	}
	sess, _ := v.(Session)
	return sess, sess.Token != ""
}

// RequireAdmin 仅管理员放行;非管理员→403。须在 SessionAuth 之后挂。
// 用于新增/删除 app、列出全部 app 等仅管理员可用的操作。
func RequireAdmin() gin.HandlerFunc {
	return func(c *gin.Context) {
		sess, ok := SessionFrom(c)
		if !ok {
			abortJSON(c, http.StatusUnauthorized, "缺少认证信息")
			return
		}
		if !sess.IsAdmin() {
			abortJSON(c, http.StatusForbidden, "需要管理员权限")
			return
		}
		c.Next()
	}
}

// AppScope 限制 app 资源访问:管理员放行任意 app(即"管理员切换 app"语义);
// 应用角色仅能访问路径参数等于自身 AppID 的资源,越权→403。param 为路径参数名(如 "appId")。
// 须在 SessionAuth 之后挂。
func AppScope(param string) gin.HandlerFunc {
	return func(c *gin.Context) {
		sess, ok := SessionFrom(c)
		if !ok {
			abortJSON(c, http.StatusUnauthorized, "缺少认证信息")
			return
		}
		if sess.IsAdmin() {
			c.Next()
			return
		}
		pathID, _ := strconv.ParseInt(c.Param(param), 10, 64)
		if pathID != sess.AppID {
			abortJSON(c, http.StatusForbidden, "无权访问该 app 资源")
			return
		}
		c.Next()
	}
}

// abortJSON 写入 {code,msg} 并中断链。响应形状与 protocol/own 的 fail() 对齐。
func abortJSON(c *gin.Context, code int, msg string) {
	c.AbortWithStatusJSON(code, gin.H{"code": code, "msg": msg})
}
