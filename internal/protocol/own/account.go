package own

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"dida/internal/auth"
	"dida/internal/dservice"
)

// ===== 账户管理 DTO / 端点 =====
//
// 管理员账户已入库(admin_user 表,首次启动 seed admin/admin123)。此处提供当前登录管理员
// 自查 / 改用户名 / 改密码三个端点。三接口均前置 [SessionAuth, RequireAdmin]——仅管理员
// 有 admin_user 记录;从 session.UserID 定位账户。应用账户改密仍走 PUT /api/apps/:appId。

// AccountProfileView 当前账户信息(不含密码)。
type AccountProfileView struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

// UpdateProfileReq 改用户名请求。
type UpdateProfileReq struct {
	Username string `json:"username" binding:"required"`
}

// ChangePasswordReq 改密码请求。old_password 校验当前密码;new_password 为明文,服务端 bcrypt。
type ChangePasswordReq struct {
	OldPassword string `json:"old_password" binding:"required"`
	NewPassword string `json:"new_password" binding:"required"`
}

// RegisterAccount 挂载 /account/* 路由:GET/PUT profile(自查/改名)、PUT password(改密)。
// 各路由前置 [SessionAuth, RequireAdmin];account 无 :appId 路径参数,不走 ownRoutes 的
// AppScope 矩阵,单独注册更清晰。须在 d.Auth != nil 时调用。
func RegisterAccount(r *gin.RouterGroup, d Deps) {
	sa := auth.SessionAuth(d.Auth)
	adm := auth.RequireAdmin()
	r.GET("/account/profile", sa, adm, d.getProfile)
	r.PUT("/account/profile", sa, adm, d.updateProfile)
	r.PUT("/account/password", sa, adm, d.changePassword)
}

// adminUsersOrFail 取账户服务;未装配(AdminUsers=nil)时回 503 并返回 nil,对齐 handler.go
// 的 PowerJobClient 路径风格(显式 nil→503,而非 nil 解引用 panic)。
func (d Deps) adminUsersOrFail(c *gin.Context) *dservice.AdminUserService {
	if d.AdminUsers == nil {
		fail(c, http.StatusServiceUnavailable, "账户服务未装配")
		return nil
	}
	return d.AdminUsers
}

func (d Deps) getProfile(c *gin.Context) {
	svc := d.adminUsersOrFail(c)
	if svc == nil {
		return
	}
	sess, _ := auth.SessionFrom(c)
	u, err := svc.Profile(sess.UserID)
	if err != nil {
		fail(c, accountStatus(err), err.Error())
		return
	}
	ok(c, AccountProfileView{ID: u.ID, Username: u.Username})
}

func (d Deps) updateProfile(c *gin.Context) {
	var req UpdateProfileReq
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "参数错误: "+err.Error())
		return
	}
	svc := d.adminUsersOrFail(c)
	if svc == nil {
		return
	}
	sess, _ := auth.SessionFrom(c)
	if err := svc.ChangeUsername(sess.UserID, req.Username); err != nil {
		fail(c, accountStatus(err), err.Error())
		return
	}
	ok(c, gin.H{"id": sess.UserID})
}

func (d Deps) changePassword(c *gin.Context) {
	var req ChangePasswordReq
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "参数错误: "+err.Error())
		return
	}
	svc := d.adminUsersOrFail(c)
	if svc == nil {
		return
	}
	sess, _ := auth.SessionFrom(c)
	if err := svc.ChangePassword(sess.UserID, req.OldPassword, req.NewPassword); err != nil {
		fail(c, accountStatus(err), err.Error())
		return
	}
	ok(c, gin.H{"id": sess.UserID})
}

// accountStatus 把管理员账户 service 错误映射到 HTTP 码(复用 handler.go 的 isSentinel)。
func accountStatus(err error) int {
	switch {
	case isSentinel(err, dservice.ErrAdminUserValidate), isSentinel(err, dservice.ErrAdminPasswordWrong):
		return http.StatusBadRequest
	case isSentinel(err, dservice.ErrAdminUserDuplicate):
		return http.StatusConflict
	case isSentinel(err, dservice.ErrAdminUserNotFound):
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}
