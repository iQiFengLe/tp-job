package own

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"

	"task-schedule/internal/auth"
)

// hashPwd 测试用 bcrypt 哈希(与 auth/login.go 的 admin 凭据格式一致)。
func hashPwd(t *testing.T, pwd string) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(pwd), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(h)
}

// newAuthDeps 在 newDeps 基础上注入鉴权:会话 store + LoginService(admin + app 表)。
func newAuthDeps(t *testing.T) (Deps, *auth.Store, *auth.LoginService) {
	t.Helper()
	d, _ := newDeps(t)
	store := auth.NewStore(time.Hour)
	d.Auth = store
	login := auth.NewLoginService(
		[]auth.AdminCredential{{Username: "admin", PasswordHash: hashPwd(t, "admin-pw")}},
		d.Apps, store,
	)
	return d, store, login
}

// buildAPI 装配完整 /api:公开 login + 受保护 RegisterAuth + 受保护资源路由(矩阵)。
func buildAPI(d Deps, store *auth.Store, login *auth.LoginService) *gin.Engine {
	g := gin.New()
	api := g.Group("/api")
	api.POST("/auth/login", LoginHandler(login))
	RegisterAuth(api, store)
	Register(api, d)
	return g
}

// authReq 发请求;token 非空则带 Bearer。
func authReq(t *testing.T, g *gin.Engine, method, path string, body any, token string) *httptest.ResponseRecorder {
	t.Helper()
	var rbody *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rbody = bytes.NewReader(b)
	} else {
		rbody = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rbody)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)
	return w
}

func loginAs(t *testing.T, g *gin.Engine, ident, pwd string) string {
	t.Helper()
	w := authReq(t, g, "POST", "/api/auth/login", LoginReq{Ident: ident, Password: pwd}, "")
	if w.Code != http.StatusOK {
		t.Fatalf("login(%s) 应 200, got %d: %s", ident, w.Code, w.Body.String())
	}
	return bodyData(t, w)["data"].(map[string]any)["token"].(string)
}

// createAppAsAdmin admin 创建 app,返回其 id。
func createAppAsAdmin(t *testing.T, g *gin.Engine, adminTok, name, pwd string) int64 {
	t.Helper()
	w := authReq(t, g, "POST", "/api/apps", CreateAppReq{AppName: name, Password: pwd}, adminTok)
	if w.Code != http.StatusOK {
		t.Fatalf("admin createApp(%s) 应 200, got %d: %s", name, w.Code, w.Body.String())
	}
	return int64(bodyData(t, w)["data"].(map[string]any)["id"].(float64))
}

func wantCode(t *testing.T, what string, w *httptest.ResponseRecorder, want int) {
	t.Helper()
	if w.Code != want {
		t.Fatalf("%s 应 %d, got %d: %s", what, want, w.Code, w.Body.String())
	}
}

func strPtr(s string) *string { return &s }

// TestOwnAuthMatrix 端到端证明双角色权限矩阵。
func TestOwnAuthMatrix(t *testing.T) {
	d, store, login := newAuthDeps(t)
	g := buildAPI(d, store, login)

	// —— 管理员链路 ——
	adminTok := loginAs(t, g, "admin", "admin-pw")

	w := authReq(t, g, "GET", "/api/auth/me", nil, adminTok)
	wantCode(t, "admin me", w, 200)
	if me := bodyData(t, w)["data"].(map[string]any); me["role"] != "admin" {
		t.Fatalf("admin me.role 应 admin, got %v", me["role"])
	}

	// admin 创建两个 app
	app1 := createAppAsAdmin(t, g, adminTok, "app1", "secret")
	app2 := createAppAsAdmin(t, g, adminTok, "app2", "secret")

	// admin 可访问任意 app 的资源(AppScope 放行 admin)
	wantCode(t, "admin 访问 app1 jobs", authReq(t, g, "GET", "/api/apps/"+itoa(app1)+"/jobs", nil, adminTok), 200)

	// —— 应用角色链路 ——
	appTok := loginAs(t, g, "app1", "secret")
	w = authReq(t, g, "GET", "/api/auth/me", nil, appTok)
	if me := bodyData(t, w)["data"].(map[string]any); me["role"] != "app" || int64(me["app_id"].(float64)) != app1 {
		t.Fatalf("app1 me 应 role=app app_id=%d, got %v", app1, me)
	}

	// app 角色:仅管理员可做的操作 → 403
	wantCode(t, "app POST /apps", authReq(t, g, "POST", "/api/apps", CreateAppReq{AppName: "x", Password: "y"}, appTok), 403)
	wantCode(t, "app GET /apps(listApps)", authReq(t, g, "GET", "/api/apps", nil, appTok), 403)
	wantCode(t, "app DELETE 别家 app", authReq(t, g, "DELETE", "/api/apps/"+itoa(app2), nil, appTok), 403)

	// app 角色:越权访问别家资源 → 403
	wantCode(t, "app GET 别家 jobs", authReq(t, g, "GET", "/api/apps/"+itoa(app2)+"/jobs", nil, appTok), 403)
	wantCode(t, "app GET 别家 app", authReq(t, g, "GET", "/api/apps/"+itoa(app2), nil, appTok), 403)

	// app 角色:自家资源 → 200
	wantCode(t, "app GET 自家 jobs", authReq(t, g, "GET", "/api/apps/"+itoa(app1)+"/jobs", nil, appTok), 200)
	wantCode(t, "app GET 自家 app", authReq(t, g, "GET", "/api/apps/"+itoa(app1), nil, appTok), 200)

	// app 角色:更新自家 app(改密码)→ 200
	wantCode(t, "app PUT 自家 app", authReq(t, g, "PUT", "/api/apps/"+itoa(app1), UpdateAppReq{Password: strPtr("new")}, appTok), 200)

	// —— 鉴权失败链路 ——
	wantCode(t, "无 token", authReq(t, g, "GET", "/api/apps", nil, ""), 401)
	wantCode(t, "伪造 token", authReq(t, g, "GET", "/api/apps", nil, "bogus"), 401)
	wantCode(t, "错密码登录", authReq(t, g, "POST", "/api/auth/login", LoginReq{Ident: "admin", Password: "nope"}, ""), 401)

	// —— logout 后旧 token 失效 ——
	wantCode(t, "logout", authReq(t, g, "POST", "/api/auth/logout", nil, appTok), 200)
	wantCode(t, "logout 后 me", authReq(t, g, "GET", "/api/auth/me", nil, appTok), 401)
}
