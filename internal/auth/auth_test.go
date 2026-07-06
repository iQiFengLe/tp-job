package auth

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"

	"task-schedule/internal/domain"
)

func init() { gin.SetMode(gin.TestMode) }

func mustHash(t *testing.T, pwd string) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(pwd), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(h)
}

// ===== Store =====

func TestStorePutGet(t *testing.T) {
	s := NewStore(time.Hour)
	sess, err := s.Put(Session{Role: RoleAdmin, Username: "admin"})
	if err != nil {
		t.Fatal(err)
	}
	if sess.Token == "" || sess.ExpiresAt.IsZero() || sess.CreatedAt.IsZero() {
		t.Fatalf("Put 应回填 token/时间, got %+v", sess)
	}
	got, ok := s.Get(sess.Token)
	if !ok || got.Username != "admin" {
		t.Fatalf("Get 应命中, got %+v ok=%v", got, ok)
	}
}

func TestStoreExpiry(t *testing.T) {
	s := NewStore(10 * time.Millisecond)
	sess, _ := s.Put(Session{Role: RoleAdmin})
	time.Sleep(30 * time.Millisecond)
	if _, ok := s.Get(sess.Token); ok {
		t.Fatal("过期 token 应 Get 不到")
	}
	// 过期项应已被 Get 顺带删除(二次 Get 仍 false 即可,确认无残留泄漏)
	if _, ok := s.Get(sess.Token); ok {
		t.Fatal("过期 token 不应残留")
	}
}

func TestStoreDelete(t *testing.T) {
	s := NewStore(time.Hour)
	sess, _ := s.Put(Session{Role: RoleApp, AppID: 7})
	s.Delete(sess.Token)
	if _, ok := s.Get(sess.Token); ok {
		t.Fatal("Delete 后应 Get 不到")
	}
	s.Delete("not-exists") // 无副作用
}

func TestStoreNilTTLDefaults(t *testing.T) {
	s := NewStore(0)
	if s.TTL() != 24*time.Hour {
		t.Fatalf("ttl<=0 应默认 24h, got %v", s.TTL())
	}
}

// ===== LoginService =====

// fakeVerifier 桩 AppVerifier:仅 "app1"/"secret" 命中,其余失败。
type fakeVerifier struct{ calls int }

func (f *fakeVerifier) Verify(name, password string) (*domain.App, error) {
	f.calls++
	if name == "app1" && password == "secret" {
		return &domain.App{ID: 7, AppName: "app1"}, nil
	}
	return nil, errors.New("unauthorized")
}

// fakeAdminLookup 桩 AdminUserLookup:仅 "admin" 命中(密码哈希预算好),其余 (nil,nil) 未命中。
type fakeAdminLookup struct {
	calls int
	hash  string
}

func newFakeAdminLookup(t *testing.T) *fakeAdminLookup {
	t.Helper()
	return &fakeAdminLookup{hash: mustHash(t, "admin-pw")}
}

func (f *fakeAdminLookup) Lookup(username string) (*domain.AdminUser, error) {
	f.calls++
	if username == "admin" {
		return &domain.AdminUser{ID: 1, Username: "admin", Password: f.hash}, nil
	}
	return nil, nil
}

func newLogin(t *testing.T) (*LoginService, *Store, *fakeVerifier, *fakeAdminLookup) {
	t.Helper()
	store := NewStore(time.Hour)
	apps := &fakeVerifier{}
	admins := newFakeAdminLookup(t)
	ls := NewLoginService(admins, apps, store)
	return ls, store, apps, admins
}

func TestLoginAdminSuccess(t *testing.T) {
	ls, store, _, _ := newLogin(t)
	sess, err := ls.Login("admin", "admin-pw")
	if err != nil {
		t.Fatal(err)
	}
	if sess.Role != RoleAdmin || sess.Username != "admin" || sess.UserID != 1 {
		t.Fatalf("admin session 字段不符: %+v", sess)
	}
	if _, ok := store.Get(sess.Token); !ok {
		t.Fatal("登录后 token 应入库")
	}
}

func TestLoginAdminWrongPassword(t *testing.T) {
	ls, _, _, _ := newLogin(t)
	if _, err := ls.Login("admin", "wrong"); !errors.Is(err, ErrLoginFailed) {
		t.Fatalf("admin 密码错应 ErrLoginFailed, got %v", err)
	}
}

func TestLoginAdminDoesNotFallThroughToApp(t *testing.T) {
	// 命中管理员用户名但密码错:不应回退到应用登录(避免同名应用凭据意外通过)。
	ls, _, apps, _ := newLogin(t)
	_, _ = ls.Login("admin", "wrong")
	if apps.calls != 0 {
		t.Fatalf("admin 用户名匹配时不应调用 app 校验, got calls=%d", apps.calls)
	}
}

// errAdminLookup 桩:Lookup 恒返回 DB 错误(模拟 admin_user 表查询故障)。
type errAdminLookup struct{}

func (errAdminLookup) Lookup(string) (*domain.AdminUser, error) {
	return nil, errors.New("db down")
}

func TestLoginAdminLookupErrorNoFallback(t *testing.T) {
	// Lookup 返回 DB 错:不回退 app 分支(保"命中管理员用户名即不回退"防同名语义),
	// 直接 ErrLoginFailed——即便存在可被 app 分支通过的同名 app(app1/secret)。
	store := NewStore(time.Hour)
	apps := &fakeVerifier{}
	ls := NewLoginService(errAdminLookup{}, apps, store)
	_, err := ls.Login("app1", "secret")
	if !errors.Is(err, ErrLoginFailed) {
		t.Fatalf("Lookup DB 错应 ErrLoginFailed, got %v", err)
	}
	if apps.calls != 0 {
		t.Fatalf("Lookup DB 错不应回退 app 校验, got calls=%d", apps.calls)
	}
}

func TestLoginAppSuccess(t *testing.T) {
	ls, store, _, _ := newLogin(t)
	sess, err := ls.Login("app1", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if sess.Role != RoleApp || sess.AppID != 7 || sess.AppName != "app1" {
		t.Fatalf("app session 字段不符: %+v", sess)
	}
	if _, ok := store.Get(sess.Token); !ok {
		t.Fatal("登录后 token 应入库")
	}
}

func TestLoginAppFail(t *testing.T) {
	ls, _, _, _ := newLogin(t)
	for _, c := range []struct{ name, pwd string }{
		{"app1", "wrong"}, {"ghost", "secret"},
	} {
		if _, err := ls.Login(c.name, c.pwd); !errors.Is(err, ErrLoginFailed) {
			t.Fatalf("%+v 应 ErrLoginFailed, got %v", c, err)
		}
	}
}

func TestLoginUnknownIdentity(t *testing.T) {
	ls, _, _, _ := newLogin(t)
	if _, err := ls.Login("nobody", "x"); !errors.Is(err, ErrLoginFailed) {
		t.Fatalf("未知身份应 ErrLoginFailed, got %v", err)
	}
}

// ===== 中间件 =====

// authRouter 装配:SessionAuth 全局 + 三条探测路由(分别挂 RequireAdmin/AppScope)。
func authRouter(t *testing.T, store *Store) *gin.Engine {
	t.Helper()
	r := gin.New()
	r.Use(SessionAuth(store))
	r.GET("/admin", RequireAdmin(), func(c *gin.Context) { c.String(200, "admin-ok") })
	r.GET("/apps/:appId", AppScope("appId"), func(c *gin.Context) { c.String(200, "app-ok") })
	return r
}

func do(r *gin.Engine, token, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("GET", path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestSessionAuthMissing(t *testing.T) {
	store := NewStore(time.Hour)
	w := do(authRouter(t, store), "", "/admin")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("无 token 应 401, got %d", w.Code)
	}
}

func TestSessionAuthBadToken(t *testing.T) {
	store := NewStore(time.Hour)
	w := do(authRouter(t, store), "bogus", "/admin")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("伪 token 应 401, got %d", w.Code)
	}
}

func TestRequireAdmin(t *testing.T) {
	store := NewStore(time.Hour)
	admin, _ := store.Put(Session{Role: RoleAdmin, Username: "a"})
	app, _ := store.Put(Session{Role: RoleApp, AppID: 1})
	r := authRouter(t, store)

	if w := do(r, admin.Token, "/admin"); w.Code != 200 {
		t.Fatalf("admin 应 200, got %d", w.Code)
	}
	if w := do(r, app.Token, "/admin"); w.Code != http.StatusForbidden {
		t.Fatalf("app 角色 /admin 应 403, got %d", w.Code)
	}
}

func TestAppScope(t *testing.T) {
	store := NewStore(time.Hour)
	admin, _ := store.Put(Session{Role: RoleAdmin})
	app, _ := store.Put(Session{Role: RoleApp, AppID: 5})
	r := authRouter(t, store)

	// admin 访问任意 app 放行
	if w := do(r, admin.Token, "/apps/999"); w.Code != 200 {
		t.Fatalf("admin 访问任意 app 应 200, got %d", w.Code)
	}
	// app 角色访问自家 app 放行
	if w := do(r, app.Token, "/apps/5"); w.Code != 200 {
		t.Fatalf("app 访问自家应 200, got %d", w.Code)
	}
	// app 角色访问别家 app 拒绝
	if w := do(r, app.Token, "/apps/6"); w.Code != http.StatusForbidden {
		t.Fatalf("app 越权应 403, got %d", w.Code)
	}
}

func TestSessionFromAbsent(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/", nil)
	if _, ok := SessionFrom(c); ok {
		t.Fatal("无 SessionAuth 时 SessionFrom 应 ok=false")
	}
}
