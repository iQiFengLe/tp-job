package powerjob

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestFetchJobsViaList 验证优先走 GET /appInfo/list(无需 app 密码)解析 appName→appId。
func TestFetchJobsViaList(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/appInfo/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"success": true, "data": []map[string]any{
			{"id": 7, "appName": "demo"},
		}})
	})
	mux.HandleFunc("/openApi/fetchAllJob", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		w.Header().Set("Content-Type", "application/json")
		if r.PostForm.Get("appId") != "7" {
			json.NewEncoder(w).Encode(map[string]any{"code": 500, "msg": "bad appId"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"code": 200, "data": []JobInfoDTO{
			{ID: 1, JobName: "j1", TimeExpressionType: 2, TimeExpression: "0 0 9 * * ? *", Status: 1},
		}})
	})
	// 故意不注册 /openApi/assert:走 list 路径就不该碰 assert
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClient(srv.Client())
	jobs, err := c.FetchJobs(context.Background(), srv.URL, "demo", "", "")
	if err != nil {
		t.Fatalf("FetchJobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].JobName != "j1" {
		t.Errorf("意外: %+v", jobs)
	}
}

// TestFetchJobsFallbackAssert:/appInfo/list 不可用 → 回退 assert(appName[,password])。
// app 设了密码时 assert 必须带 password(对齐真实 PowerJob "PowerJobException: password error!")。
func TestFetchJobsFallbackAssert(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/openApi/assert", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		w.Header().Set("Content-Type", "application/json")
		if r.PostForm.Get("password") != "secret" {
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": "password error"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"code": 200, "data": 7})
	})
	mux.HandleFunc("/openApi/fetchAllJob", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"code": 200, "data": []JobInfoDTO{}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClient(srv.Client())
	if _, err := c.FetchJobs(context.Background(), srv.URL, "demo", "", ""); err == nil {
		t.Error("缺密码期望报错")
	}
	if _, err := c.FetchJobs(context.Background(), srv.URL, "demo", "secret", ""); err != nil {
		t.Fatalf("带密码应成功: %v", err)
	}
}

// TestFetchJobsToken 验证 POWERJOB-TOKEN header 被带上(兼容 code:0 成功码)。
func TestFetchJobsToken(t *testing.T) {
	var gotToken string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("POWERJOB-TOKEN")
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/appInfo/list" {
			json.NewEncoder(w).Encode(map[string]any{"success": true, "data": []map[string]any{{"id": 1, "appName": "x"}}})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": []JobInfoDTO{}})
	}))
	defer srv.Close()
	c := NewClient(srv.Client())
	if _, err := c.FetchJobs(context.Background(), srv.URL, "x", "", "secret-tok"); err != nil {
		t.Fatalf("FetchJobs: %v", err)
	}
	if gotToken != "secret-tok" {
		t.Errorf("期望 token secret-tok,得 %q", gotToken)
	}
}

func TestFetchJobsBadAddr(t *testing.T) {
	c := NewClient(http.DefaultClient)
	if _, err := c.FetchJobs(context.Background(), "not-a-url", "x", "", ""); err == nil {
		t.Error("非法地址期望报错")
	}
	if _, err := c.FetchJobs(context.Background(), "ftp://x.com", "x", "", ""); err == nil {
		t.Error("非 http(s) 期望报错")
	}
}
