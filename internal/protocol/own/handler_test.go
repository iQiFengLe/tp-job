package own

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"

	"task-schedule/internal/dispatch"
	"task-schedule/internal/domain"
	"task-schedule/internal/dservice"
	"task-schedule/internal/instancelog"
	"task-schedule/internal/repository"
	"task-schedule/internal/workerreg"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newDeps(t *testing.T) (Deps, *repository.Store) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "t.db")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	st, err := repository.FromDB(db)
	if err != nil {
		t.Fatal(err)
	}
	reg := workerreg.New(time.Minute, nil)
	il := instancelog.New(t.TempDir(), 0)
	sch := dispatch.NewScheduler(st, dispatch.New(reg, time.Second), il, 50*time.Millisecond, discardLog())
	return Deps{
		Apps: dservice.NewAppService(st), Jobs: dservice.NewJobService(st, sch),
		Instances: dservice.NewInstanceService(st, sch, il), Store: st,
	}, st
}

func req(t *testing.T, method, path string, body any, d Deps) *httptest.ResponseRecorder {
	t.Helper()
	var rbody *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rbody = bytes.NewReader(b)
	} else {
		rbody = bytes.NewReader(nil)
	}
	g := gin.New()
	Register(g.Group("/api"), d)
	req := httptest.NewRequest(method, path, rbody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)
	return w
}

func bodyData(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	return m
}

// createApp → createJob → trigger → listInstances 全链路。
func TestOwnFullChain(t *testing.T) {
	d, _ := newDeps(t)

	// 1. createApp
	w := req(t, "POST", "/api/apps", CreateAppReq{AppName: "demo", Password: "p"}, d)
	if w.Code != http.StatusOK {
		t.Fatalf("createApp 应 200, got %d: %s", w.Code, w.Body.String())
	}
	appID := int64(bodyData(t, w)["data"].(map[string]any)["id"].(float64))

	// 2. createJob(cron)
	w = req(t, "POST", "/api/apps/"+itoa(appID)+"/jobs",
		CreateJobReq{Name: "j", ScheduleKind: "manual", JobParams: "p1", Tag: "t"}, d)
	if w.Code != http.StatusOK {
		t.Fatalf("createJob 应 200, got %d: %s", w.Code, w.Body.String())
	}
	jobID := int64(bodyData(t, w)["data"].(map[string]any)["id"].(float64))

	// 3. trigger
	w = req(t, "POST", "/api/apps/"+itoa(appID)+"/jobs/"+itoa(jobID)+"/trigger?priority=3", nil, d)
	if w.Code != http.StatusOK {
		t.Fatalf("trigger 应 200, got %d: %s", w.Code, w.Body.String())
	}

	// 4. listInstances:应有 1 个 queued 手动实例(无 worker,不会派出)
	w = req(t, "GET", "/api/apps/"+itoa(appID)+"/instances", nil, d)
	data := bodyData(t, w)["data"].(map[string]any)
	list := data["list"].([]any)
	if len(list) != 1 {
		t.Fatalf("应 1 个实例, got %d", len(list))
	}
	ins := list[0].(map[string]any)
	if ins["status"] != "queued" || ins["trigger_type"] != "manual" {
		t.Fatalf("实例状态/类型不符: %+v", ins)
	}
}

// createJob 非法 schedule_kind → 400。
func TestOwnCreateJobValidate(t *testing.T) {
	d, _ := newDeps(t)
	req(t, "POST", "/api/apps", CreateAppReq{AppName: "a", Password: "p"}, d)
	w := req(t, "POST", "/api/apps/1/jobs", CreateJobReq{Name: "j", ScheduleKind: "bogus"}, d)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("非法 kind 应 400, got %d", w.Code)
	}
}

// getInstance 不存在 → 404。
func TestOwnInstanceNotFound(t *testing.T) {
	d, _ := newDeps(t)
	w := req(t, "GET", "/api/apps/1/instances/999", nil, d)
	if w.Code != http.StatusNotFound {
		t.Fatalf("不存在应 404, got %d", w.Code)
	}
}

// listWorkers 读 workerreg 在线节点。心跳一个 → 列出 1 个;Reg nil 时返回空。
func TestListWorkers(t *testing.T) {
	d, _ := newDeps(t)
	reg := workerreg.New(time.Minute, nil)
	d.Reg = reg

	req(t, "POST", "/api/apps", CreateAppReq{AppName: "a", Password: "p"}, d) // app id=1
	reg.Heartbeat(workerreg.WorkerInfo{
		AppID: 1, WorkerAddress: "10.0.0.1:9000", Protocol: workerreg.ProtocolHTTP,
		Tags: []string{"gpu"}, AcceptNotTagJob: true,
		Metrics: domain.SystemMetrics{Score: 10, CpuProcessors: 4, CpuLoad: 1.2},
	})

	w := req(t, "GET", "/api/apps/1/workers", nil, d)
	if w.Code != http.StatusOK {
		t.Fatalf("应 200, got %d: %s", w.Code, w.Body.String())
	}
	list := bodyData(t, w)["data"].(map[string]any)["list"].([]any)
	if len(list) != 1 {
		t.Fatalf("应 1 个在线 worker, got %d", len(list))
	}
	wk := list[0].(map[string]any)
	if wk["worker_address"] != "10.0.0.1:9000" || wk["protocol"] != "http" {
		t.Fatalf("worker 字段不符: %+v", wk)
	}
}

// Reg 为 nil 时 listWorkers 返回空列表(不 panic)。
func TestListWorkersNilReg(t *testing.T) {
	d, _ := newDeps(t) // Reg 未设
	req(t, "POST", "/api/apps", CreateAppReq{AppName: "a", Password: "p"}, d)
	w := req(t, "GET", "/api/apps/1/workers", nil, d)
	if w.Code != http.StatusOK {
		t.Fatalf("应 200, got %d", w.Code)
	}
	data := bodyData(t, w)["data"].(map[string]any)
	if data["count"].(float64) != 0 {
		t.Fatalf("Reg nil 应 0 worker, got %v", data["count"])
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// 越权防护回归:getInstance / instanceLogs 用 :iid 时校验归属 :appId,跨 app 读取应 404。
// 覆盖 GetInApp/LogsInApp——AppScope 中间件只校验路径 :appId,实例归属在此补齐。
func TestOwnInstanceCrossAppDenied(t *testing.T) {
	d, _ := newDeps(t)

	wa1 := req(t, "POST", "/api/apps", CreateAppReq{AppName: "a1", Password: "p"}, d)
	a1 := int64(bodyData(t, wa1)["data"].(map[string]any)["id"].(float64))
	wa2 := req(t, "POST", "/api/apps", CreateAppReq{AppName: "a2", Password: "p"}, d)
	a2 := int64(bodyData(t, wa2)["data"].(map[string]any)["id"].(float64))

	wj := req(t, "POST", "/api/apps/"+itoa(a1)+"/jobs", CreateJobReq{Name: "j", ScheduleKind: "manual"}, d)
	jobID := int64(bodyData(t, wj)["data"].(map[string]any)["id"].(float64))

	// 手动触发 → app1 名下 1 个 queued 实例(SubmitManual 落库即返回,不依赖调度循环)
	req(t, "POST", "/api/apps/"+itoa(a1)+"/jobs/"+itoa(jobID)+"/trigger", nil, d)
	wl := req(t, "GET", "/api/apps/"+itoa(a1)+"/instances", nil, d)
	list := bodyData(t, wl)["data"].(map[string]any)["list"].([]any)
	if len(list) != 1 {
		t.Fatalf("app1 应 1 个实例, got %d", len(list))
	}
	insID := int64(list[0].(map[string]any)["id"].(float64))

	// 自家 app1 读取实例:200
	if w := req(t, "GET", "/api/apps/"+itoa(a1)+"/instances/"+itoa(insID), nil, d); w.Code != http.StatusOK {
		t.Fatalf("自家 app 读取实例应 200, got %d: %s", w.Code, w.Body.String())
	}
	// 跨 app(a2) 读取 app1 的实例:应 404(防身份枚举,非 403)
	if w := req(t, "GET", "/api/apps/"+itoa(a2)+"/instances/"+itoa(insID), nil, d); w.Code != http.StatusNotFound {
		t.Fatalf("跨 app 读取实例应 404, got %d: %s", w.Code, w.Body.String())
	}
	// 跨 app 读日志:同样 404
	if w := req(t, "GET", "/api/apps/"+itoa(a2)+"/instances/"+itoa(insID)+"/logs", nil, d); w.Code != http.StatusNotFound {
		t.Fatalf("跨 app 读日志应 404, got %d: %s", w.Code, w.Body.String())
	}
}
