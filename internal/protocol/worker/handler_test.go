package worker

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

func newDeps(t *testing.T) (Deps, *repository.Store, *workerreg.Registry) {
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
		Apps: dservice.NewAppService(st), Instances: dservice.NewInstanceService(st, sch, il),
		Reg: reg, IL: il, Store: st,
	}, st, reg
}

func postJSON(t *testing.T, r http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// 心跳:注册 worker(appName→AppID),随后 registry 能 Pick 到。
func TestHeartbeatRegisters(t *testing.T) {
	d, st, reg := newDeps(t)
	app, _ := d.Apps.Create("demo", "p", 0)

	g := gin.New()
	Register(g.Group("/worker"), d)

	resp := postJSON(t, g, "/worker/heartbeat", HeartbeatReq{
		AppName: "demo", WorkerAddress: "1.2.3.4:9000",
		SystemMetrics: domain.SystemMetrics{Score: 7}, Tags: []string{"gpu"}, Protocol: "http",
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("应 200, got %d body=%s", resp.Code, resp.Body.String())
	}
	if got := reg.Pick(app.ID, "gpu"); got != "1.2.3.4:9000" {
		t.Fatalf("Pick 应返回注册的 worker, got %q", got)
	}
	_ = st
}

// 心跳:app 不存在 → 404。
func TestHeartbeatUnknownApp(t *testing.T) {
	d, _, _ := newDeps(t)
	g := gin.New()
	Register(g.Group("/worker"), d)
	resp := postJSON(t, g, "/worker/heartbeat", HeartbeatReq{AppName: "nope", WorkerAddress: "x:9"})
	if resp.Code != http.StatusNotFound {
		t.Fatalf("未知 app 应 404, got %d", resp.Code)
	}
}

// 回报 status:running→success;终态后迟到 failed 不覆盖。
func TestReportStatus(t *testing.T) {
	d, st, _ := newDeps(t)
	d.Apps.Create("a", "p", 0)
	ins := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusRunning}
	_ = st.Instance.Create(ins)

	g := gin.New()
	Register(g.Group("/worker"), d)

	postJSON(t, g, "/worker/instances/"+itoa(ins.ID)+"/status", ReportStatusReq{Status: domain.StatusSuccess, Result: "ok"})
	got, _ := st.Instance.Get(ins.ID)
	if got.Status != domain.StatusSuccess {
		t.Fatalf("应为 success, got %s", got.Status)
	}
	// 迟到 failed
	postJSON(t, g, "/worker/instances/"+itoa(ins.ID)+"/status", ReportStatusReq{Status: domain.StatusFailed})
	got, _ = st.Instance.Get(ins.ID)
	if got.Status != domain.StatusSuccess {
		t.Fatalf("终态守护应拒绝覆盖, got %s", got.Status)
	}
}

// 上报日志:写入实例日志文件。
func TestReportLog(t *testing.T) {
	d, st, _ := newDeps(t)
	il := d.IL
	d.Apps.Create("a", "p", 0)
	ins := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusRunning}
	_ = st.Instance.Create(ins)

	g := gin.New()
	Register(g.Group("/worker"), d)
	postJSON(t, g, "/worker/instances/"+itoa(ins.ID)+"/logs", ReportLogReq{Level: "warn", Message: "hello", Time: 1000})

	lines, total, err := il.Read(ins.AppID, ins.ID, ins.RootInstanceID, instancelog.LogQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(lines) != 1 {
		t.Fatalf("应 1 行日志, got %d", total)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
