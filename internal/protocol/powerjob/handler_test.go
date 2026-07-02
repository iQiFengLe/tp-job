package powerjob

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
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
		Reg: reg, IL: il, Store: st, ServerAddr: "host:8080",
	}, st, reg
}

func do(t *testing.T, method, path string, body any, d Deps) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	} else {
		r = bytes.NewReader(nil)
	}
	g := gin.New()
	RegisterServer(g.Group("/server"), d)
	req := httptest.NewRequest(method, path, r)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)
	return w
}

// assert:appName → appId。
func TestAssert(t *testing.T) {
	d, _, _ := newDeps(t)
	app, _ := d.Apps.Create("myapp", "p", 0)

	w := do(t, "GET", "/server/assert?appName=myapp", nil, d)
	if w.Code != 200 {
		t.Fatalf("应 200, got %d", w.Code)
	}
	var resp ResultDTO
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if !resp.Success || resp.Data != float64(app.ID) {
		t.Fatalf("assert 应返回 appId=%d, got %+v", app.ID, resp)
	}

	// 未知 app
	w = do(t, "GET", "/server/assert?appName=nope", nil, d)
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Success {
		t.Fatal("未知 app 应 fail")
	}
}

// 心跳:注册后 registry 能 Pick(protocol=powerjob)。
func TestHeartbeatPick(t *testing.T) {
	d, _, reg := newDeps(t)
	a, _ := d.Apps.Create("a", "p", 0)
	do(t, "POST", "/server/workerHeartbeat", HeartbeatReq{
		AppName: "a", WorkerAddress: "1.2.3.4:9",
		SystemMetrics: domain.SystemMetrics{Score: 5}, Tag: "gpu",
	}, d)
	// Reg 应能 Pick(单 tag → tags)
	if got := reg.Pick(a.ID, "gpu"); got != "1.2.3.4:9" {
		t.Fatalf("Pick 应返回注册 worker, got %q", got)
	}
	// 在线列表含该 worker,且 protocol=powerjob
	online := reg.Online(a.ID)
	if len(online) != 1 || online[0].Protocol != workerreg.ProtocolPowerJob {
		t.Fatalf("protocol 应 powerjob, got %+v", online)
	}
}

// reportInstanceStatus:数字码 5 → success;终态守护拒绝迟到 4。
func TestReportInstanceStatus(t *testing.T) {
	d, st, _ := newDeps(t)
	d.Apps.Create("a", "p", 0)
	ins := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusRunning}
	_ = st.Instance.Create(ins)

	do(t, "POST", "/server/reportInstanceStatus",
		ReportInstanceStatusReq{InstanceID: ins.ID, InstanceStatus: WireSucceed, Result: "ok"}, d)
	got, _ := st.Instance.Get(ins.ID)
	if got.Status != domain.StatusSuccess {
		t.Fatalf("数字码 5 应→success, got %s", got.Status)
	}
	// 迟到 4(FAILED)不覆盖
	do(t, "POST", "/server/reportInstanceStatus",
		ReportInstanceStatusReq{InstanceID: ins.ID, InstanceStatus: WireFailed}, d)
	got, _ = st.Instance.Get(ins.ID)
	if got.Status != domain.StatusSuccess {
		t.Fatalf("终态守护应拒绝覆盖, got %s", got.Status)
	}
	// 非法码 7 静默
	do(t, "POST", "/server/reportInstanceStatus",
		ReportInstanceStatusReq{InstanceID: ins.ID, InstanceStatus: 7}, d)
}

// reportLog:批量落库到实例日志。
func TestReportLog(t *testing.T) {
	d, st, _ := newDeps(t)
	il := d.IL
	d.Apps.Create("a", "p", 0)
	ins := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusRunning}
	_ = st.Instance.Create(ins)

	do(t, "POST", "/server/reportLog", LogReportReq{InstanceLogContents: []LogContent{
		{InstanceID: ins.ID, LogLevel: 4, LogContent: "boom", LogTime: 1000},
		{InstanceID: ins.ID, LogLevel: 2, LogContent: "ok"},
	}}, d)
	lines, total, _ := il.Read(ins.AppID, ins.ID, ins.RootInstanceID, instancelog.LogQuery{})
	if total != 2 {
		t.Fatalf("应 2 行日志, got %d", total)
	}
	_ = lines
}

// queryJobCluster:返回在线 worker 地址(base64 编码)。
func TestQueryJobCluster(t *testing.T) {
	d, _, _ := newDeps(t)
	app, _ := d.Apps.Create("a", "p", 0)
	d.Reg.Heartbeat(workerreg.WorkerInfo{AppID: app.ID, WorkerAddress: "w1:9", Protocol: workerreg.ProtocolPowerJob})
	d.Reg.Heartbeat(workerreg.WorkerInfo{AppID: app.ID, WorkerAddress: "w2:9", Protocol: workerreg.ProtocolPowerJob})

	w := do(t, "POST", "/server/queryJobCluster", QueryClusterReq{AppID: app.ID}, d)
	var resp AskResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if !resp.Success {
		t.Fatal("应 success")
	}
	raw, _ := base64.StdEncoding.DecodeString(resp.Data)
	var addrs []string
	_ = json.Unmarshal(raw, &addrs)
	if len(addrs) != 2 {
		t.Fatalf("应返回 2 个 worker 地址, got %v", addrs)
	}
}
