package powerjob

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
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
	"task-schedule/internal/wire"
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
	sch := dispatch.NewScheduler(st, dispatch.New(reg, time.Second), il, 50*time.Millisecond, discardLog(), dispatch.NoopCallbackBuilder{})
	return Deps{
		Apps: dservice.NewAppService(st), Instances: dservice.NewInstanceService(st, sch, il, dispatch.NoopCallbackBuilder{}),
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

// reportInstanceStatus:数字码 5 → success;终态守护拒绝迟到 4;字符串 instanceId 也能推进;
// 回报 1(WAITING_DISPATCH→queued)解绑 worker(无法处理,请重新调度)。
func TestReportInstanceStatus(t *testing.T) {
	d, st, _ := newDeps(t)
	d.Apps.Create("a", "p", 0)
	ins := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusRunning}
	_ = st.Instance.Create(ins)

	do(t, "POST", "/server/reportInstanceStatus",
		ReportInstanceStatusReq{InstanceID: wire.FlexInt64(ins.ID), InstanceStatus: WireSucceed, Result: "ok"}, d)
	got, _ := st.Instance.Get(ins.ID)
	if got.Status != domain.StatusSuccess {
		t.Fatalf("数字码 5 应→success, got %s", got.Status)
	}
	// 迟到 4(FAILED)不覆盖
	do(t, "POST", "/server/reportInstanceStatus",
		ReportInstanceStatusReq{InstanceID: wire.FlexInt64(ins.ID), InstanceStatus: WireFailed}, d)
	got, _ = st.Instance.Get(ins.ID)
	if got.Status != domain.StatusSuccess {
		t.Fatalf("终态守护应拒绝覆盖, got %s", got.Status)
	}
	// 非法码 7 静默
	do(t, "POST", "/server/reportInstanceStatus",
		ReportInstanceStatusReq{InstanceID: wire.FlexInt64(ins.ID), InstanceStatus: 7}, d)
	// 字符串写法(多语言 worker 实际发 "instanceId":"5"):FlexInt64 必须兼容,状态也能推进
	ins2 := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusRunning}
	_ = st.Instance.Create(ins2)
	do(t, "POST", "/server/reportInstanceStatus", map[string]any{
		"instanceId": strconv.FormatInt(ins2.ID, 10), "instanceStatus": WireSucceed, "result": "ok",
	}, d)
	got2, _ := st.Instance.Get(ins2.ID)
	if got2.Status != domain.StatusSuccess {
		t.Fatalf("字符串 instanceId 应也能→success, got %s", got2.Status)
	}

	// worker 回报 1(WAITING_DISPATCH)表示无法处理:应解绑 worker + 清 start_time,使实例可重新派发
	ins3 := &domain.Instance{
		JobID: 1, AppID: 1, Status: domain.StatusWaitingReceive,
		WorkerAddress: "worker1:9000", StartTime: timePtr(time.Now()),
	}
	_ = st.Instance.Create(ins3)
	do(t, "POST", "/server/reportInstanceStatus",
		ReportInstanceStatusReq{InstanceID: wire.FlexInt64(ins3.ID), InstanceStatus: WireWaitingDispatch,
			Result: "资源不足,请重新调度"}, d)
	got3, _ := st.Instance.Get(ins3.ID)
	if got3.Status != domain.StatusQueued {
		t.Fatalf("回报 1 应→queued, got %s", got3.Status)
	}
	if got3.WorkerAddress != "" {
		t.Fatalf("回报 queued 应解绑 worker,got %s", got3.WorkerAddress)
	}
	if got3.StartTime != nil {
		t.Fatalf("回报 queued 应清 start_time,got %v", got3.StartTime)
	}
	if got3.Result != "资源不足,请重新调度" {
		t.Fatalf("result 应保留,got %s", got3.Result)
	}
}

func timePtr(t time.Time) *time.Time { return &t }

// reportLog:批量落库到实例日志。
func TestReportLog(t *testing.T) {
	d, st, _ := newDeps(t)
	il := d.IL
	d.Apps.Create("a", "p", 0)
	ins := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusRunning}
	_ = st.Instance.Create(ins)

	// 数字写法(PowerJob 标准):instanceId 是数字
	do(t, "POST", "/server/reportLog", LogReportReq{InstanceLogContents: []LogContent{
		{InstanceID: wire.FlexInt64(ins.ID), LogLevel: 4, LogContent: "boom", LogTime: 1000},
	}}, d)
	// 字符串写法(多语言 worker 实际发 "instanceId":"5"):wire.FlexInt64 必须兼容,
	// 否则 Go json 严格类型置零 → 关联不到实例 → 日志丢失(曾因此丢全部 worker 过程日志)
	do(t, "POST", "/server/reportLog", map[string]any{
		"instanceLogContents": []map[string]any{
			{"instanceId": strconv.FormatInt(ins.ID, 10), "logLevel": 2, "logContent": "ok", "logTime": 2000},
		},
	}, d)
	lines, total, _ := il.Read(ins.AppID, ins.ID, ins.RootInstanceID, instancelog.LogQuery{})
	if total != 2 {
		t.Fatalf("数字+字符串两种 instanceId 写法都应落库, got %d 行: %v", total, lines)
	}
	// 坏 id(空/非法,FlexInt64 解析为 0)必须被跳过,且不影响同批合法日志落库——
	// 防 cache[0]=nil 污染回归:一旦把 iid=0 的 nil 写入 cache,同批后续合法日志也可能被连带吞掉。
	do(t, "POST", "/server/reportLog", map[string]any{
		"instanceLogContents": []map[string]any{
			{"instanceId": "", "logLevel": 4, "logContent": "bad-empty", "logTime": 3000},
			{"instanceId": "not-a-number", "logLevel": 4, "logContent": "bad-nan", "logTime": 3100},
			{"instanceId": strconv.FormatInt(ins.ID, 10), "logLevel": 2, "logContent": "after-bad", "logTime": 3200},
		},
	}, d)
	_, total2, _ := il.Read(ins.AppID, ins.ID, ins.RootInstanceID, instancelog.LogQuery{})
	if total2 != 3 {
		t.Fatalf("坏 id 应跳过且不污染批次,合法日志应继续落库(预期 3 行), got %d", total2)
	}
	_ = lines
}

// queryJobCluster:返回在线 worker 地址(base64 编码)。
func TestQueryJobCluster(t *testing.T) {
	d, _, _ := newDeps(t)
	app, _ := d.Apps.Create("a", "p", 0)
	d.Reg.Heartbeat(workerreg.WorkerInfo{AppID: app.ID, WorkerAddress: "w1:9", Protocol: workerreg.ProtocolPowerJob})
	d.Reg.Heartbeat(workerreg.WorkerInfo{AppID: app.ID, WorkerAddress: "w2:9", Protocol: workerreg.ProtocolPowerJob})

	w := do(t, "POST", "/server/queryJobCluster", QueryClusterReq{AppID: wire.FlexInt64(app.ID)}, d)
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

func newOpenApiDeps(t *testing.T) (OpenApiDeps, *repository.Store) {
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
	sch := dispatch.NewScheduler(st, dispatch.New(reg, time.Second), il, 50*time.Millisecond, discardLog(), dispatch.NoopCallbackBuilder{})
	return OpenApiDeps{
		Jobs:      dservice.NewJobService(st, sch),
		Instances: dservice.NewInstanceService(st, sch, il, dispatch.NoopCallbackBuilder{}),
		Apps:      dservice.NewAppService(st),
		Store:     st,
	}, st
}

// /openApi/runJob:PowerJob OpenAPI 兼容——form 触发,返回 ResultDTO<Long>;Content-Type 必须是 JSON。
func TestOpenApiRunJob(t *testing.T) {
	d, st := newOpenApiDeps(t)
	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	_ = st.Job.Create(&domain.Job{ID: 1, AppID: 1, Name: "j", ExecuteType: "http", ScheduleKind: "api", Enabled: true})

	post := func(body string) *httptest.ResponseRecorder {
		g := gin.New()
		RegisterOpenApi(g.Group("/openApi"), d)
		req := httptest.NewRequest("POST", "/openApi/runJob", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		g.ServeHTTP(w, req)
		return w
	}

	// 正常触发 → success + instanceId + Content-Type: json
	w := post("appId=1&jobId=1&instanceParams=demo&delayMS=0")
	if w.Code != 200 {
		t.Fatalf("应 200, got %d: %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "json") {
		t.Fatalf("Content-Type 应含 json(客户端 JsonResponseHandler 拒 text/html), got %s", ct)
	}
	var resp ResultDTO
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("应解析为 JSON: %v body=%s", err, w.Body.String())
	}
	if !resp.Success {
		t.Fatalf("应 success: %+v", resp)
	}
	instanceID := int64(resp.Data.(float64))
	if instanceID <= 0 {
		t.Fatalf("应返回正 instanceId, got %v", resp.Data)
	}
	ins, _ := st.Instance.Get(instanceID)
	if ins == nil || ins.JobID != 1 || ins.JobInstanceParams != "demo" {
		t.Fatalf("实例应已创建并回填参数: %+v", ins)
	}

	// jobId 不属于 appId(越权防护)→ fail;缺 appId → fail
	var fail ResultDTO
	_ = json.Unmarshal(post("appId=999&jobId=1&delayMS=0").Body.Bytes(), &fail)
	if fail.Success {
		t.Fatal("不存在的 appId/jobId 应 fail")
	}
	_ = json.Unmarshal(post("jobId=1&delayMS=0").Body.Bytes(), &fail)
	if fail.Success {
		t.Fatal("缺 appId 应 fail")
	}
}

// 综合覆盖:assertApp → saveJob(create) → fetchJob → runJob → fetchInstanceStatus → cancelInstance。
func TestOpenApiJobInstance(t *testing.T) {
	d, st := newOpenApiDeps(t)
	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})

	g := gin.New()
	RegisterOpenApi(g.Group("/openApi"), d)
	call := func(path, body string) *httptest.ResponseRecorder {
		var r io.Reader
		if body != "" {
			r = strings.NewReader(body)
		}
		req := httptest.NewRequest("POST", "/openApi/"+path, r)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		g.ServeHTTP(w, req)
		return w
	}
	callJSON := func(path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/openApi/"+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		g.ServeHTTP(w, req)
		return w
	}
	var resp ResultDTO

	// assertApp → appId
	_ = json.Unmarshal(call("assert", "appName=a").Body.Bytes(), &resp)
	if !resp.Success || int64(resp.Data.(float64)) != 1 {
		t.Fatalf("assertApp 应 success 且 appId=1: %+v", resp)
	}

	// saveJob(create,cron) → jobId
	_ = json.Unmarshal(callJSON("saveJob", `{"appId":1,"jobName":"j1","timeExpressionType":2,"timeExpression":"*/1 * * * *","enable":true}`).Body.Bytes(), &resp)
	if !resp.Success {
		t.Fatalf("saveJob create 应 success: %+v", resp)
	}
	jobID := int64(resp.Data.(float64))

	// fetchJob → JobInfoDTO(timeExpressionType 应=2 CRON)
	_ = json.Unmarshal(call("fetchJob", "appId=1&jobId="+strconv.FormatInt(jobID, 10)).Body.Bytes(), &resp)
	if !resp.Success {
		t.Fatalf("fetchJob 应 success: %+v", resp)
	}

	// runJob → instanceId
	_ = json.Unmarshal(call("runJob", "appId=1&jobId="+strconv.FormatInt(jobID, 10)+"&delayMS=0").Body.Bytes(), &resp)
	if !resp.Success {
		t.Fatalf("runJob 应 success: %+v", resp)
	}
	instanceID := int64(resp.Data.(float64))

	// fetchInstanceStatus → 数字码 queued=1
	_ = json.Unmarshal(call("fetchInstanceStatus", "instanceId="+strconv.FormatInt(instanceID, 10)).Body.Bytes(), &resp)
	if !resp.Success || int(resp.Data.(float64)) != WireWaitingDispatch {
		t.Fatalf("fetchInstanceStatus 应 success 且 queued=1: %+v", resp)
	}

	// cancelInstance → success;实例变 canceled
	_ = json.Unmarshal(call("cancelInstance", "instanceId="+strconv.FormatInt(instanceID, 10)).Body.Bytes(), &resp)
	if !resp.Success {
		t.Fatalf("cancelInstance 应 success: %+v", resp)
	}
	ins, _ := st.Instance.Get(instanceID)
	if ins.Status != domain.StatusCanceled {
		t.Fatalf("实例应 canceled, got %s", ins.Status)
	}
}

// saveJob 更新非 schedule 字段时校验 job 归属 app:跨 app 改名应 fail。
// 回归:Jobs.Update 仅在 schedule 字段变化时经 Get 带 app_id 校验,改 name 等会直达
// JobStore.Update(只按 id)——openapi saveJob 更新分支补 jobBelongToApp 后应拦截此越权。
func TestOpenApiSaveJobUpdateCrossAppDenied(t *testing.T) {
	d, st := newOpenApiDeps(t)
	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	_ = st.App.Create(&domain.App{ID: 2, AppName: "b"})
	_ = st.Job.Create(&domain.Job{ID: 1, AppID: 1, Name: "j", ExecuteType: "http", Enabled: true})

	g := gin.New()
	RegisterOpenApi(g.Group("/openApi"), d)
	doPost := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/openApi/saveJob", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		g.ServeHTTP(w, req)
		return w
	}

	// appId=2 改 app1 的 job1 的名字(只改 name,非 schedule 字段)→ 应 fail
	var resp ResultDTO
	_ = json.Unmarshal(doPost(`{"id":1,"appId":2,"jobName":"hacked"}`).Body.Bytes(), &resp)
	if resp.Success {
		t.Fatal("跨 app 改非 schedule 字段应 fail(越权)")
	}
	j, _ := st.Job.Get(1, 1)
	if j.Name != "j" {
		t.Fatalf("job 名不应被越权修改, got %q", j.Name)
	}

	// 自家 app1 改名应 success
	_ = json.Unmarshal(doPost(`{"id":1,"appId":1,"jobName":"renamed"}`).Body.Bytes(), &resp)
	if !resp.Success {
		t.Fatalf("自家 app 改名应 success: %+v", resp)
	}
	j, _ = st.Job.Get(1, 1)
	if j.Name != "renamed" {
		t.Fatalf("job 名应已改为 renamed, got %q", j.Name)
	}
}
