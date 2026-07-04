package dservice

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"

	"task-schedule/internal/dispatch"
	"task-schedule/internal/domain"
	"task-schedule/internal/instancelog"
	"task-schedule/internal/repository"
	"task-schedule/internal/workerreg"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newSvc(t *testing.T) (*repository.Store, *dispatch.Scheduler, *instancelog.Logger) {
	t.Helper()
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
	return st, sch, il
}

func TestAppCreateVerifyDelete(t *testing.T) {
	st, _, _ := newSvc(t)
	appSvc := NewAppService(st)

	app, err := appSvc.Create("demo", "pass123", 0)
	if err != nil {
		t.Fatal(err)
	}
	if app.ID == 0 {
		t.Fatal("ID 应自增")
	}
	// Verify 正确密码
	if _, err := appSvc.Verify("demo", "pass123"); err != nil {
		t.Fatalf("Verify 应成功: %v", err)
	}
	// Verify 错误密码
	if _, err := appSvc.Verify("demo", "wrong"); err == nil {
		t.Fatal("错误密码应失败")
	}
	// 删除:无 job 应成功
	if err := appSvc.Delete(app.ID); err != nil {
		t.Fatalf("删除应成功: %v", err)
	}
}

// App 下有 job → 拒绝删除。
func TestAppDeleteInUse(t *testing.T) {
	st, sch, _ := newSvc(t)
	appSvc, jobSvc := NewAppService(st), NewJobService(st, sch)
	app, _ := appSvc.Create("a", "p", 0)
	job := &domain.Job{AppID: app.ID, Name: "j", ExecuteType: "http", ScheduleKind: "api", Enabled: true}
	if err := jobSvc.Create(job); err != nil {
		t.Fatal(err)
	}
	if err := appSvc.Delete(app.ID); err != ErrAppInUse {
		t.Fatalf("应拒绝删除(InUse), got %v", err)
	}
}

// Job.Create:cron 推算 next_run;disabled 不推算。
func TestJobCreateNextRun(t *testing.T) {
	st, sch, _ := newSvc(t)
	jobSvc := NewJobService(st, sch)
	appSvc := NewAppService(st)
	app, _ := appSvc.Create("a", "p", 0)

	// enabled cron → 有 next_run
	j := &domain.Job{AppID: app.ID, Name: "j1", ExecuteType: "http",
		ScheduleKind: "cron", ScheduleExpr: "*/1 * * * *", Enabled: true}
	if err := jobSvc.Create(j); err != nil {
		t.Fatal(err)
	}
	if j.NextRunTime == nil {
		t.Fatal("enabled cron 应有 next_run_time")
	}
	// disabled → 无 next_run
	j2 := &domain.Job{AppID: app.ID, Name: "j2", ExecuteType: "http",
		ScheduleKind: "cron", ScheduleExpr: "*/1 * * * *", Enabled: false}
	if err := jobSvc.Create(j2); err != nil {
		t.Fatal(err)
	}
	if j2.NextRunTime != nil {
		t.Fatal("disabled 应无 next_run_time")
	}
	// 非法 kind → 报错
	if err := jobSvc.Create(&domain.Job{AppID: app.ID, Name: "x", ScheduleKind: "bogus"}); err == nil {
		t.Fatal("非法 kind 应报错")
	}
}

// validateJob:start_time 晚于 end_time → 报错(生效窗口非法)。
func TestJobValidateWindow(t *testing.T) {
	st, sch, _ := newSvc(t)
	jobSvc := NewJobService(st, sch)
	appSvc := NewAppService(st)
	app, _ := appSvc.Create("a", "p", 0)

	start := time.Now().Add(time.Hour)
	end := time.Now()
	if err := jobSvc.Create(&domain.Job{AppID: app.ID, Name: "j", ExecuteType: "http",
		ScheduleKind: "cron", ScheduleExpr: "*/1 * * * *",
		StartTime: &start, EndTime: &end, Enabled: true}); err == nil {
		t.Fatal("start_time 晚于 end_time 应报错")
	}
}

// Update 清空 start_time(fields 值为 *time.Time nil) → gorm map 应写 NULL,确认清空生效。
// 回归:own 时间戳 0=清空 依赖 gorm 对 map nil 写 NULL(非跳过)。
func TestJobUpdateClearStartTime(t *testing.T) {
	st, sch, _ := newSvc(t)
	jobSvc := NewJobService(st, sch)
	appSvc := NewAppService(st)
	app, _ := appSvc.Create("a", "p", 0)
	start := time.Now().Add(time.Hour)
	j := &domain.Job{AppID: app.ID, Name: "j", ExecuteType: "http",
		ScheduleKind: "cron", ScheduleExpr: "*/1 * * * *", StartTime: &start, Enabled: true}
	if err := jobSvc.Create(j); err != nil {
		t.Fatal(err)
	}
	// own UpdateJobReqToFields 对 ms<=0 产出 (*time.Time)(nil)
	if err := jobSvc.Update(app.ID, j.ID, map[string]any{"start_time": (*time.Time)(nil)}); err != nil {
		t.Fatalf("Update 失败: %v", err)
	}
	got, err := st.Job.Get(app.ID, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.StartTime != nil {
		t.Fatalf("start_time 应被清空(nil), got %v", got.StartTime)
	}
}

// Trigger:经 SubmitManual 产生 queued 实例。
func TestJobTrigger(t *testing.T) {
	st, sch, il := newSvc(t)
	jobSvc := NewJobService(st, sch)
	insSvc := NewInstanceService(st, sch, il, dispatch.NoopCallbackBuilder{})
	appSvc := NewAppService(st)
	app, _ := appSvc.Create("a", "p", 0)
	job := &domain.Job{AppID: app.ID, Name: "j", ExecuteType: "http", ScheduleKind: "api", Enabled: true}
	_ = jobSvc.Create(job)

	// 手动触发需要一个在线 worker 让 dispatcher 派出去;这里只验证 SubmitManual 落库 queued
	mw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer mw.Close()
	// 注意:此 sch 的 reg 无 worker,SubmitManual 仍会落 queued 实例
	if err := jobSvc.Trigger(app.ID, job.ID, 5, "iparams"); err != nil {
		t.Fatal(err)
	}
	var queued []domain.Instance
	st.DB.Where("status = ? AND trigger_type = ?", domain.StatusQueued, "manual").Find(&queued)
	if len(queued) != 1 {
		t.Fatalf("应 1 个 queued 手动实例, got %d", len(queued))
	}
	if queued[0].JobInstanceParams != "iparams" || queued[0].Priority != 5 {
		t.Fatalf("实例参数/优先级不符: %+v", queued[0])
	}
	_ = insSvc
}

// ReportStatus:终态守护 + 白名单;终态时 ReleaseInFlight(幂等,无槽也安全)。
func TestInstanceReportStatus(t *testing.T) {
	st, sch, il := newSvc(t)
	insSvc := NewInstanceService(st, sch, il, dispatch.NoopCallbackBuilder{})
	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	ins := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusRunning}
	_ = st.Instance.Create(ins)

	// 非法 status 拒绝
	if err := insSvc.ReportStatus(ins.ID, "hacked", ""); err == nil {
		t.Fatal("非法 status 应拒绝")
	}
	// running → success
	if err := insSvc.ReportStatus(ins.ID, domain.StatusSuccess, "ok"); err != nil {
		t.Fatal(err)
	}
	// 终态守护:success 后报 failed 不覆盖
	_ = insSvc.ReportStatus(ins.ID, domain.StatusFailed, "late")
	got, _ := st.Instance.Get(ins.ID)
	if got.Status != domain.StatusSuccess {
		t.Fatalf("终态不应被覆盖, got %s", got.Status)
	}
}

// Update:调度字段变化时重算 next_run(api→cron、禁用→启用 否则会永不触发);
// 非调度字段更新不重算;非法 cron 报错。
func TestJobUpdateReschedules(t *testing.T) {
	st, sch, _ := newSvc(t)
	jobSvc := NewJobService(st, sch)
	appSvc := NewAppService(st)
	app, _ := appSvc.Create("a", "p", 0)

	// api+enabled 创建 → 无 next_run
	j := &domain.Job{AppID: app.ID, Name: "j", ExecuteType: "http", ScheduleKind: "api", Enabled: true}
	if err := jobSvc.Create(j); err != nil {
		t.Fatal(err)
	}
	if j.NextRunTime != nil {
		t.Fatal("api 应无 next_run_time")
	}

	// 改成 cron → 应重算出 next_run(否则永不触发)
	cron := "*/1 * * * *"
	if err := jobSvc.Update(app.ID, j.ID, map[string]any{
		"schedule_kind": "cron", "schedule_expr": cron,
	}); err != nil {
		t.Fatalf("Update 失败: %v", err)
	}
	got, _ := jobSvc.Get(app.ID, j.ID)
	if got.ScheduleKind != "cron" || got.ScheduleExpr != cron {
		t.Fatalf("调度字段未更新: %+v", got)
	}
	if got.NextRunTime == nil {
		t.Fatal("改成 cron 后应重算出 next_run_time")
	}

	// 禁用 → next_run 清空
	if err := jobSvc.Update(app.ID, j.ID, map[string]any{"enabled": false}); err != nil {
		t.Fatalf("Update disable 失败: %v", err)
	}
	if g, _ := jobSvc.Get(app.ID, j.ID); g.NextRunTime != nil {
		t.Fatalf("禁用后 next_run 应清空, got %v", g.NextRunTime)
	}

	// 重新启用 → 又应有 next_run
	if err := jobSvc.Update(app.ID, j.ID, map[string]any{"enabled": true}); err != nil {
		t.Fatalf("Update enable 失败: %v", err)
	}
	got3, _ := jobSvc.Get(app.ID, j.ID)
	if got3.NextRunTime == nil {
		t.Fatal("重新启用后应重算出 next_run_time")
	}

	// 非调度字段更新不重算 next_run(保持启用时的值)
	before := *got3.NextRunTime
	if err := jobSvc.Update(app.ID, j.ID, map[string]any{"timeout_sec": 30}); err != nil {
		t.Fatalf("Update timeout 失败: %v", err)
	}
	got4, _ := jobSvc.Get(app.ID, j.ID)
	if got4.TimeoutSec != 30 {
		t.Fatalf("timeout_sec 应=30, got %d", got4.TimeoutSec)
	}
	if got4.NextRunTime == nil || !got4.NextRunTime.Equal(before) {
		t.Fatalf("非调度字段更新不应重算 next_run, before=%v after=%v", before, got4.NextRunTime)
	}

	// 非法 cron → 报错
	if err := jobSvc.Update(app.ID, j.ID, map[string]any{"schedule_expr": "not-a-cron"}); err == nil {
		t.Fatal("非法 cron 应报错")
	}
}
