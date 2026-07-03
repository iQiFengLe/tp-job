package dispatch

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"

	"task-schedule/internal/domain"
	"task-schedule/internal/instancelog"
	"task-schedule/internal/repository"
	"task-schedule/internal/workerreg"
)

func newTestStore(t *testing.T) *repository.Store {
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
	return st
}

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func waitFor(t *testing.T, timeout time.Duration, fn func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", msg)
}

// 到期 job → 选 worker → POST run → 实例 waiting_receive,绑定 worker 地址。
func TestSchedulerDispatchesToWorker(t *testing.T) {
	st := newTestStore(t)
	var mu sync.Mutex
	var gotBody map[string]any
	mw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		_ = json.Unmarshal(body, &gotBody)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer mw.Close()

	reg := workerreg.New(time.Minute, nil)
	reg.Heartbeat(workerreg.WorkerInfo{
		AppID: 1, WorkerAddress: mw.Listener.Addr().String(),
		Metrics: domain.SystemMetrics{Score: 1}, Protocol: workerreg.ProtocolHTTP,
		AcceptNotTagJob: true, // 接受任意 tag,匹配 job.Tag="t1"
	})

	il := instancelog.New(t.TempDir(), 0)
	sch := NewScheduler(st, New(reg, time.Second), il, 50*time.Millisecond, discardLog())

	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	now := time.Now()
	job := &domain.Job{AppID: 1, Name: "j", ExecuteType: "http", JobParams: "p1", Tag: "t1",
		ScheduleKind: "cron", ScheduleExpr: "*/1 * * * *", NextRunTime: &now}
	if err := st.Job.Create(job); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sch.Run(ctx)

	// 等实例出现并为 waiting_receive
	var insID int64
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		_, ok := gotBody["jobId"]
		return ok
	}, "等待 worker 收到 runJob")

	// DB 里应有 waiting_receive 实例,绑定 worker。
	// dispatch 的"POST 成功 → UpdateStatus(waiting_receive)"与 worker 收到 POST 之间存在窗口,
	// 单次查询会撞上中间态(running/queued)致 flaky,故用 waitFor 轮询全部断言条件。
	var list []domain.Instance
	waitFor(t, 3*time.Second, func() bool {
		list, _ = st.Instance.ListGeneralizedActive(0)
		return len(list) == 1 && list[0].Status == domain.StatusWaitingReceive &&
			list[0].WorkerAddress != "" && list[0].Tag == "t1"
	}, "应 1 个 waiting_receive 实例并绑 worker+tag")
	insID = list[0].ID
	_ = insID
}

// 定时触发串行:实例在飞(未回报终态)时,下次到期跳过(不推进游标),不会产生第 2 个实例。
func TestSchedulerCronSerial(t *testing.T) {
	st := newTestStore(t)
	mw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer mw.Close()

	reg := workerreg.New(time.Minute, nil)
	reg.Heartbeat(workerreg.WorkerInfo{AppID: 1, WorkerAddress: mw.Listener.Addr().String(),
		Metrics: domain.SystemMetrics{Score: 1}, Protocol: workerreg.ProtocolHTTP})

	il := instancelog.New(t.TempDir(), 0)
	sch := NewScheduler(st, New(reg, time.Second), il, 50*time.Millisecond, discardLog())

	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	now := time.Now()
	past := now.Add(-time.Second)
	// fix_rate 100ms:多处到期,但首个未释放 → 后续跳过(不推进游标);释放后继续派发
	job := &domain.Job{AppID: 1, Name: "j", ExecuteType: "http", JobParams: "p",
		ScheduleKind: "fix_rate", ScheduleExpr: "100", NextRunTime: &past}
	if err := st.Job.Create(job); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sch.Run(ctx)

	// 跑多个 tick,期间实例始终在飞(worker 不回报),应只产生 1 个实例
	time.Sleep(350 * time.Millisecond)
	list, _ := st.Instance.ListGeneralizedActive(0)
	if len(list) != 1 {
		t.Fatalf("串行期应只 1 个在飞实例, got %d", len(list))
	}

	// 模拟 worker 回报终态释放槽
	sch.ReleaseInFlight(list[0].ID)
	time.Sleep(20 * time.Millisecond)
	waitFor(t, 2*time.Second, func() bool {
		l, _ := st.Instance.ListGeneralizedActive(0)
		return len(l) >= 2 // 释放后下个 tick 又派一个
	}, "释放后应继续派发")
}

// 派发失败(无 worker)→ 实例 failed,不持有槽。
func TestSchedulerNoWorkerFailsInstance(t *testing.T) {
	st := newTestStore(t)
	// 不注册任何 worker
	reg := workerreg.New(time.Minute, nil)
	il := instancelog.New(t.TempDir(), 0)
	sch := NewScheduler(st, New(reg, time.Second), il, 50*time.Millisecond, discardLog())

	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	now := time.Now()
	job := &domain.Job{AppID: 1, Name: "j", ExecuteType: "http", JobParams: "p",
		ScheduleKind: "cron", ScheduleExpr: "*/1 * * * *", NextRunTime: &now}
	_ = st.Job.Create(job)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sch.Run(ctx)

	waitFor(t, 2*time.Second, func() bool {
		var failed []domain.Instance
		st.DB.Where("status = ?", domain.StatusFailed).Find(&failed)
		return len(failed) >= 1
	}, "无 worker 应落 failed 实例")
}

// reaper:worker 失联(心跳超时)后,其 waiting_receive 实例被标记 failed。
func TestReaperWorkerGone(t *testing.T) {
	st := newTestStore(t)
	reg := workerreg.New(20*time.Millisecond, nil) // 短心跳超时
	il := instancelog.New(t.TempDir(), 0)
	sch := NewScheduler(st, New(reg, time.Second), il, 50*time.Millisecond, discardLog())

	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	_ = st.Job.Create(&domain.Job{ID: 1, AppID: 1, Name: "j", ExecuteType: "http", TimeoutSec: 0})
	// 直接造一个 waiting_receive 实例,绑定一个将超时的 worker
	reg.Heartbeat(workerreg.WorkerInfo{AppID: 1, WorkerAddress: "9.9.9.9:9", Metrics: domain.SystemMetrics{Score: 1}})
	ins := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusWaitingReceive, WorkerAddress: "9.9.9.9:9"}
	_ = st.Instance.Create(ins)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sch.RunInstanceReaper(ctx, reg)

	waitFor(t, 2*time.Second, func() bool {
		got, err := st.Instance.Get(ins.ID)
		return err == nil && got.Status == domain.StatusFailed
	}, "失联 worker 的实例应被 reaper 标 failed")
}

// RetryPump:failed 实例设 next_retry_time 到期后,产生 retry_index+1 的重试实例。
func TestRetryPump(t *testing.T) {
	st := newTestStore(t)
	mw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer mw.Close()

	reg := workerreg.New(time.Minute, nil)
	reg.Heartbeat(workerreg.WorkerInfo{AppID: 1, WorkerAddress: mw.Listener.Addr().String(),
		Metrics: domain.SystemMetrics{Score: 1}, Protocol: workerreg.ProtocolHTTP, AcceptNotTagJob: true})

	il := instancelog.New(t.TempDir(), 0)
	sch := NewScheduler(st, New(reg, time.Second), il, 50*time.Millisecond, discardLog())

	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	_ = st.Job.Create(&domain.Job{ID: 1, AppID: 1, Name: "j", ExecuteType: "http", RetryCount: 1, RetryIntervalSec: 1})

	// 一个 failed 实例,重试时间已到期
	orig := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusFailed, RetryIndex: 0}
	_ = st.Instance.Create(orig)
	_ = st.Instance.SetNextRetryTime(orig.ID, time.Now().Add(-time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sch.RunRetryPump(ctx)

	waitFor(t, 3*time.Second, func() bool {
		var retry []domain.Instance
		st.DB.Where("retry_index = ?", 1).Find(&retry)
		return len(retry) >= 1
	}, "应产生 retry_index=1 的重试实例")

	var retryIns domain.Instance
	if err := st.DB.Where("retry_index = ?", 1).First(&retryIns).Error; err != nil {
		t.Fatal("重试实例应存在")
	}
	if retryIns.RootInstanceID != orig.ID {
		t.Fatalf("重试实例 RootInstanceID 应=链首(orig=%d), got %d", orig.ID, retryIns.RootInstanceID)
	}
}

// RecoverQueued:重启前残留的 queued 实例,启动恢复后重新入队并被派发。
// 覆盖"pq 纯内存、重启即丢、queued 无其他兜底"的修复。
func TestRecoverQueued(t *testing.T) {
	st := newTestStore(t)
	mw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer mw.Close()

	reg := workerreg.New(time.Minute, nil)
	reg.Heartbeat(workerreg.WorkerInfo{AppID: 1, WorkerAddress: mw.Listener.Addr().String(),
		Metrics: domain.SystemMetrics{Score: 1}, Protocol: workerreg.ProtocolHTTP, AcceptNotTagJob: true})

	il := instancelog.New(t.TempDir(), 0)
	sch := NewScheduler(st, New(reg, time.Second), il, 50*time.Millisecond, discardLog())

	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	// MaxConcurrency=5 保证两个实例都能拿到槽(测试 worker 不回报终态,不释放槽)
	_ = st.Job.Create(&domain.Job{ID: 1, AppID: 1, Name: "j", ExecuteType: "http", MaxConcurrency: 5})

	// 模拟重启前残留的 queued 手动实例(不同优先级)
	_ = st.Instance.Create(&domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusQueued, TriggerType: "manual", Priority: 1})
	_ = st.Instance.Create(&domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusQueued, TriggerType: "manual", Priority: 5})

	// 恢复前 pq 为空;恢复后应含 2 个
	if n := sch.pendingLen(); n != 0 {
		t.Fatalf("恢复前 pq 应为空, got %d", n)
	}
	if err := sch.RecoverQueued(); err != nil {
		t.Fatalf("恢复失败: %v", err)
	}
	if n := sch.pendingLen(); n != 2 {
		t.Fatalf("应恢复 2 个 queued 实例, got %d", n)
	}

	// 启动手动派发器,恢复的实例应都被派发为 waiting_receive
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sch.RunManualDispatcher(ctx)

	waitFor(t, 3*time.Second, func() bool {
		list, _ := st.Instance.ListGeneralizedActive(0)
		return len(list) == 2
	}, "恢复的 2 个 queued 实例应都被派发")
}

// RecoverStaleActive:重启前未终结的实例,启动清理后 failed;配了重试的设 next_retry_time 交
// RetryPump 重派(重启不丢),取代旧 MarkStaleActiveAsFailed 的"只标 failed 不重试"。
func TestRecoverStaleActiveRetry(t *testing.T) {
	st := newTestStore(t)
	il := instancelog.New(t.TempDir(), 0)
	sch := NewScheduler(st, New(workerreg.New(time.Minute, nil), time.Second), il,
		50*time.Millisecond, discardLog())

	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	// job 配了 1 次重试
	_ = st.Job.Create(&domain.Job{ID: 1, AppID: 1, Name: "j", ExecuteType: "http",
		RetryCount: 1, RetryIntervalSec: 1})
	// 重启前未终结的实例
	ins := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusWaitingReceive,
		WorkerAddress: "1.2.3.4:5", RetryIndex: 0}
	_ = st.Instance.Create(ins)

	if err := sch.RecoverStaleActive(); err != nil {
		t.Fatalf("RecoverStaleActive 失败: %v", err)
	}
	got, _ := st.Instance.Get(ins.ID)
	if got.Status != domain.StatusFailed {
		t.Fatalf("应标 failed, got %s", got.Status)
	}
	if got.NextRetryTime == nil {
		t.Fatal("有重试余力应设 next_retry_time(交 RetryPump 重派,重启不丢)")
	}
}

// retryInstance 派发失败(无 worker)若仍有重试余力,应继续重试(交 RetryPump),而非定格 failed。
// 回归:原实现派发失败直接 failed 终态,重试链因一次瞬时派发失败被提前耗光(停在 retry_index=1)。
func TestRetryDispatchFailContinues(t *testing.T) {
	st := newTestStore(t)
	reg := workerreg.New(time.Minute, nil) // 无 worker → 每次派发必失败
	il := instancelog.New(t.TempDir(), 0)
	sch := NewScheduler(st, New(reg, time.Second), il, 50*time.Millisecond, discardLog())

	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	// RetryCount=2:允许 0→1→2
	_ = st.Job.Create(&domain.Job{ID: 1, AppID: 1, Name: "j", ExecuteType: "http",
		RetryCount: 2, RetryIntervalSec: 1})

	orig := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusFailed, RetryIndex: 0}
	_ = st.Instance.Create(orig)
	_ = st.Instance.SetNextRetryTime(orig.ID, time.Now().Add(-time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sch.RunRetryPump(ctx)

	// 应推进到 retry_index=2(证明 index=1 派发失败后仍被重试;未修复时停在 1)
	waitFor(t, 8*time.Second, func() bool {
		var r2 []domain.Instance
		st.DB.Where("retry_index = ?", 2).Find(&r2)
		return len(r2) >= 1
	}, "派发失败应继续重试至 retry_index=2")
}

// 此处补 auto/retry:模拟 Create(queued) 与 Dispatch 之间崩溃残留的实例,启动后应被恢复派发)。
func TestRecoverQueuedAllTypes(t *testing.T) {
	st := newTestStore(t)
	mw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer mw.Close()
	reg := workerreg.New(time.Minute, nil)
	reg.Heartbeat(workerreg.WorkerInfo{AppID: 1, WorkerAddress: mw.Listener.Addr().String(),
		Metrics: domain.SystemMetrics{Score: 1}, Protocol: workerreg.ProtocolHTTP, AcceptNotTagJob: true})

	il := instancelog.New(t.TempDir(), 0)
	sch := NewScheduler(st, New(reg, time.Second), il, 50*time.Millisecond, discardLog())

	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	_ = st.Job.Create(&domain.Job{ID: 1, AppID: 1, Name: "j", ExecuteType: "http", MaxConcurrency: 5})

	// auto/retry 触发路径崩溃残留的 queued 实例(旧 RecoverManualQueued 不捞,会永久滞留)
	_ = st.Instance.Create(&domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusQueued, TriggerType: "auto"})
	_ = st.Instance.Create(&domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusQueued,
		TriggerType: "retry", RetryIndex: 1})

	if err := sch.RecoverQueued(); err != nil {
		t.Fatalf("RecoverQueued 失败: %v", err)
	}
	if n := sch.pendingLen(); n != 2 {
		t.Fatalf("应恢复 2 个(auto+retry) queued 实例, got %d", n)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sch.RunManualDispatcher(ctx)
	waitFor(t, 3*time.Second, func() bool {
		list, _ := st.Instance.ListGeneralizedActive(0)
		return len(list) == 2
	}, "auto/retry 的 queued 实例应都被恢复派发")
}

// SubmitManualDelayed:返回 instanceID;delay=0 立即入队;delay>0 立即返回 ID 但延迟入队(对齐 OpenAPI runJob)。
func TestSubmitManualDelayed(t *testing.T) {
	st := newTestStore(t)
	reg := workerreg.New(time.Minute, nil)
	il := instancelog.New(t.TempDir(), 0)
	sch := NewScheduler(st, New(reg, time.Second), il, 50*time.Millisecond, discardLog())
	t.Cleanup(func() { sch.stopTimers() }) // 取消 delay>0 的入队 timer,防测试结束后悬挂回调
	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	_ = st.Job.Create(&domain.Job{ID: 1, AppID: 1, Name: "j"})
	job, _ := st.Job.Get(1, 1)

	// delay=0:立即返回 ID 并入队
	id, err := sch.SubmitManualDelayed(job, 0, "p", 0)
	if err != nil || id <= 0 {
		t.Fatalf("应返回正 ID, err=%v", err)
	}
	if n := sch.pendingLen(); n != 1 {
		t.Fatalf("delay=0 应立即入队(1), got %d", n)
	}

	// delay>0:立即返回 ID + 落库 queued,但不立即入队
	id2, err := sch.SubmitManualDelayed(job, 0, "p", time.Hour)
	if err != nil || id2 <= 0 {
		t.Fatalf("应返回正 ID, err=%v", err)
	}
	ins, _ := st.Instance.Get(id2)
	if ins == nil || ins.Status != domain.StatusQueued {
		t.Fatalf("delay 实例应已落库 queued: %+v", ins)
	}
	if n := sch.pendingLen(); n != 1 {
		t.Fatalf("delay>0 不应立即入队, pendingLen 应仍 1, got %d", n)
	}

	// 旧 SubmitManual 仍可用(返回 error)
	if err := sch.SubmitManual(job, 0, "p"); err != nil {
		t.Fatalf("SubmitManual 应可用: %v", err)
	}
}

// runManualHeld 派发前查 DB:延迟入队期间被 cancel 的实例,到点不应被派发。
// 回归:内存 manualItem.ins 是入队快照,原实现 runManualHeld 不查 DB,会把已取消实例 Dispatch
// (无 worker → failed)。修复后派发前查 DB 见终态 → 跳过派发 + releaseByJob 归还槽,实例保持 canceled。
func TestCancelDelayedNotDispatched(t *testing.T) {
	st := newTestStore(t)
	reg := workerreg.New(time.Minute, nil)
	il := instancelog.New(t.TempDir(), 0)
	sch := NewScheduler(st, New(reg, time.Second), il, 50*time.Millisecond, discardLog())
	t.Cleanup(func() { sch.stopTimers() })
	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	_ = st.Job.Create(&domain.Job{ID: 1, AppID: 1, Name: "j", ExecuteType: "http", MaxConcurrency: 1})
	job, _ := st.Job.Get(1, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sch.RunManualDispatcher(ctx)

	// delay=200ms 入队;立即 cancel → 到点 runManualHeld 应查 DB 见终态、不派发
	id, err := sch.SubmitManualDelayed(job, 0, "", 200*time.Millisecond)
	if err != nil || id <= 0 {
		t.Fatalf("SubmitManualDelayed 应返回正 ID, err=%v", err)
	}
	if err := st.Instance.SetStatus(id, domain.StatusCanceled, "cancel"); err != nil {
		t.Fatalf("SetStatus 失败: %v", err)
	}

	// 等延迟入队 + 派发器处理(未修复时实例会被 Dispatch → failed,状态偏离 canceled)
	time.Sleep(500 * time.Millisecond)
	ins, _ := st.Instance.Get(id)
	if ins.Status != domain.StatusCanceled {
		t.Fatalf("已 cancel 的延迟实例不应被派发(应保持 canceled), got %s", ins.Status)
	}
}
