package dispatch

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"task-schedule/internal/domain"
	"task-schedule/internal/instancelog"
	"task-schedule/internal/workerreg"
)

func testLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestReaperUnboundQueued 验证 reaper 扫描 worker 无法处理已解绑的实例。
func TestReaperUnboundQueued(t *testing.T) {
	st := newTestStore(t)

	reg := workerreg.New(time.Minute, nil)
	il := instancelog.New(t.TempDir(), 0)
	exec := &fakeExecutor{pick: true}
	sch := NewScheduler(st, exec, il, 50*time.Millisecond, testLog(), NoopCallbackBuilder{})

	// 创建 app + job
	app := &domain.App{AppName: "test", Password: "p"}
	_ = st.App.Create(app)
	job := &domain.Job{AppID: app.ID, Name: "job1", ExecuteType: "http", RetryCount: 1, RetryIntervalSec: 1}
	_ = st.Job.Create(job)

	// 场景 1：auto 触发实例 worker 回报 queued 解绑，超 30s 后被 reaper 转 failed
	ins1 := &domain.Instance{
		JobID: job.ID, AppID: app.ID,
		Status:        domain.StatusWaitingReceive,
		WorkerAddress: "worker1:9000",
		TriggerType:   "auto",
		TriggerTime:   time.Now().Add(-35 * time.Second), // 35s 前触发
	}
	now := time.Now().Add(-32 * time.Second)
	ins1.StartTime = &now
	_ = st.Instance.Create(ins1)
	// 模拟 worker 回报 queued（解绑）
	_ = st.Instance.UpdateResult(ins1.ID, domain.StatusQueued, "资源不足")
	// 手动改 updated_at 到 32s 前（模拟超过 30s 阈值）
	_ = st.DB.Model(&domain.Instance{}).Where("id = ?", ins1.ID).Update("updated_at", time.Now().Add(-32*time.Second))

	// 场景 2：manual 触发排队实例（等并发槽，worker_address=null），不应被 reaper 捞
	// （手动排队 updated_at 不刷新，reaper 误杀是回归点；trigger_type=manual 被排除）
	ins2 := &domain.Instance{
		JobID: job.ID, AppID: app.ID,
		Status:      domain.StatusQueued,
		TriggerType: "manual",
		TriggerTime: time.Now(),
	}
	_ = st.Instance.Create(ins2)

	// 场景 3：刚解绑的实例（10s 前），不应被 reaper 捞（阈值 30s）
	ins3 := &domain.Instance{
		JobID: job.ID, AppID: app.ID,
		Status:        domain.StatusQueued,
		WorkerAddress: "",
		TriggerType:   "auto",
		TriggerTime:   time.Now().Add(-12 * time.Second),
	}
	_ = st.Instance.Create(ins3)
	_ = st.DB.Model(&domain.Instance{}).Where("id = ?", ins3.ID).Update("updated_at", time.Now().Add(-10*time.Second))

	// 执行 reaper
	sch.reapOnce(reg)

	// 验证：ins1 被转 failed + 设置 next_retry_time
	got1, _ := st.Instance.Get(ins1.ID)
	if got1.Status != domain.StatusFailed {
		t.Errorf("ins1 应被 reaper 转 failed, got %s", got1.Status)
	}
	if got1.Result != "worker 无法处理已解绑" {
		t.Errorf("ins1 result 应记录 reaper 原因, got %s", got1.Result)
	}
	if got1.NextRetryTime == nil {
		t.Error("ins1 应设置 next_retry_time（job.RetryCount>0）")
	}

	// 验证：ins2 不受影响（正常排队）
	got2, _ := st.Instance.Get(ins2.ID)
	if got2.Status != domain.StatusQueued {
		t.Errorf("ins2 应仍为 queued, got %s", got2.Status)
	}

	// 验证：ins3 不受影响（刚解绑，未超阈值）
	got3, _ := st.Instance.Get(ins3.ID)
	if got3.Status != domain.StatusQueued {
		t.Errorf("ins3 应仍为 queued, got %s", got3.Status)
	}
}

// TestReaperUnboundQueuedNoRetry 验证无重试配置的实例被定格 failed。
func TestReaperUnboundQueuedNoRetry(t *testing.T) {
	st := newTestStore(t)

	reg := workerreg.New(time.Minute, nil)
	il := instancelog.New(t.TempDir(), 0)
	exec := &fakeExecutor{pick: true}
	sch := NewScheduler(st, exec, il, 50*time.Millisecond, testLog(), NoopCallbackBuilder{})

	app := &domain.App{AppName: "test", Password: "p"}
	_ = st.App.Create(app)
	// RetryCount=0：无重试
	job := &domain.Job{AppID: app.ID, Name: "job1", ExecuteType: "http", RetryCount: 0}
	_ = st.Job.Create(job)

	ins := &domain.Instance{
		JobID: job.ID, AppID: app.ID,
		Status:        domain.StatusQueued,
		WorkerAddress: "",
		TriggerType:   "auto",
		TriggerTime:   time.Now().Add(-35 * time.Second),
	}
	_ = st.Instance.Create(ins)
	_ = st.DB.Model(&domain.Instance{}).Where("id = ?", ins.ID).Update("updated_at", time.Now().Add(-32*time.Second))

	sch.reapOnce(reg)

	got, _ := st.Instance.Get(ins.ID)
	if got.Status != domain.StatusFailed {
		t.Errorf("无重试配置应定格 failed, got %s", got.Status)
	}
	if got.NextRetryTime != nil {
		t.Error("无重试配置不应设置 next_retry_time")
	}
}

// TestListUnboundQueued 验证仓储方法过滤逻辑。
func TestListUnboundQueued(t *testing.T) {
	st := newTestStore(t)

	app := &domain.App{AppName: "test", Password: "p"}
	_ = st.App.Create(app)
	job := &domain.Job{AppID: app.ID, Name: "job1", ExecuteType: "http"}
	_ = st.Job.Create(job)

	// 符合条件：queued + trigger_type=auto + worker_address=null + updated_at 超 30s
	ins1 := &domain.Instance{JobID: job.ID, AppID: app.ID, Status: domain.StatusQueued, WorkerAddress: "", TriggerType: "auto"}
	_ = st.Instance.Create(ins1)
	_ = st.DB.Model(&domain.Instance{}).Where("id = ?", ins1.ID).Update("updated_at", time.Now().Add(-35*time.Second))

	// 不符合：status 不是 queued
	ins2 := &domain.Instance{JobID: job.ID, AppID: app.ID, Status: domain.StatusRunning, WorkerAddress: ""}
	_ = st.Instance.Create(ins2)
	_ = st.DB.Model(&domain.Instance{}).Where("id = ?", ins2.ID).Update("updated_at", time.Now().Add(-35*time.Second))

	// 不符合：worker_address 非空
	ins3 := &domain.Instance{JobID: job.ID, AppID: app.ID, Status: domain.StatusQueued, WorkerAddress: "w:9"}
	_ = st.Instance.Create(ins3)
	_ = st.DB.Model(&domain.Instance{}).Where("id = ?", ins3.ID).Update("updated_at", time.Now().Add(-35*time.Second))

	// 不符合：updated_at 未超阈值
	ins4 := &domain.Instance{JobID: job.ID, AppID: app.ID, Status: domain.StatusQueued, WorkerAddress: ""}
	_ = st.Instance.Create(ins4)
	_ = st.DB.Model(&domain.Instance{}).Where("id = ?", ins4.ID).Update("updated_at", time.Now().Add(-10*time.Second))

	// 不符合：trigger_type=manual（手动排队等槽，updated_at 不刷新但 reaper 不应误杀）
	ins5 := &domain.Instance{JobID: job.ID, AppID: app.ID, Status: domain.StatusQueued, WorkerAddress: "", TriggerType: "manual"}
	_ = st.Instance.Create(ins5)
	_ = st.DB.Model(&domain.Instance{}).Where("id = ?", ins5.ID).Update("updated_at", time.Now().Add(-35*time.Second))

	list, err := st.Instance.ListUnboundQueued(30 * time.Second)
	if err != nil {
		t.Fatalf("ListUnboundQueued 失败: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("应返回 1 个实例, got %d", len(list))
	}
	if list[0].ID != ins1.ID {
		t.Errorf("应返回 ins1, got %d", list[0].ID)
	}
}

// TestReaperWarmupSkipsWorkerOffline 验证 reaper 启动宽限:服务重启后 workerreg 尚空、worker 未及
// 重新心跳注册,此期间 reaper 不应把"重启前在飞"的实例判为 worker 失联(否则失败转移 → 重复执行)。
// warmup 关闭后恢复正常失联判定。
func TestReaperWarmupSkipsWorkerOffline(t *testing.T) {
	st := newTestStore(t)
	reg := workerreg.New(time.Minute, nil) // 空 reg:任何 worker 都判"不在线",模拟重启后 worker 未回心
	il := instancelog.New(t.TempDir(), 0)
	exec := &fakeExecutor{pick: true}
	sch := NewScheduler(st, exec, il, 50*time.Millisecond, testLog(), NoopCallbackBuilder{})
	sch.SetReaperWarmup(10 * time.Second) // warmup 窗口内

	app := &domain.App{AppName: "warmup", Password: "p"}
	_ = st.App.Create(app)
	job := &domain.Job{AppID: app.ID, Name: "j", ExecuteType: "http", RetryCount: 0}
	_ = st.Job.Create(job)

	// 重启前在飞的实例:waiting_receive + 已绑定 worker,但该 worker 重启后还没重新心跳(reg 里没有)
	stime := time.Now().Add(-30 * time.Second)
	ins := &domain.Instance{
		JobID: job.ID, AppID: app.ID,
		Status:        domain.StatusWaitingReceive,
		WorkerAddress: "10.0.0.5:9000",
		TriggerType:   "auto",
		TriggerTime:   stime,
		StartTime:     &stime,
	}
	_ = st.Instance.Create(ins)

	// warmup 内:reaper 不应判 worker 失联 → 实例仍 waiting_receive(避免重启误杀)
	sch.reapOnce(reg)
	if got, _ := st.Instance.Get(ins.ID); got.Status != domain.StatusWaitingReceive {
		t.Fatalf("warmup 内不应判 worker 失联(避免重启误杀), got %s", got.Status)
	}

	// warmup 关闭后:同样的实例应被 reaper 判失联 → failed
	sch.SetReaperWarmup(0)
	sch.reapOnce(reg)
	if got, _ := st.Instance.Get(ins.ID); got.Status != domain.StatusFailed {
		t.Fatalf("warmup 关闭后应判 worker 失联 → failed, got %s", got.Status)
	}
}

// TestReaperExecutionTimeout 验证 reaper 把"在线 worker 上执行超 job.TimeoutSec"的实例标 timeout
// (区别于 worker 失联/未绑定 → failed),且配了 RetryCount 时设 next_retry_time 可重试。
func TestReaperExecutionTimeout(t *testing.T) {
	st := newTestStore(t)
	reg := workerreg.New(time.Minute, nil)
	il := instancelog.New(t.TempDir(), 0)
	exec := &fakeExecutor{pick: true}
	sch := NewScheduler(st, exec, il, 50*time.Millisecond, testLog(), NoopCallbackBuilder{})

	app := &domain.App{AppName: "t", Password: "p"}
	_ = st.App.Create(app)
	// TimeoutSec=2 + RetryCount=1:在线 worker 上跑超时应转 timeout 且可重试
	job := &domain.Job{AppID: app.ID, Name: "j", ExecuteType: "http", TimeoutSec: 2, RetryCount: 1, RetryIntervalSec: 1}
	_ = st.Job.Create(job)

	// 注册在线 worker(reaper 不判失联,从而落到 TimeoutSec 判定)
	reg.Heartbeat(workerreg.WorkerInfo{AppID: app.ID, WorkerAddress: "10.0.0.1:9000"})

	// 实例已派发、绑定在线 worker,StartTime 早于 now-TimeoutSec(已超时)
	stime := time.Now().Add(-10 * time.Second)
	ins := &domain.Instance{
		JobID: job.ID, AppID: app.ID,
		Status:        domain.StatusRunning,
		WorkerAddress: "10.0.0.1:9000",
		TriggerType:   "auto",
		TriggerTime:   stime,
		StartTime:     &stime,
	}
	_ = st.Instance.Create(ins)

	sch.reapOnce(reg)

	got, _ := st.Instance.Get(ins.ID)
	if got.Status != domain.StatusTimeout {
		t.Fatalf("执行超时应→timeout, got %s", got.Status)
	}
	if !strings.Contains(got.Result, "执行超时") {
		t.Errorf("result 应注明执行超时, got %s", got.Result)
	}
	if got.NextRetryTime == nil {
		t.Error("配了 RetryCount 的 timeout 实例应设 next_retry_time(可重试)")
	}
}

// TestGoTrackRecoversAndRestarts 验证 goTrack 的 panic 自愈:fn 首次 panic 后应 recover 并自动重启,
// 不崩进程、不静默停摆。第二次执行正常(等 ctx 取消退出)。
func TestGoTrackRecoversAndRestarts(t *testing.T) {
	st := newTestStore(t)
	il := instancelog.New(t.TempDir(), 0)
	sch := NewScheduler(st, New(workerreg.New(time.Minute, nil), time.Second), il, 50*time.Millisecond, testLog(), NoopCallbackBuilder{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var count int32
	done := make(chan struct{})
	sch.goTrack(func() {
		if atomic.AddInt32(&count, 1) == 1 {
			panic("首次执行 panic,应被自愈后重启")
		}
		<-ctx.Done() // 第二次起正常:等 ctx 取消后退出
		close(done)
	})

	// panic 后 runTracked sleep 1s 再重启,3s 内应观察到 count>=2(说明已重启)。
	waitFor(t, 3*time.Second, func() bool { return atomic.LoadInt32(&count) >= 2 }, "goTrack 应在 panic 后自愈重启")

	cancel() // 触发第二次 fn 正常 return → goroutine 退出
	<-done
	sch.Wait()
}

// 辅助：fake Executor
type fakeExecutor struct {
	pick bool
}

func (f *fakeExecutor) PickWorker(*domain.Job, *domain.Instance) (string, string, bool) {
	if !f.pick {
		return "", "", false
	}
	return "fake:9000", "http", true
}

func (f *fakeExecutor) Send(context.Context, string, string, *domain.Job, *domain.Instance) error {
	return nil
}
