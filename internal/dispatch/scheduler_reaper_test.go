package dispatch

import (
	"context"
	"io"
	"log/slog"
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
