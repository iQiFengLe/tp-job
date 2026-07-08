package repository

import (
	"testing"
	"time"

	"task-schedule/internal/domain"
)

// TestInstanceDurationOnTerminal 验证终态写入时计算 duration_ms = end - start_time。
func TestInstanceDurationOnTerminal(t *testing.T) {
	st := newTestStore(t)
	start := time.Now().Add(-2 * time.Second)
	ins := &domain.Instance{
		AppID: 1, JobID: 1, Status: domain.StatusRunning,
		StartTime: &start, TriggerTime: start.Add(-time.Second),
	}
	if err := st.Instance.Create(ins); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := st.Instance.UpdateResult(ins.ID, domain.StatusSuccess, "ok"); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, err := st.Instance.Get(ins.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.StatusSuccess {
		t.Fatalf("status=%s want success", got.Status)
	}
	if got.EndTime == nil {
		t.Fatal("end_time 应在终态写入")
	}
	// start_time 在 2s 前 → duration 约 2000ms,给 [1500,10000] 抗抖动。
	if got.DurationMS < 1500 || got.DurationMS > 10000 {
		t.Fatalf("duration_ms=%d 期望约 2000", got.DurationMS)
	}
}

// TestInstanceDurationFallsBackToTriggerTime 验证 start_time 为空(异常/迁移脏数据)退到 trigger_time。
func TestInstanceDurationFallsBackToTriggerTime(t *testing.T) {
	st := newTestStore(t)
	ins := &domain.Instance{
		AppID: 1, JobID: 1, Status: domain.StatusRunning,
		TriggerTime: time.Now().Add(-1 * time.Second), // 无 StartTime
	}
	if err := st.Instance.Create(ins); err != nil {
		t.Fatal(err)
	}
	if err := st.Instance.UpdateResult(ins.ID, domain.StatusFailed, "err"); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Instance.Get(ins.ID)
	if got.DurationMS < 500 || got.DurationMS > 10000 {
		t.Fatalf("duration_ms=%d 期望回退 trigger_time 约 1000", got.DurationMS)
	}
}

// TestInstanceDurationClearedOnRevive 验证 SetStatus 复活(终态→非终态)清空 duration_ms 与 end_time。
func TestInstanceDurationClearedOnRevive(t *testing.T) {
	st := newTestStore(t)
	start := time.Now().Add(-2 * time.Second)
	ins := &domain.Instance{
		AppID: 1, JobID: 1, Status: domain.StatusRunning,
		StartTime: &start, TriggerTime: start,
	}
	if err := st.Instance.Create(ins); err != nil {
		t.Fatal(err)
	}
	if err := st.Instance.UpdateResult(ins.ID, domain.StatusSuccess, "ok"); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Instance.Get(ins.ID); got.DurationMS == 0 {
		t.Fatal("前置:终态应已写 duration_ms")
	}
	// 复活回 queued
	if err := st.Instance.SetStatus(ins.ID, domain.StatusQueued, ""); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Instance.Get(ins.ID)
	if got.DurationMS != 0 {
		t.Fatalf("复活后 duration_ms=%d 应清零", got.DurationMS)
	}
	if got.EndTime != nil {
		t.Fatal("复活后 end_time 应清空")
	}
}

// TestInstanceDurationWithCallback 验证事务版 UpdateResultWithCallback 同样写 duration_ms。
func TestInstanceDurationWithCallback(t *testing.T) {
	st := newTestStore(t)
	start := time.Now().Add(-1500 * time.Millisecond)
	ins := &domain.Instance{
		AppID: 1, JobID: 1, Status: domain.StatusRunning,
		StartTime: &start, TriggerTime: start,
	}
	if err := st.Instance.Create(ins); err != nil {
		t.Fatal(err)
	}
	rows, err := st.Instance.UpdateResultWithCallback(ins.ID, domain.StatusTimeout, "超时", nil)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows=%d want 1", rows)
	}
	got, _ := st.Instance.Get(ins.ID)
	if got.DurationMS < 1000 || got.DurationMS > 10000 {
		t.Fatalf("duration_ms=%d 期望约 1500(WithCallback 路径)", got.DurationMS)
	}
}
