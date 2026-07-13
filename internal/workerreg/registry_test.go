package workerreg

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"tp-job/internal/domain"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func mkMetrics(score int) domain.SystemMetrics { return domain.SystemMetrics{Score: score} }

// 基本选址:两个 worker,取 score 高者。
func TestPickByScore(t *testing.T) {
	r := New(time.Minute, discardLog())
	r.Heartbeat(WorkerInfo{AppID: 1, WorkerAddress: "w1:9", Metrics: mkMetrics(5)})
	r.Heartbeat(WorkerInfo{AppID: 1, WorkerAddress: "w2:9", Metrics: mkMetrics(11)})

	if got := r.Pick(1, ""); got != "w2:9" {
		t.Fatalf("应选 score=11 的 w2:9, got %s", got)
	}
}

// tag 匹配:仅 tag 命中的 worker 进候选。
func TestPickTagMatch(t *testing.T) {
	r := New(time.Minute, discardLog())
	r.Heartbeat(WorkerInfo{AppID: 1, WorkerAddress: "gpu:9", Metrics: mkMetrics(1), Tags: []string{"gpu"}})
	r.Heartbeat(WorkerInfo{AppID: 1, WorkerAddress: "cpu:9", Metrics: mkMetrics(99), Tags: []string{"cpu"}})

	if got := r.Pick(1, "gpu"); got != "gpu:9" {
		t.Fatalf("gpu 任务应派给 gpu worker, got %s", got)
	}
	if got := r.Pick(1, "cpu"); got != "cpu:9" {
		t.Fatalf("cpu 任务应派给 cpu worker, got %s", got)
	}
	if got := r.Pick(1, "missing"); got != "" {
		t.Fatalf("无匹配 tag 应返回空, got %s", got)
	}
}

// acceptNotTagJob:接受任意任务的 worker 命中所有 tag。
func TestPickAcceptNotTag(t *testing.T) {
	r := New(time.Minute, discardLog())
	r.Heartbeat(WorkerInfo{AppID: 1, WorkerAddress: "any:9", Metrics: mkMetrics(3), AcceptNotTagJob: true})

	if got := r.Pick(1, "whatever"); got != "any:9" {
		t.Fatalf("acceptNotTagJob 应匹配任意 tag, got %s", got)
	}
}

// 双空匹配:任务无 tag && worker 无 tags → 命中;任务有 tag 而 worker 不接受 → 不命中。
func TestPickBothEmpty(t *testing.T) {
	r := New(time.Minute, discardLog())
	r.Heartbeat(WorkerInfo{AppID: 1, WorkerAddress: "plain:9", Metrics: mkMetrics(2)}) // tags 空,acceptNotTagJob=false

	if got := r.Pick(1, ""); got != "plain:9" {
		t.Fatalf("双空应命中, got %s", got)
	}
	if got := r.Pick(1, "x"); got != "" {
		t.Fatalf("worker 无 tags 且不 acceptNotTagJob, 有 tag 任务不应命中, got %s", got)
	}
}

// 负载感知选址:两 worker 在飞数不同时优先选在飞少的(空闲);相同时回退 Score 降序。
// 消除"反复派给繁忙 worker":派发成功 AcquireInflight 使繁忙 worker 在后续 PickFull 中排名下降,
// 新任务/重派自然分散到空闲节点(配合 scheduler 接收超时,即使选错也能快速回收重派)。
func TestPickByInflight(t *testing.T) {
	r := New(time.Minute, discardLog())
	r.Heartbeat(WorkerInfo{AppID: 1, WorkerAddress: "w1:9", Metrics: mkMetrics(99)}) // score 高但将变繁忙
	r.Heartbeat(WorkerInfo{AppID: 1, WorkerAddress: "w2:9", Metrics: mkMetrics(1)})  // score 低但空闲

	// 初始在飞均为 0:回退 Score 降序,选 w1
	if got := r.Pick(1, ""); got != "w1:9" {
		t.Fatalf("无在飞时应选 score 高的 w1:9, got %s", got)
	}
	// w1 派发 3 个在飞:负载感知应选空闲的 w2(尽管 score 低)
	r.AcquireInflight(1, "w1:9")
	r.AcquireInflight(1, "w1:9")
	r.AcquireInflight(1, "w1:9")
	if got := r.Pick(1, ""); got != "w2:9" {
		t.Fatalf("w1 繁忙(3 在飞)时应选空闲的 w2:9, got %s", got)
	}
	// w1 全释放归零:w1 空闲,再次回退 Score 选 w1
	r.ReleaseInflight(1, "w1:9")
	r.ReleaseInflight(1, "w1:9")
	r.ReleaseInflight(1, "w1:9")
	if got := r.Pick(1, ""); got != "w1:9" {
		t.Fatalf("w1 归零空闲后应回退 Score 选 w1:9, got %s", got)
	}
}

// AcquireInflight/ReleaseInflight 计数与幂等:Release 多于 Acquire 不为负(归零后 no-op,可重复调);
// 归零删 key(重新 Acquire 从 1 起,非叠加残留);addr 空 no-op。
func TestInflightAcquireRelease(t *testing.T) {
	r := New(time.Minute, discardLog())
	r.AcquireInflight(1, "")  // addr 空 no-op
	r.ReleaseInflight(1, "")  // addr 空 no-op
	r.ReleaseInflight(1, "ghost:9") // 未 Acquire 过:幂等 no-op,不创建 key

	r.AcquireInflight(1, "w:9")
	r.AcquireInflight(1, "w:9")
	r.ReleaseInflight(1, "w:9")
	r.ReleaseInflight(1, "w:9")
	r.ReleaseInflight(1, "w:9") // 多调一次:幂等,不为负
	r.ReleaseInflight(1, "w:9")

	// 归零后 key 已删:再 Acquire 同 addr 应从 1 开始(无残留叠加)
	r.AcquireInflight(1, "w:9")
	r.mu.RLock()
	got := r.inflight[1]["w:9"]
	r.mu.RUnlock()
	if got != 1 {
		t.Fatalf("归零删 key 后重新 Acquire 应为 1, got %d", got)
	}
}

func TestSweepTimeout(t *testing.T) {
	r := New(time.Millisecond*20, discardLog())
	r.Heartbeat(WorkerInfo{AppID: 1, WorkerAddress: "w:9", Metrics: mkMetrics(1)})
	time.Sleep(time.Millisecond * 60)
	if n := r.Sweep(); n != 1 {
		t.Fatalf("应清理 1 个超时 worker, got %d", n)
	}
	if r.Pick(1, "") != "" {
		t.Fatal("清理后应无在线 worker")
	}
}

// TestSweepClearsInflight 验证 Sweep 剔除超时 worker 时同步清 inflight——worker 下线后其计数无意义,
// 残留(泄漏/陈旧窗口)会随同址重连继承、扭曲 PickFull 选址。
func TestSweepClearsInflight(t *testing.T) {
	r := New(time.Millisecond*20, discardLog())
	r.Heartbeat(WorkerInfo{AppID: 1, WorkerAddress: "w:9", Metrics: mkMetrics(1)})
	r.AcquireInflight(1, "w:9")
	r.AcquireInflight(1, "w:9")
	if n := r.Inflight(1, "w:9"); n != 2 {
		t.Fatalf("Acquire 后应为 2, got %d", n)
	}
	time.Sleep(time.Millisecond * 60)
	if n := r.Sweep(); n != 1 {
		t.Fatalf("应清理 1 个超时 worker, got %d", n)
	}
	if n := r.Inflight(1, "w:9"); n != 0 {
		t.Fatalf("Sweep 应同步清掉下线 worker 的 inflight, got %d", n)
	}
}

func TestNormalizeAddress(t *testing.T) {
	cases := map[string]string{
		"1.2.3.4:9000":          "1.2.3.4:9000",
		"http://1.2.3.4:9000":   "1.2.3.4:9000",
		"http://1.2.3.4:9000/x": "1.2.3.4:9000",
	}
	for in, want := range cases {
		if got := NormalizeAddress(in); got != want {
			t.Errorf("NormalizeAddress(%q)=%q, want %q", in, got, want)
		}
	}
}
