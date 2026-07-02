package workerreg

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"task-schedule/internal/domain"
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
