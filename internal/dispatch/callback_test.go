package dispatch

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"task-schedule/internal/config"
	"task-schedule/internal/domain"
	"task-schedule/internal/workerreg"
)

func TestBuildCallbackFields(t *testing.T) {
	job := &domain.Job{ID: 7, AppID: 2, Name: "j", Tag: "gpu", CallbackURL: "http://h/x"}
	ins := &domain.Instance{ID: 5, JobID: 7, AppID: 2, TriggerType: "auto", Status: domain.StatusQueued,
		WorkerAddress: "w:9000"}
	cb := BuildCallback(ins, job, domain.StatusQueued, job.CallbackURL)
	if cb == nil {
		t.Fatal("有 url 时 cb 不应为 nil")
	}
	if cb.EventStatus != domain.StatusQueued || cb.URL != job.CallbackURL || cb.State != domain.CallbackPending {
		t.Errorf("字段不符: %+v", cb)
	}
	if cb.JobID != 7 || cb.AppID != 2 || cb.RootInstanceID != domain.RootOf(ins) {
		t.Errorf("job/app/root id 不符: %+v", cb)
	}
	if cb.NextRetryAt == nil {
		t.Error("NextRetryAt 应设为 now(pump 立即可取)")
	}
	// 空 url / nil job 返回 nil
	if BuildCallback(ins, job, "x", "") != nil {
		t.Error("空 url 应返回 nil")
	}
	if BuildCallback(ins, nil, "x", "http://h") != nil {
		t.Error("nil job 应返回 nil")
	}
}

func TestCallbackHandleFailBackoff(t *testing.T) {
	st := newTestStore(t)
	cfg := config.CallbackCfg{MaxAttempts: 5, BackoffBaseSec: 1, BackoffMaxSec: 10}
	p := NewCallbackPump(st, nil, time.Second, cfg, testLog())
	now := time.Now()
	cb := &domain.Callback{JobID: 1, AppID: 1, EventStatus: "running", URL: "http://x",
		State: domain.CallbackPending, NextRetryAt: &now}
	if err := st.Callback.Create(cb); err != nil {
		t.Fatal(err)
	}
	p.handleFail(cb, fmt.Errorf("err"))
	var got domain.Callback
	if err := st.DB.First(&got, cb.ID).Error; err != nil {
		t.Fatal(err)
	}
	if got.Attempt != 1 || got.State != domain.CallbackPending {
		t.Errorf("应 MarkRetry attempt=1 仍 pending, got attempt=%d state=%s", got.Attempt, got.State)
	}
	if got.NextRetryAt == nil || got.NextRetryAt.Before(time.Now()) {
		t.Error("next_retry_at 应在未来(指数退避)")
	}
}

func TestCallbackHandleFailDead(t *testing.T) {
	st := newTestStore(t)
	cfg := config.CallbackCfg{MaxAttempts: 3, BackoffBaseSec: 1, BackoffMaxSec: 10}
	p := NewCallbackPump(st, nil, time.Second, cfg, testLog())
	now := time.Now()
	cb := &domain.Callback{JobID: 1, AppID: 1, EventStatus: "running", URL: "http://x",
		State: domain.CallbackPending, NextRetryAt: &now, Attempt: 2} // attempt+1=3 >= MaxAttempts → dead
	if err := st.Callback.Create(cb); err != nil {
		t.Fatal(err)
	}
	p.handleFail(cb, fmt.Errorf("err"))
	var got domain.Callback
	st.DB.First(&got, cb.ID)
	if got.State != domain.CallbackDead {
		t.Errorf("达 MaxAttempts 应 dead, got %s", got.State)
	}
}

// SSRF:白名单只含 10.x,httptest 地址(127.0.0.1)应被 DialContext 拒绝 → send 失败。
func TestCallbackSendSSRFBlocked(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	pol, err := workerreg.NewAddressPolicy([]string{"10.0.0.0/8"})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{
		Timeout:   2 * time.Second,
		Transport: NewSSRFTransport(pol, time.Second),
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	p := NewCallbackPump(newTestStore(t), client, time.Second, config.CallbackCfg{TimeoutSec: 2}, testLog())
	cb := &domain.Callback{URL: srv.URL, Payload: "{}"}
	if err := p.send(context.Background(), cb); err == nil {
		t.Error("白名单外的 127.0.0.1 应被 SSRF 拦截,期望 send 失败")
	}
}

// 非 2xx → send 失败(触发 pump 重试)。
func TestCallbackSendNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer srv.Close()
	pol, _ := workerreg.NewAddressPolicy([]string{"127.0.0.0/8"})
	client := &http.Client{Timeout: 2 * time.Second, Transport: NewSSRFTransport(pol, time.Second)}
	p := NewCallbackPump(newTestStore(t), client, time.Second, config.CallbackCfg{TimeoutSec: 2}, testLog())
	cb := &domain.Callback{URL: srv.URL, Payload: "{}"}
	if err := p.send(context.Background(), cb); err == nil {
		t.Error("500 应判失败")
	}
}

// send 成功但 MarkSent 记账失败(DB 瞬时故障)→ markProgress 已在 MarkSent 之前乐观推进 attempt+退避,
// 故 cb 保持 attempt+1 + 未来 next_retry_at,下轮按退避重投,attempt 受 MaxAttempts 上限保护可达 dead;
// 而非停留 attempt=0/pending/next_retry_at<=now 导致下轮立即重投、永不触达上限。
// 故障注入:sqlite 触发器,UPDATE 到 state=sent 时 RAISE(FAIL),精确模拟 MarkSent 失败
// (ListDue 是 SELECT、Create 是 INSERT、MarkRetry 不改 state,均不触发,故只让 MarkSent 失败;
// markProgress 的 MarkRetry 不改 state 也不触发)。
func TestCallbackMarkSentFailureRetries(t *testing.T) {
	st := newTestStore(t)
	if err := st.DB.Exec(`CREATE TRIGGER fail_sent BEFORE UPDATE ON instance_callback
WHEN NEW.state = 'sent' BEGIN SELECT RAISE(FAIL, 'injected marksent failure'); END`).Error; err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200) // send 成功(对端收到)
	}))
	defer srv.Close()
	pol, _ := workerreg.NewAddressPolicy([]string{"127.0.0.0/8"})
	client := &http.Client{Timeout: 2 * time.Second, Transport: NewSSRFTransport(pol, time.Second)}
	cfg := config.CallbackCfg{MaxAttempts: 5, BackoffBaseSec: 1, BackoffMaxSec: 10}
	p := NewCallbackPump(st, client, time.Second, cfg, testLog())

	now := time.Now()
	cb := &domain.Callback{JobID: 1, AppID: 1, EventStatus: "running", URL: srv.URL, Payload: "{}",
		State: domain.CallbackPending, NextRetryAt: &now}
	if err := st.Callback.Create(cb); err != nil {
		t.Fatal(err)
	}

	p.once(context.Background()) // send 成功 → markProgress 乐观推进 → MarkSent 失败(触发器),cb 保持退避

	var got domain.Callback
	if err := st.DB.First(&got, cb.ID).Error; err != nil {
		t.Fatal(err)
	}
	if got.Attempt != 1 {
		t.Errorf("MarkSent 失败应转 handleFail 推进 attempt=1, got %d (修复前:停留 0 无限重投)", got.Attempt)
	}
	if got.State != domain.CallbackPending {
		t.Errorf("应仍 pending 待重投, got %s", got.State)
	}
	if got.NextRetryAt == nil || got.NextRetryAt.Before(time.Now()) {
		t.Error("next_retry_at 应退避到未来,而非停留 now 致下轮立即重投")
	}
	// last_error:markProgress 的 MarkRetry 写空(MarkSent 失败原因只进日志,不回写 DB)。
	// 核心保证已在上面三项:attempt 推进 + 仍 pending + next_retry_at 退避到未来 → 下轮重投 attempt 受上限约束。
}

// send 成功 + 本次 attempt 达 maxAtt → markProgress MarkDead 并返回 true,once 据此跳过 MarkSent,
// 最终 state=dead。修复前:once 无条件 MarkSent(WHERE 仅 id=?),把刚写的 dead 改回 sent,
// 使"已达上限放弃投递"的记录被误标投递成功,B1。
func TestCallbackSendSuccessMaxAttDead(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200) // send 成功(对端收到)
	}))
	defer srv.Close()
	pol, _ := workerreg.NewAddressPolicy([]string{"127.0.0.0/8"})
	client := &http.Client{Timeout: 2 * time.Second, Transport: NewSSRFTransport(pol, time.Second)}
	st := newTestStore(t)
	cfg := config.CallbackCfg{MaxAttempts: 2, BackoffBaseSec: 1, BackoffMaxSec: 10}
	p := NewCallbackPump(st, client, time.Second, cfg, testLog())

	now := time.Now()
	cb := &domain.Callback{JobID: 1, AppID: 1, EventStatus: "running", URL: srv.URL, Payload: "{}",
		State: domain.CallbackPending, NextRetryAt: &now, Attempt: 1} // attempt+1=2 >= MaxAttempts=2 → dead
	if err := st.Callback.Create(cb); err != nil {
		t.Fatal(err)
	}

	p.once(context.Background())

	var got domain.Callback
	if err := st.DB.First(&got, cb.ID).Error; err != nil {
		t.Fatal(err)
	}
	if got.State != domain.CallbackDead {
		t.Errorf("send 成功 + 达 maxAtt 应终止为 dead(不被 MarkSent 覆盖), got %s", got.State)
	}
}

// *WithCallback 的 build 闭包应拿到 tx 内 UPDATE 后 SELECT 出的最新行(事件瞬间值),
// 而非 tx 外的读快照——避免并发改 DB 期间 payload stale(TOCTOU)。
func TestUpdateResultWithCallbackBuildGetsLatest(t *testing.T) {
	st := newTestStore(t)
	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	ins := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusQueued, Result: "old"}
	_ = st.Instance.Create(ins)

	var captured *domain.Instance
	build := func(latest *domain.Instance) *domain.Callback {
		captured = latest
		return nil // 不插 cb,仅验证 latest
	}
	if _, err := st.Instance.UpdateResultWithCallback(ins.ID, domain.StatusRunning, "ok", build); err != nil {
		t.Fatal(err)
	}
	if captured == nil {
		t.Fatal("build 闭包未被调用")
	}
	if captured.Status != domain.StatusRunning || captured.Result != "ok" {
		t.Errorf("build 收到的 latest 应为 tx 写入后的值(status=running/result=ok), got status=%s result=%s (TOCTOU:用了读快照)",
			captured.Status, captured.Result)
	}
}
