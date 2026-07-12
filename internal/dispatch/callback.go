package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"tp-job/internal/config"
	"tp-job/internal/domain"
	"tp-job/internal/repository"
)

// ===== CallbackBuilder(装配层注入,scheduler/dservice 在状态变更点调用) =====

// CallbackBuilder 由装配层实现:scheduler/dservice 在实例状态变更点调用,
// 返回待落库的回调记录(事件瞬间快照)。回调未启用或 job 无 callback_url 时返回 nil
// (使 *WithCallback 走原路径,零开销)。
type CallbackBuilder interface {
	Enabled() bool // false=回调未启用,调用方可据此跳过为快照而做的 ins/job 查询(scheduler hook 已握 ins/job 不必判断)
	Build(ins *domain.Instance, job *domain.Job, eventStatus string) *domain.Callback
}

// NoopCallbackBuilder 永不回调(回调未启用时的占位)。
type NoopCallbackBuilder struct{}

func (NoopCallbackBuilder) Enabled() bool                                                { return false }
func (NoopCallbackBuilder) Build(*domain.Instance, *domain.Job, string) *domain.Callback { return nil }

// NewCallbackBuilder 装配:enabled=false 返回 Noop;true 返回读 job.CallbackURL 的实现。
func NewCallbackBuilder(enabled bool) CallbackBuilder {
	if !enabled {
		return NoopCallbackBuilder{}
	}
	return enabledCallbackBuilder{}
}

type enabledCallbackBuilder struct{}

func (enabledCallbackBuilder) Enabled() bool { return true }
func (enabledCallbackBuilder) Build(ins *domain.Instance, job *domain.Job, eventStatus string) *domain.Callback {
	if job == nil || job.CallbackURL == "" {
		return nil
	}
	return BuildCallback(ins, job, eventStatus, job.CallbackURL)
}

// ===== payload =====

type callbackPayload struct {
	EventStatus string           `json:"event_status"`
	OccurredAt  time.Time        `json:"occurred_at"`
	Instance    instanceSnapshot `json:"instance"`
	Job         jobSnapshot      `json:"job"`
}

type instanceSnapshot struct {
	ID             int64      `json:"id"`
	JobID          int64      `json:"job_id"`
	AppID          int64      `json:"app_id"`
	RootInstanceID int64      `json:"root_instance_id,omitempty"`
	TriggerType    string     `json:"trigger_type,omitempty"`
	RetryIndex     int        `json:"retry_index,omitempty"`
	Status         string     `json:"status"`
	WorkerAddress  string     `json:"worker_address,omitempty"`
	Result         string     `json:"result,omitempty"`
	TriggerTime    time.Time  `json:"trigger_time"`
	StartTime      *time.Time `json:"start_time,omitempty"`
	EndTime        *time.Time `json:"end_time,omitempty"`
	DurationMS     int64      `json:"duration_ms,omitempty"`
}

type jobSnapshot struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	Tag  string `json:"tag,omitempty"`
}

// BuildCallback 构造一条回调记录(payload 在事件瞬间快照)。callbackURL 为空/nil 入参返回 nil。
// payload.Instance.Status 用 eventStatus(事件触发的新状态);其余字段取 ins 快照——调用方在
// 状态变更点 build 前应先更新 ins 内存的关键字段(worker_address/result 等),以保证快照准确。
func BuildCallback(ins *domain.Instance, job *domain.Job, eventStatus, callbackURL string) *domain.Callback {
	if callbackURL == "" || ins == nil || job == nil {
		return nil
	}
	payload := callbackPayload{
		EventStatus: eventStatus,
		OccurredAt:  time.Now(),
		Instance: instanceSnapshot{
			ID: ins.ID, JobID: ins.JobID, AppID: ins.AppID,
			RootInstanceID: domain.RootOf(ins), TriggerType: ins.TriggerType, RetryIndex: ins.RetryIndex,
			Status: eventStatus, WorkerAddress: ins.WorkerAddress, Result: ins.Result,
			TriggerTime: ins.TriggerTime, StartTime: ins.StartTime, EndTime: ins.EndTime,
			DurationMS: ins.DurationMS,
		},
		Job: jobSnapshot{ID: job.ID, Name: job.Name, Tag: job.Tag},
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	now := time.Now()
	return &domain.Callback{
		JobID:          job.ID,
		AppID:          job.AppID,
		RootInstanceID: domain.RootOf(ins),
		TriggerType:    ins.TriggerType,
		RetryIndex:     ins.RetryIndex,
		EventStatus:    eventStatus,
		URL:            callbackURL,
		Payload:        string(b),
		State:          domain.CallbackPending,
		NextRetryAt:    &now,
	}
}

// ===== HTTP Transport =====

// NewDialTransport 构造带连接超时的 http.Transport(callback POST / PowerJob 客户端用)。
// worker 地址与 callback URL 的可信性由部署侧网络隔离保证(/server/*、/worker/* 无鉴权),
// 此处仅设连接超时,不做来源限制。
func NewDialTransport(dialTimeout time.Duration) *http.Transport {
	dialer := &net.Dialer{Timeout: dialTimeout}
	return &http.Transport{
		DialContext: dialer.DialContext,
	}
}

// ===== CallbackPump(周期扫描 + 投递 + 退避) =====

// CallbackPump 周期扫描 pending 回调,经 SSRF 安全的 client POST,失败指数退避重试,达上限置 dead。
// retention>0 时另起 sweepLoop 周期清理 sent/dead 记录,控制 instance_callback 表体量。
type CallbackPump struct {
	store     *repository.Store
	client    *http.Client
	interval  time.Duration
	limit     int
	maxAtt    int
	backBase  time.Duration
	backMax   time.Duration
	retention time.Duration // sent/dead 记录保留期;sweepLoop 按 retention/2 周期 PurgeOld
	log       *slog.Logger
	wg        sync.WaitGroup
}

// NewCallbackPump 构造。client 应使用 NewDialTransport + 禁重定向(CheckRedirect)。
// retention 从 cfg.RetentionDays 派生(<=0 不启用清理)。
func NewCallbackPump(st *repository.Store, client *http.Client, interval time.Duration, cfg config.CallbackCfg, log *slog.Logger) *CallbackPump {
	return &CallbackPump{
		store: st, client: client, interval: interval, limit: 500,
		maxAtt:    cfg.MaxAttempts,
		backBase:  time.Duration(cfg.BackoffBaseSec) * time.Second,
		backMax:   time.Duration(cfg.BackoffMaxSec) * time.Second,
		retention: time.Duration(cfg.RetentionDays) * 24 * time.Hour,
		log:       log,
	}
}

func (p *CallbackPump) Start(ctx context.Context) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			if p.runSupervised(ctx, p.run) {
				return
			}
		}
	}()
	if p.retention > 0 {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				if p.runSupervised(ctx, p.sweepLoop) {
					return
				}
			}
		}()
	}
}

// runSupervised 执行 fn:正常返回 true;panic 时 recover+log+1s 退避后返回 false(供重启决策)。
func (p *CallbackPump) runSupervised(ctx context.Context, fn func(context.Context)) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			p.log.Error("callback 循环 panic,1s 后重启", "panic", r)
			time.Sleep(time.Second)
		} else {
			ok = true
		}
	}()
	fn(ctx)
	return
}

func (p *CallbackPump) Wait() { p.wg.Wait() }

// sweepLoop 周期清理已终态(sent/dead)的回调记录。按 retention/2 周期清扫(最小 1h,避免高频删除);
// PurgeOld 内部限定 state IN (sent,dead),pending 永不删(未投递回调不丢)。
func (p *CallbackPump) sweepLoop(ctx context.Context) {
	interval := p.retention / 2
	if interval < time.Hour {
		interval = time.Hour
	}
	p.log.Info("callback 清理循环启动", "retention", p.retention, "interval", interval)
	p.purgeOnce() // 启动即首扫,避免过期记录最长等一个 interval 才清
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			p.log.Info("callback 清理循环停止")
			return
		case <-t.C:
			p.purgeOnce()
		}
	}
}

// purgeOnce 执行一次清理并记录结果。
func (p *CallbackPump) purgeOnce() {
	n, err := p.store.Callback.PurgeOld(time.Now().Add(-p.retention))
	if err != nil {
		p.log.Error("callback 清理失败", "err", err)
		return
	}
	if n > 0 {
		p.log.Info("callback 清理", "purged", n)
	}
}

func (p *CallbackPump) run(ctx context.Context) {
	p.log.Info("callback pump 启动", "interval", p.interval)
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			p.log.Info("callback pump 停止")
			return
		case <-t.C:
			p.once(ctx)
		}
	}
}

func (p *CallbackPump) once(ctx context.Context) {
	list, err := p.store.Callback.ListDue(time.Now(), p.limit)
	if err != nil {
		p.log.Error("callback 扫描失败", "err", err)
		return
	}
	for i := range list {
		cb := list[i]
		if err := p.send(ctx, &cb); err != nil {
			p.handleFail(&cb, err)
			continue
		}
		// send 已成功(对端收到)。先乐观持久化 attempt+1 + 退避 next_retry_at;达 maxAtt 时 markProgress
		// 会 MarkDead 终止并返回 true——此时必须跳过 MarkSent:MarkSent 的 WHERE 仅 id=? 无 state 守卫,
		// 紧跟在 MarkDead 之后调用会把 dead 改回 sent,使"已达上限放弃投递"的记录被误标投递成功。
		// 正常路径(未达上限)markProgress 推进退避后返回 false,继续 MarkSent 收尾;MarkSent 失败(DB 瞬时故障)
		// 时 cb 仍 pending + 退避,下轮 ListDue 按退避重投,attempt 受 maxAtt 约束可达 dead 终止——避免旧版
		// "MarkSent 失败→handleFail→MarkRetry 也失败"时 attempt 永不增长、DB 抖动下已投递成功的回调被无限重投。
		if p.markProgress(&cb) {
			continue // 已 MarkDead(达上限),不再 MarkSent
		}
		if err := p.store.Callback.MarkSent(cb.ID); err != nil {
			p.log.Warn("callback MarkSent 记账失败(已乐观推进退避,下轮按退避重投)", "id", cb.ID, "err", err)
		}
	}
}

// backoff 指数退避:backBase * 2^attempt,clamp 到 backMax(防移位溢出/下溢致每 tick 捞出)。
// markProgress(乐观推进)与 handleFail(失败重试)共用,确保两路径退避节奏一致。
func (p *CallbackPump) backoff(attempt int) time.Duration {
	d := p.backBase * time.Duration(int64(1)<<uint(attempt))
	if d <= 0 || d > p.backMax {
		d = p.backMax
	}
	return d
}

// markProgress send 成功后乐观持久化 attempt+1 + 退避 next_retry_at(MarkSent 之前调)。
// 达 maxAtt 时 MarkDead 终止后续重投并返回 true,使 once 跳过 MarkSent——send 已成功,dead 表"本地
// 放弃记账"(对端大概率已收到);此时若继续 MarkSent,其 WHERE 仅 id=? 会把刚写的 dead 改回 sent。
// 返回 false 表示未达上限(已 MarkRetry 推进退避),once 应继续 MarkSent 收尾。
// 失败只记日志:极端 DB 持续全故障下 attempt 无法推进(物理限制,无法纯软件兜底)。
func (p *CallbackPump) markProgress(cb *domain.Callback) (dead bool) {
	attempt := cb.Attempt + 1
	if attempt >= p.maxAtt {
		if e := p.store.Callback.MarkDead(cb.ID, "记账达上限(send 已成功)"); e != nil {
			p.log.Error("callback MarkDead 失败", "id", cb.ID, "err", e)
		}
		return true
	}
	next := time.Now().Add(p.backoff(attempt))
	if e := p.store.Callback.MarkRetry(cb.ID, attempt, next, ""); e != nil {
		p.log.Error("callback 乐观 MarkRetry 失败", "id", cb.ID, "err", e)
	}
	return false
}

func (p *CallbackPump) send(ctx context.Context, cb *domain.Callback) error {
	u, err := url.Parse(cb.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("非法 callback url %q", cb.URL)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cb.URL, strings.NewReader(cb.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-TaskSchedule-Event-ID", fmt.Sprintf("cb-%d", cb.ID))
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20)) // 释放连接,上限 1MB 防巨幅响应体耗内存
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("callback 非 2xx: %d", resp.StatusCode)
	}
	return nil
}

// handleFail 失败善后:未达上限则指数退避(MarkRetry);达上限置 dead。
func (p *CallbackPump) handleFail(cb *domain.Callback, err error) {
	attempt := cb.Attempt + 1
	if attempt >= p.maxAtt {
		if e := p.store.Callback.MarkDead(cb.ID, err.Error()); e != nil {
			p.log.Error("callback MarkDead 失败", "id", cb.ID, "err", e)
		}
		p.log.Warn("callback 达上限放弃", "id", cb.ID, "attempt", attempt, "err", err)
		return
	}
	next := time.Now().Add(p.backoff(attempt))
	if e := p.store.Callback.MarkRetry(cb.ID, attempt, next, err.Error()); e != nil {
		p.log.Error("callback MarkRetry 失败", "id", cb.ID, "err", e)
	}
}
