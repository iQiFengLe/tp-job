// Package workerreg 维护 worker 心跳注册表(内存,不入库)。
//
// worker 经 /worker/* 或 /server/* 上报心跳(含 workerAddress + SystemMetrics + tags 等),
// 注册表按 AppID 收集在线节点(协议层负责 appName→AppID 转换)。调度器派发时按 jobInstanceTag
// 匹配候选、按 systemMetrics.score 择优。超时未上报的 worker 由 Sweep 剔除(reaper 据此做失败转移)。
package workerreg

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"tp-job/internal/domain"
)

// Protocol worker 接入协议。
const (
	ProtocolHTTP     = "http"
	ProtocolPowerJob = "powerjob"
)

// WorkerInfo 心跳上报的 worker 信息(快照)。
type WorkerInfo struct {
	AppID           int64
	WorkerAddress   string
	Metrics         domain.SystemMetrics
	Tags            []string
	AcceptNotTagJob bool
	Protocol        string // http | powerjob
	LastHeartbeat   time.Time
}

// Registry worker 心跳注册表。
type Registry struct {
	mu       sync.RWMutex
	workers  map[int64]map[string]*WorkerInfo // appID -> address -> info
	inflight map[int64]map[string]int         // appID -> address -> 在飞实例数(派发成功 +1 / 终态 -1,负载感知选址用)
	timeout  time.Duration
	log      *slog.Logger
}

func New(timeout time.Duration, log *slog.Logger) *Registry {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Registry{
		workers:  make(map[int64]map[string]*WorkerInfo),
		inflight: make(map[int64]map[string]int),
		timeout:  timeout,
		log:      log,
	}
}

// Heartbeat 注册或刷新一个 worker。
func (r *Registry) Heartbeat(w WorkerInfo) {
	if w.AppID == 0 || w.WorkerAddress == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.workers[w.AppID]
	if !ok {
		m = make(map[string]*WorkerInfo)
		r.workers[w.AppID] = m
	}
	if existing, ok := m[w.WorkerAddress]; ok {
		existing.Metrics = w.Metrics
		existing.Tags = w.Tags
		existing.AcceptNotTagJob = w.AcceptNotTagJob
		existing.Protocol = w.Protocol
		existing.LastHeartbeat = time.Now()
		return
	}
	w.LastHeartbeat = time.Now()
	cp := w
	m[w.WorkerAddress] = &cp
}

func (r *Registry) onlineLocked(appID int64) []*WorkerInfo {
	m, ok := r.workers[appID]
	if !ok {
		return nil
	}
	now := time.Now()
	out := make([]*WorkerInfo, 0, len(m))
	for _, w := range m {
		if now.Sub(w.LastHeartbeat) <= r.timeout {
			out = append(out, w)
		}
	}
	return out
}

// HasOnlineWorker 该 app 是否有任一在线 worker(心跳未超时)。供派发层区分「无在线 worker」
// (重启窗口/worker 全挂:临时性,实例应 requeue 等待,不判 failed)与「有在线但 tag 全不匹配」
// (配置问题:应 failed 给反馈)。与 PickFull/IsOnline 同源(均基于 onlineLocked 的心跳过滤)。
func (r *Registry) HasOnlineWorker(appID int64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.onlineLocked(appID)) > 0
}

// matchTag worker 是否匹配给定任务 tag:
//
//	acceptNotTagJob || tag ∈ worker.tags || (tag 空 && worker.tags 空)
func matchTag(tag string, w *WorkerInfo) bool {
	if w.AcceptNotTagJob {
		return true
	}
	for _, t := range w.Tags {
		if t == tag {
			return true
		}
	}
	return tag == "" && len(w.Tags) == 0
}

// Pick 选一个匹配 jobTag 的在线 worker(按 Metrics.Score 降序取首);无候选返回空串。
func (r *Registry) Pick(appID int64, jobTag string) string {
	w, ok := r.PickFull(appID, jobTag)
	if !ok {
		return ""
	}
	return w.WorkerAddress
}

// PickFull 同 Pick,但返回完整 WorkerInfo(调度器需 protocol/metrics 等)。
func (r *Registry) PickFull(appID int64, jobTag string) (WorkerInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	online := r.onlineLocked(appID)
	var best *WorkerInfo
	bestInflight := 0
	for _, w := range online {
		if !matchTag(jobTag, w) {
			continue
		}
		// 负载感知:在飞实例少的 worker 优先(把任务分散到空闲节点,避免反复派给繁忙 worker——
		// 后者正是"worker 繁忙但心跳正常、任务卡在 waiting_receive"的成因之一);在飞相同时
		// 回退 Score 降序(PowerJob 约定:分数越高越空闲)。
		inflight := r.inflightLocked(appID, w.WorkerAddress)
		if best == nil || inflight < bestInflight ||
			(inflight == bestInflight && w.Metrics.Score > best.Metrics.Score) {
			best = w
			bestInflight = inflight
		}
	}
	if best == nil {
		// 诊断:区分"该 app 无在线 worker"与"在线但 tag 全不匹配"。
		// 否则上层只报笼统"无可用 worker(tag 不匹配或全部离线)",无从定位是 worker 未注册/心跳超时,
		// 还是 tag 实际不符。双空匹配逻辑见 matchTag(有 TestPickBothEmpty 覆盖,正确)。
		if len(online) == 0 {
			r.log.Warn("派发选址失败:该 app 无在线 worker", "appID", appID, "jobTag", jobTag)
		} else {
			diag := make([]string, 0, len(online))
			for _, w := range online {
				diag = append(diag, fmt.Sprintf("%s tags=%v acceptAny=%v", w.WorkerAddress, w.Tags, w.AcceptNotTagJob))
			}
			r.log.Warn("派发选址失败:在线 worker tag 均不匹配", "appID", appID, "jobTag", jobTag, "workers", diag)
		}
		return WorkerInfo{}, false
	}
	return *best, true
}

// IsOnline worker 是否在线。
func (r *Registry) IsOnline(appID int64, address string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.workers[appID]
	if !ok {
		return false
	}
	w, ok := m[address]
	if !ok {
		return false
	}
	return time.Since(w.LastHeartbeat) <= r.timeout
}

// inflightLocked 读 worker 当前在飞实例数(调用方持 mu)。无记录=0。供 PickFull 负载感知选址。
func (r *Registry) inflightLocked(appID int64, addr string) int {
	if m, ok := r.inflight[appID]; ok {
		return m[addr]
	}
	return 0
}

// Inflight 返回 worker 当前在飞实例数(供测试/管理端诊断负载)。持 RLock 读。
func (r *Registry) Inflight(appID int64, addr string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.inflightLocked(appID, addr)
}

// AcquireInflight 派发成功(Send 完成)后调用:记录该 worker 在飞实例 +1。addr 为 PickFull 选定、
// MarkDispatched/Send 使用的原值(与心跳注册 key 一致);addr 空(no-op)防误调。
// 仅影响后续 PickFull 选址打分,不改变 worker 在线性/任务级并发槽(后者由 scheduler.slots 管)。
func (r *Registry) AcquireInflight(appID int64, addr string) {
	if addr == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.inflight[appID]
	if !ok {
		m = make(map[string]int)
		r.inflight[appID] = m
	}
	m[addr]++
}

// ReleaseInflight 实例终态/失败转移时调用:在飞实例 -1。幂等——未 Acquire 过或已归零时 no-op,
// 不会变负(worker 重启/异常路径可能多调一次 Release)。归零删 key(与 Sweep 共用 deleteInflightLocked)
// 防 inflight map 无限增长。
func (r *Registry) ReleaseInflight(appID int64, addr string) {
	if addr == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.inflight[appID]
	if !ok {
		return
	}
	if m[addr] <= 1 {
		r.deleteInflightLocked(appID, addr)
	} else {
		m[addr]--
	}
}

// deleteInflightLocked 删除 worker 的在飞计数条目(调用方持 mu)。供 ReleaseInflight 归零删 key 与 Sweep
// 清理下线 worker 复用:worker 下线时其 inflight 已无意义,直接删整个 addr 条目(非递减),防泄漏计数随
// worker churn 累积、同址重连继承陈旧值扭曲 PickFull 选址。
func (r *Registry) deleteInflightLocked(appID int64, addr string) {
	im, ok := r.inflight[appID]
	if !ok {
		return
	}
	delete(im, addr)
	if len(im) == 0 {
		delete(r.inflight, appID)
	}
}

// Online 在线 worker 列表副本(供管理端展示)。
func (r *Registry) Online(appID int64) []WorkerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	src := r.onlineLocked(appID)
	out := make([]WorkerInfo, 0, len(src))
	for _, w := range src {
		out = append(out, *w)
	}
	return out
}

// Sweep 剔除心跳超时的 worker,返回清理数。
func (r *Registry) Sweep() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	removed := 0
	for appID, m := range r.workers {
		for addr, w := range m {
			if time.Since(w.LastHeartbeat) > r.timeout {
				delete(m, addr)
				r.deleteInflightLocked(appID, addr) // 同步清在飞计数:worker 下线后其计数无意义,防泄漏/陈旧残留
				removed++
			}
		}
		if len(m) == 0 {
			delete(r.workers, appID)
		}
	}
	return removed
}

// Run 周期 Sweep 剔除超时 worker,直到 ctx 取消。仅做内存卫生(IsOnline/PickFull 已按 timeout
// 过滤在线性,Sweep 只回收死条目,避免长期 churn 下 map 无限增长)。间隔取 timeout(下限 1s)。
func (r *Registry) Run(ctx context.Context) {
	interval := r.timeout
	if interval < time.Second {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.Sweep()
		}
	}
}

// NormalizeAddress 保证地址为 host:port(去 scheme / 路径)。worker 上报的通常已是 "ip:port"。
func NormalizeAddress(addr string) string {
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	if i := strings.Index(addr, "/"); i >= 0 {
		addr = addr[:i]
	}
	return addr
}
