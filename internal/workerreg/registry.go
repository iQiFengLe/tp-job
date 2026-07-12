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

	"task-schedule/internal/domain"
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
	mu      sync.RWMutex
	workers map[int64]map[string]*WorkerInfo // appID -> address -> info
	timeout time.Duration
	log     *slog.Logger
}

func New(timeout time.Duration, log *slog.Logger) *Registry {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Registry{workers: make(map[int64]map[string]*WorkerInfo), timeout: timeout, log: log}
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
	for _, w := range online {
		if !matchTag(jobTag, w) {
			continue
		}
		if best == nil || w.Metrics.Score > best.Metrics.Score {
			best = w
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
