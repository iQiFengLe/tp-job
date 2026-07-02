package dispatch

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"task-schedule/internal/domain"
	"task-schedule/internal/instancelog"
	"task-schedule/internal/repository"
	"task-schedule/internal/schedtime"
	"task-schedule/internal/workerreg"
)

// Scheduler domain 调度器:周期扫描到期 Job → 认领(AdvanceNextRun 乐观锁)→ 经 Executor 派发。
//
// 任务级并发槽随实例生命周期(派发成功后绑定到实例,worker 回报终态 / reaper 转移才经
// ReleaseInFlight 释放)。定时触发固定串行(同 job 有实例在飞则跳过本次到期,不推进游标)。
//
// 失败兜底两条:
//   - RunInstanceReaper:扫 waiting_receive/running 实例,worker 失联或执行超 TimeoutSec → failed 重派。
//   - RunRetryPump:扫 failed 且 next_retry_time 到期的实例,按 retryIndex+1 重派(DB 驱动,重启不丢)。
type Scheduler struct {
	store    *repository.Store
	executor domain.Executor
	il       *instancelog.Logger
	log      *slog.Logger

	interval time.Duration
	limit    int

	// 手动触发优先队列
	pqMu  sync.Mutex
	pq    pqHeap
	pqSeq int64
	wake  chan struct{}

	slotMu sync.Mutex
	slots  map[int64]int   // jobID -> 在飞实例计数(auto+manual 共享)
	held   map[int64]int64 // instanceID -> jobID(在飞绑定,供终态按实例释放)
}

// NewScheduler 创建调度器。interval 为扫描周期。
func NewScheduler(st *repository.Store, exec domain.Executor, il *instancelog.Logger, interval time.Duration, log *slog.Logger) *Scheduler {
	if interval <= 0 {
		interval = time.Second
	}
	return &Scheduler{
		store: st, executor: exec, il: il, log: log,
		interval: interval, limit: 500,
		wake:  make(chan struct{}, 1),
		slots: make(map[int64]int), held: make(map[int64]int64),
	}
}

// Run 定时调度循环,直到 ctx 取消。
func (s *Scheduler) Run(ctx context.Context) {
	s.log.Info("domain 调度器启动", "interval", s.interval)
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s.log.Info("domain 调度器停止")
			return
		case <-t.C:
			s.runOnce(ctx)
		}
	}
}

func (s *Scheduler) runOnce(ctx context.Context) {
	jobs, err := s.store.Job.ListDue(time.Now(), s.limit)
	if err != nil {
		s.log.Error("扫描到期 job 失败", "err", err)
		return
	}
	for i := range jobs {
		job := jobs[i] // 副本,避免闭包捕获循环变量
		s.dispatch(ctx, &job)
	}
}

// dispatch 定时触发串行:tryAcquire(1) 成功才认领派发;同 job 有实例在飞则跳过。
func (s *Scheduler) dispatch(ctx context.Context, job *domain.Job) {
	if job.NextRunTime == nil {
		return
	}
	if !s.tryAcquire(job.ID, 1) {
		return
	}
	oldNext := *job.NextRunTime
	go s.execute(ctx, job, oldNext)
}

// execute 认领 → 创建实例 → 派发。
// 槽一致性:tryAcquire 已 +1;CreateInstance 后立即 bindHeld,此后任一退出路径用 releaseByInstance;
// CreateInstance 之前(AdvanceNextRun 失败等)用 releaseByJob。
func (s *Scheduler) execute(ctx context.Context, job *domain.Job, oldNext time.Time) {
	var ins *domain.Instance
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("调度执行 panic", "job_id", job.ID, "panic", r)
			if ins != nil {
				s.releaseByInstance(ins.ID)
			} else {
				s.releaseByJob(job.ID)
			}
		}
	}()

	newNext := computeNextRun(job, time.Now())
	ok, err := s.store.Job.AdvanceNextRun(job.ID, oldNext, newNext)
	if err != nil {
		s.log.Error("认领 job 失败", "job_id", job.ID, "err", err)
		s.releaseByJob(job.ID)
		return
	}
	if !ok {
		s.releaseByJob(job.ID)
		return // 已被认领 / 游标已变
	}

	now := time.Now()
	ins = &domain.Instance{
		JobID:             job.ID,
		AppID:             job.AppID,
		Status:            domain.StatusQueued,
		TriggerType:       "auto",
		TriggerTime:       now,
		Tag:               job.Tag,
		JobInstanceParams: job.JobParams, // 定时触发:实例参数默认=任务参数
	}
	if err := s.store.Instance.Create(ins); err != nil {
		s.log.Error("创建实例失败", "job_id", job.ID, "err", err)
		s.releaseByJob(job.ID)
		return
	}
	s.appendLog(job, ins, "CREATE", "info", "实例创建")
	s.bindHeld(ins.ID, job.ID) // 先绑:保证后续任一终态路径能经 ReleaseInFlight 释放

	res := s.executor.Dispatch(ctx, job, ins)
	if res.Accepted {
		if err := s.store.Instance.MarkDispatched(ins.ID, res.WorkerAddress); err != nil {
			s.log.Error("标记派发失败", "instance_id", ins.ID, "err", err)
		}
		s.appendLog(job, ins, "DISPATCH", "info", "派发到 worker "+res.WorkerAddress)
		// 槽随实例到终态(不释放)
	} else {
		_ = s.store.Instance.UpdateResult(ins.ID, domain.StatusFailed, res.Reason)
		s.appendLog(job, ins, "STATUS", "error", "派发失败→failed: "+res.Reason)
		s.releaseByInstance(ins.ID)
	}
}

// ===== 手动触发(优先队列) =====

type manualItem struct {
	job      *domain.Job
	ins      *domain.Instance
	priority int
	seq      int64
}

type pqHeap []manualItem

func (h pqHeap) Len() int { return len(h) }
func (h pqHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority
	}
	return h[i].seq < h[j].seq
}
func (h pqHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *pqHeap) Push(x any)         { *h = append(*h, x.(manualItem)) }
func (h *pqHeap) Pop() any           { old := *h; x := old[len(old)-1]; *h = old[:len(old)-1]; return x }

// SubmitManual 将一次手动触发落库(queued)并入优先队列,由 RunManualDispatcher 按
// MaxConcurrency 调度。返回 error 仅在实例落库失败时非 nil——此前为 fire-and-forget,
// 调用方无法感知落库失败会空报 triggered。重启后残留的 queued 实例由 RecoverManualQueued 恢复。
func (s *Scheduler) SubmitManual(ctx context.Context, job *domain.Job, priority int, instanceParams string) error {
	now := time.Now()
	ins := &domain.Instance{
		JobID:             job.ID,
		AppID:             job.AppID,
		Status:            domain.StatusQueued,
		TriggerType:       "manual",
		Priority:          priority,
		TriggerTime:       now,
		Tag:               job.Tag,
		JobInstanceParams: instanceParams,
	}
	if err := s.store.Instance.Create(ins); err != nil {
		return fmt.Errorf("创建 queued 实例失败: %w", err)
	}
	s.appendLog(job, ins, "CREATE", "info", "手动触发排队")
	s.pqMu.Lock()
	s.pqSeq++
	heap.Push(&s.pq, manualItem{job: job, ins: ins, priority: priority, seq: s.pqSeq})
	s.pqMu.Unlock()
	s.notifyWake()
	return nil
}

// RunManualDispatcher 按 MaxConcurrency 消费手动队列:抢任务级槽 → 派发(槽随实例生命周期)。
func (s *Scheduler) RunManualDispatcher(ctx context.Context) {
	for {
		if s.pendingLen() == 0 {
			select {
			case <-ctx.Done():
				return
			case <-s.wake:
			}
		}
		item, ok := s.popPending()
		if !ok {
			continue
		}
		if !s.tryAcquire(item.job.ID, item.job.MaxConcurrency) {
			// 超限:实例仍为 queued,丢回队列尾部,等槽释放(wake)或兜底退避后重试。
			// 不归还槽(未抢到)。wake 由 releaseByJob/releaseByInstance 在槽释放时发出,
			// 使排队实例在终态/失败释放后立即被重派,而非盲轮询。
			s.pqMu.Lock()
			heap.Push(&s.pq, item)
			s.pqMu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-s.wake:
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		go s.runManualHeld(ctx, item)
	}
}

func (s *Scheduler) runManualHeld(ctx context.Context, item manualItem) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("手动触发 panic", "job_id", item.job.ID, "panic", r)
			s.releaseByInstance(item.ins.ID)
		}
	}()
	s.bindHeld(item.ins.ID, item.job.ID)
	res := s.executor.Dispatch(ctx, item.job, item.ins)
	if res.Accepted {
		if err := s.store.Instance.MarkDispatched(item.ins.ID, res.WorkerAddress); err != nil {
			s.log.Error("标记派发失败", "instance_id", item.ins.ID, "err", err)
		}
		s.appendLog(item.job, item.ins, "DISPATCH", "info", "手动触发派发到 "+res.WorkerAddress)
	} else {
		_ = s.store.Instance.UpdateResult(item.ins.ID, domain.StatusFailed, res.Reason)
		s.appendLog(item.job, item.ins, "STATUS", "error", "手动派发失败: "+res.Reason)
		s.releaseByInstance(item.ins.ID)
	}
}

func (s *Scheduler) pendingLen() int {
	s.pqMu.Lock()
	defer s.pqMu.Unlock()
	return s.pq.Len()
}

func (s *Scheduler) popPending() (manualItem, bool) {
	s.pqMu.Lock()
	defer s.pqMu.Unlock()
	if s.pq.Len() == 0 {
		return manualItem{}, false
	}
	return heap.Pop(&s.pq).(manualItem), true
}

// RecoverManualQueued 启动恢复:把重启前残留的 queued 手动实例重新入优先队列。
//
// pq 是纯内存,重启即丢;而 queued 实例不被 reaper/RetryPump 捞(ListGeneralizedActive 只看
// waiting_receive/running,ListRetryDue 只看 failed),无人推进会永久滞留——违背 SubmitManual
// "落库即不丢"的承诺。本方法在启动时扫库,按 priority desc / created_at asc 重建 seq 入队,
// 保证恢复后顺序稳定。应在 main 启动、MarkStaleActiveAsFailed 之后、RunManualDispatcher 之前调用。
func (s *Scheduler) RecoverManualQueued() error {
	list, err := s.store.Instance.ListManualQueued()
	if err != nil {
		return err
	}
	if len(list) == 0 {
		return nil
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Priority != list[j].Priority {
			return list[i].Priority > list[j].Priority
		}
		return list[i].CreatedAt.Before(list[j].CreatedAt)
	})
	s.pqMu.Lock()
	defer s.pqMu.Unlock()
	for i := range list {
		ins := list[i]
		job, err := s.store.Job.Get(ins.AppID, ins.JobID)
		if err != nil {
			s.log.Warn("恢复 queued 实例时 job 不存在,跳过", "instance_id", ins.ID, "err", err)
			continue
		}
		s.pqSeq++
		heap.Push(&s.pq, manualItem{job: job, ins: &ins, priority: ins.Priority, seq: s.pqSeq})
	}
	if n := s.pq.Len(); n > 0 {
		s.log.Info("已恢复重启前排队的手动实例", "count", n)
	}
	return nil
}

// ===== 任务级并发槽 =====

func (s *Scheduler) tryAcquire(jobID int64, max int) bool {
	if max < 1 {
		max = 1
	}
	s.slotMu.Lock()
	defer s.slotMu.Unlock()
	if s.slots[jobID] >= max {
		return false
	}
	s.slots[jobID]++
	return true
}

// releaseByJob 按 jobID 释放一个槽(实例未创建 / 未绑定时用)。
func (s *Scheduler) releaseByJob(jobID int64) {
	s.slotMu.Lock()
	if s.slots[jobID] <= 1 {
		delete(s.slots, jobID)
	} else {
		s.slots[jobID]--
	}
	s.slotMu.Unlock()
	s.notifyWake() // 槽空出:唤醒手动派发器重试排队实例
}

// bindHeld 将"已 tryAcquire 的槽"绑定到实例(不重复 +1,仅记映射),供终态按实例释放。
func (s *Scheduler) bindHeld(insID, jobID int64) {
	s.slotMu.Lock()
	s.held[insID] = jobID
	s.slotMu.Unlock()
}

// releaseByInstance 按实例释放其绑定的槽(幂等:未绑定则 no-op)。
func (s *Scheduler) releaseByInstance(insID int64) {
	s.slotMu.Lock()
	jobID, ok := s.held[insID]
	if ok {
		delete(s.held, insID)
		if s.slots[jobID] <= 1 {
			delete(s.slots, jobID)
		} else {
			s.slots[jobID]--
		}
	}
	s.slotMu.Unlock()
	s.notifyWake() // 槽空出:唤醒手动派发器重试排队实例
}

// notifyWake 非阻塞唤醒手动派发器:新实例入队(SubmitManual)或槽释放(终态/失败)时调用。
// wake 为 cap=1 的 channel,合并多次信号(派发器醒来会 pop 尽可能多的可派实例)。
func (s *Scheduler) notifyWake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// ReleaseInFlight worker 回报终态 / reaper 转移时调用,释放该实例占用的槽(幂等)。
func (s *Scheduler) ReleaseInFlight(insID int64) {
	s.releaseByInstance(insID)
}

// ===== 失败转移 reaper =====
//
// worker 收到任务后异步执行并回报。若 worker 崩溃/网络分区/静默死亡,实例会永久停在
// waiting_receive/running,形成"任务吊死"。reaper 周期扫描,对"绑定 worker 已失联"或
// "执行超过 job.TimeoutSec"的实例标记 failed 并触发服务端重试。
// at-least-once:worker 迟到的成功回报可能与重派实例并存,业务需自行幂等。

// RunInstanceReaper 周期扫描未终结实例做失败转移,直到 ctx 取消。
func (s *Scheduler) RunInstanceReaper(ctx context.Context, reg *workerreg.Registry) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reapOnce(reg)
		}
	}
}

func (s *Scheduler) reapOnce(reg *workerreg.Registry) {
	list, err := s.store.Instance.ListGeneralizedActive()
	if err != nil {
		s.log.Error("reaper 扫描失败", "err", err)
		return
	}
	now := time.Now()
	for i := range list {
		ins := list[i]
		job, err := s.store.Job.Get(ins.AppID, ins.JobID)
		if err != nil {
			s.finalizeReaped(&ins, "job 不存在,失败转移")
			continue
		}
		if reason := s.stallReason(&ins, job, reg, now); reason != "" {
			s.finalizeReaped(&ins, reason)
		}
	}
}

// finalizeReaped 标记实例 failed + 释放槽 + 调度重试(设 next_retry_time)。
func (s *Scheduler) finalizeReaped(ins *domain.Instance, reason string) {
	if err := s.store.Instance.UpdateResult(ins.ID, domain.StatusFailed, reason); err != nil {
		s.log.Error("reaper 标记失败", "instance_id", ins.ID, "err", err)
		return
	}
	s.appendLogRaw(ins, "REAP", "error", "失败转移: "+reason)
	s.ReleaseInFlight(ins.ID)
	s.scheduleRetry(ins)
}

// stallReason 判定实例是否卡死。空串=未卡死。
func (s *Scheduler) stallReason(ins *domain.Instance, job *domain.Job, reg *workerreg.Registry, now time.Time) string {
	if ins.WorkerAddress == "" {
		return "实例缺少 worker 绑定"
	}
	if reg != nil && !reg.IsOnline(ins.AppID, ins.WorkerAddress) {
		return "worker 失联(心跳超时)"
	}
	if job.TimeoutSec > 0 && ins.StartTime != nil {
		if now.Sub(*ins.StartTime) > time.Duration(job.TimeoutSec)*time.Second {
			return "实例执行超时(>" + strconv.Itoa(job.TimeoutSec) + "s)"
		}
	}
	return ""
}

// ===== DB 驱动重试 RetryPump =====

// RunRetryPump 周期扫描 failed 且 next_retry_time 到期的实例,按 retryIndex+1 重派。直到 ctx 取消。
func (s *Scheduler) RunRetryPump(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.retryOnce(ctx)
		}
	}
}

func (s *Scheduler) retryOnce(ctx context.Context) {
	list, err := s.store.Instance.ListRetryDue(time.Now(), s.limit)
	if err != nil {
		s.log.Error("retry 扫描失败", "err", err)
		return
	}
	for i := range list {
		ins := list[i]
		got, err := s.store.Instance.ClearNextRetryTime(ins.ID) // 原子去重
		if err != nil || !got {
			continue
		}
		job, err := s.store.Job.Get(ins.AppID, ins.JobID)
		if err != nil {
			continue
		}
		if ins.RetryIndex >= job.RetryCount {
			continue // 已达上限
		}
		s.retryInstance(ctx, job, &ins)
	}
}

// retryInstance 按 ins.RetryIndex+1 创建重试实例并派发;复用原实例的 root(链首)。
func (s *Scheduler) retryInstance(ctx context.Context, job *domain.Job, orig *domain.Instance) {
	if !s.tryAcquire(job.ID, job.MaxConcurrency) {
		// 槽满:短延后再设 next_retry_time,RetryPump 下轮重试(不丢)
		_ = s.store.Instance.SetNextRetryTime(orig.ID, time.Now().Add(time.Second))
		return
	}
	retryIns := &domain.Instance{
		JobID:             job.ID,
		AppID:             job.AppID,
		Status:            domain.StatusQueued,
		TriggerType:       "retry",
		Priority:          orig.Priority,
		RetryIndex:        orig.RetryIndex + 1,
		RootInstanceID:    domain.RootOf(orig),
		TriggerTime:       time.Now(),
		Tag:               orig.Tag,
		JobInstanceParams: orig.JobInstanceParams,
	}
	if err := s.store.Instance.Create(retryIns); err != nil {
		s.log.Error("创建重试实例失败", "orig", orig.ID, "err", err)
		s.releaseByJob(job.ID)
		return
	}
	s.appendLogRaw(retryIns, "RETRY", "info",
		"重试派发 retry_index="+strconv.Itoa(retryIns.RetryIndex)+" (from "+strconv.FormatInt(orig.ID, 10)+")")
	s.bindHeld(retryIns.ID, job.ID)
	res := s.executor.Dispatch(ctx, job, retryIns)
	if res.Accepted {
		_ = s.store.Instance.MarkDispatched(retryIns.ID, res.WorkerAddress)
	} else {
		_ = s.store.Instance.UpdateResult(retryIns.ID, domain.StatusFailed, res.Reason)
		s.releaseByInstance(retryIns.ID)
	}
}

// scheduleRetry failed 实例若仍有重试余力,设 next_retry_time 由 RetryPump 重派。
func (s *Scheduler) scheduleRetry(ins *domain.Instance) {
	job, err := s.store.Job.Get(ins.AppID, ins.JobID)
	if err != nil || job.RetryCount <= 0 || ins.RetryIndex >= job.RetryCount {
		return
	}
	interval := time.Duration(job.RetryIntervalSec) * time.Second
	if interval < time.Second {
		interval = time.Second
	}
	if err := s.store.Instance.SetNextRetryTime(ins.ID, time.Now().Add(interval)); err != nil {
		s.log.Error("设定重试时间失败", "instance_id", ins.ID, "err", err)
	}
}

// ===== helpers =====

func (s *Scheduler) appendLog(job *domain.Job, ins *domain.Instance, kind, level, msg string) {
	if s.il == nil {
		return
	}
	s.il.Append(ins.AppID, ins.ID, ins.RootInstanceID, instancelog.LogEntry{
		Time: time.Now(), Kind: kind, Level: level, Message: msg,
	})
}

// appendLogRaw 仅需实例的日志埋点(reaper/retry 路径,无 job 上下文)。
func (s *Scheduler) appendLogRaw(ins *domain.Instance, kind, level, msg string) {
	s.appendLog(nil, ins, kind, level, msg)
}

// computeNextRun 按 ScheduleKind 推算下次执行时间;manual/run_at 等一次性类型返回 nil(停止自动调度)。
func computeNextRun(job *domain.Job, from time.Time) *time.Time {
	switch job.ScheduleKind {
	case "cron":
		if n, err := schedtime.NextCron(job.ScheduleExpr, from); err == nil {
			return &n
		}
	case "fix_rate", "fix_delay":
		if ms, err := strconv.ParseInt(job.ScheduleExpr, 10, 64); err == nil && ms > 0 {
			n := from.Add(time.Duration(ms) * time.Millisecond)
			return &n
		}
	case "delay":
		if sec, err := strconv.Atoi(job.ScheduleExpr); err == nil && sec > 0 {
			n := from.Add(time.Duration(sec) * time.Second)
			return &n
		}
	}
	return nil
}
