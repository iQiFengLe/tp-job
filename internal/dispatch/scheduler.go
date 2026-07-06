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
	store     *repository.Store
	executor  domain.Executor
	il        *instancelog.Logger
	log       *slog.Logger
	cbBuilder CallbackBuilder // 实例状态变更回调构造(nil=Noop,走原路径)

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

	// wg 跟踪由 Start 启动的循环及派发子协程(execute/runManualHeld),供 Wait 优雅关闭:
	// main cancel 后等它们退出再关 DB,避免关闭期 DB 写入与 sqlDB.Close 竞态。
	wg sync.WaitGroup

	// timers 跟踪 SubmitManualDelayed 的延迟入队 timer(delay>0),Wait 前 stopTimers 统一取消,
	// 避免关闭/测试结束后回调悬挂触发、持有 ins/job 指针妨碍 GC(pushPending 本身不碰 DB,无竞态)。
	timerMu sync.Mutex
	timers  map[int64]*time.Timer
}

// NewScheduler 创建调度器。interval 为扫描周期。
func NewScheduler(st *repository.Store, exec domain.Executor, il *instancelog.Logger, interval time.Duration, log *slog.Logger, cbBuilder CallbackBuilder) *Scheduler {
	if interval <= 0 {
		interval = time.Second
	}
	if cbBuilder == nil {
		cbBuilder = NoopCallbackBuilder{}
	}
	return &Scheduler{
		store: st, executor: exec, il: il, log: log, cbBuilder: cbBuilder,
		interval: interval, limit: 500,
		wake:  make(chan struct{}, 1),
		slots: make(map[int64]int), held: make(map[int64]int64),
		timers: make(map[int64]*time.Timer),
	}
}

// Start 启动四个后台循环(定时调度 / 手动派发 / reaper / retry),全部纳入 wg 跟踪。reg 为 reaper
// 判定 worker 在线性所需。main 应在 HTTP 启动前调用;优雅关闭时 cancel ctx 后调 Wait。
func (s *Scheduler) Start(ctx context.Context, reg *workerreg.Registry) {
	s.goTrack(func() { s.Run(ctx) })
	s.goTrack(func() { s.RunManualDispatcher(ctx) })
	s.goTrack(func() { s.RunInstanceReaper(ctx, reg) })
	s.goTrack(func() { s.RunRetryPump(ctx) })
}

// Wait 阻塞至所有 Start 启动的循环及在飞派发协程退出。应在 ctx 取消后调用,并配合超时
// 以防某轮 runOnce 卡住拖死关闭进程。先 stopTimers 取消未触发的延迟入队 timer,再等协程。
func (s *Scheduler) Wait() {
	s.stopTimers()
	s.wg.Wait()
}

// stopTimers 取消所有未触发的延迟入队 timer,避免关闭/测试结束后回调悬挂(回调持有 ins/job 指针
// 妨碍 GC)。已触发或正在执行的 timer.Stop 为 no-op,其回调自行收尾(pushPending 安全)。
func (s *Scheduler) stopTimers() {
	s.timerMu.Lock()
	for _, t := range s.timers {
		t.Stop()
	}
	s.timers = make(map[int64]*time.Timer)
	s.timerMu.Unlock()
}

// goTrack 启动一个受 wg 跟踪的 goroutine,供优雅关闭统一等待(含派发子协程 execute/runManualHeld)。
func (s *Scheduler) goTrack(fn func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn()
	}()
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
	s.goTrack(func() { s.execute(ctx, job, oldNext) })
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

	now := time.Now()
	newNext, nerr := schedtime.NextByKind(job.ScheduleKind, job.ScheduleExpr, now)
	if nerr != nil {
		// 表达式非法(被改坏/边界解析失败):不静默置 nil 停摆——先 error 告警,再置 nil 暂停
		// 该 job 自动调度,等管理员修表达式后 update 触发重算(比旧版静默吞错多了告警)。
		s.log.Error("推算下次执行失败,暂停该 job 自动调度",
			"job_id", job.ID, "kind", job.ScheduleKind, "expr", job.ScheduleExpr, "err", nerr)
		newNext = nil
	}

	// 生效窗口(可选 StartTime/EndTime):仅约束自动调度,手动触发不受限。
	// 窗口外不创建实例,但仍推进游标——start 前一次性跳到窗口开始(避免每 tick 空耗),
	// end 后置 nil 停摆(保持 enabled,改 end_time 可续期)。
	inWindow := true
	if job.StartTime != nil && now.Before(*job.StartTime) {
		inWindow = false
		if newNext == nil || newNext.Before(*job.StartTime) {
			newNext = job.StartTime
		}
	}
	if job.EndTime != nil && now.After(*job.EndTime) {
		inWindow = false
		newNext = nil
	}

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
	if !inWindow {
		s.releaseByJob(job.ID)
		s.log.Info("生效窗口外,跳过自动调度", "job_id", job.ID, "has_next", newNext != nil)
		return // 游标已推进,本次不创建实例
	}

	ins = &domain.Instance{
		JobID:             job.ID,
		AppID:             job.AppID,
		Status:            domain.StatusQueued,
		TriggerType:       "auto",
		TriggerTime:       now,
		Tag:               job.Tag,
		JobInstanceParams: job.JobParams, // 定时触发:实例参数默认=任务参数
	}
	if err := s.store.Instance.CreateWithCallback(ins, func() *domain.Callback {
		return s.cbBuilder.Build(ins, job, domain.StatusQueued)
	}); err != nil {
		s.log.Error("创建实例失败", "job_id", job.ID, "err", err)
		s.releaseByJob(job.ID)
		return
	}
	s.appendLog(job, ins, "CREATE", "info", "实例创建")
	s.bindHeld(ins.ID, job.ID) // 先绑:保证后续任一终态路径能经 ReleaseInFlight 释放

	// 选后即绑派发:PickWorker → MarkDispatched → Send,任一失败由 dispatchToWorker 统一善后
	// (UpdateResult failed + 衔接 RetryPump 重试;RetryCount=0 时 scheduleRetry 定格 failed 无副作用,
	// 故对不重试的 job 行为不变)。prefix=「派发」用于 STATUS/DISPATCH 日志文案。
	if s.dispatchToWorker(ctx, job, ins, "派发") {
		return
	}
	// 槽随实例到终态(不释放)
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
func (h pqHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *pqHeap) Push(x any)   { *h = append(*h, x.(manualItem)) }
func (h *pqHeap) Pop() any     { old := *h; x := old[len(old)-1]; *h = old[:len(old)-1]; return x }

// SubmitManual 立即手动触发(落库 queued + 入优先队列)。返回 error(落库失败时非 nil)。
// 不关心 instanceId 的内部调用方用此;需立即拿到 instanceId 的外部触发(OpenAPI runJob)用 SubmitManualDelayed。
// 重启后残留的 queued 实例由 RecoverQueued 恢复。
func (s *Scheduler) SubmitManual(job *domain.Job, priority int, instanceParams string) error {
	_, err := s.SubmitManualDelayed(job, priority, instanceParams, 0)
	return err
}

// SubmitManualDelayed 手动触发并返回实例 ID。delay>0 时延迟入队——立即落库返回 ID(客户端可立即
// 拿到 instanceId),到点才真正入优先队列派发;对齐 PowerJob OpenAPI runJob 的 delay 语义。
//
// 延迟入队用进程内 timer:进程运行期完全正确;重启时未触发的延迟丢失,但实例已落库 queued,
// 由 RecoverQueued 兜底(重启后立即入队派发——延迟语义丢失但实例不丢,at-least-once)。
func (s *Scheduler) SubmitManualDelayed(job *domain.Job, priority int, instanceParams string, delay time.Duration) (int64, error) {
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
	if err := s.store.Instance.CreateWithCallback(ins, func() *domain.Callback {
		return s.cbBuilder.Build(ins, job, domain.StatusQueued)
	}); err != nil {
		return 0, fmt.Errorf("创建 queued 实例失败: %w", err)
	}
	s.appendLog(job, ins, "CREATE", "info", "手动触发排队")
	s.enqueueManual(ins, job, priority, delay)
	return ins.ID, nil
}

// enqueueManual 把已落库的 queued 实例加入优先队列。delay<=0 立即入队;delay>0 起 timer 到点入队。
func (s *Scheduler) enqueueManual(ins *domain.Instance, job *domain.Job, priority int, delay time.Duration) {
	if delay <= 0 {
		s.pushPending(ins, job, priority)
		return
	}
	// 起 timer 到点入队,并登记到 timers 供 Wait 前 stopTimers 取消(防关闭后悬挂回调)。
	// 回调执行后自删;重启时未触发的延迟丢失,但实例已落库 queued,由 RecoverQueued 兜底。
	t := time.AfterFunc(delay, func() {
		s.pushPending(ins, job, priority)
		s.timerMu.Lock()
		delete(s.timers, ins.ID)
		s.timerMu.Unlock()
	})
	s.timerMu.Lock()
	s.timers[ins.ID] = t
	s.timerMu.Unlock()
}

// pushPending 入队一条手动实例并唤醒派发器。
func (s *Scheduler) pushPending(ins *domain.Instance, job *domain.Job, priority int) {
	s.pqMu.Lock()
	s.pqSeq++
	heap.Push(&s.pq, manualItem{job: job, ins: ins, priority: priority, seq: s.pqSeq})
	s.pqMu.Unlock()
	s.notifyWake()
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
		s.goTrack(func() { s.runManualHeld(ctx, item) })
	}
}

func (s *Scheduler) runManualHeld(ctx context.Context, item manualItem) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("手动触发 panic", "job_id", item.job.ID, "panic", r)
			s.releaseByInstance(item.ins.ID)
		}
	}()
	// 派发前查 DB:延迟入队(time.AfterFunc)或排队期间,实例可能已被 OpenAPI stop/cancel 改成终态。
	// 内存 manualItem.ins 是入队快照,不反映后续状态变更——若已终态则放弃派发,归还 tryAcquire 抢的
	// job 槽(此时尚未 bindHeld,故用 releaseByJob 而非 releaseByInstance),避免把"已停止"的实例下发 worker。
	if cur, err := s.store.Instance.Get(item.ins.ID); err != nil || domain.StatusTerminal(cur.Status) {
		status := ""
		if err == nil {
			status = cur.Status
		}
		s.appendLog(item.job, item.ins, "STATUS", "info", "派发前实例已终态("+status+"),跳过派发")
		s.releaseByJob(item.job.ID)
		return
	}
	s.bindHeld(item.ins.ID, item.job.ID)
	// 选后即绑派发(同 execute,详见 dispatchToWorker)。
	if s.dispatchToWorker(ctx, item.job, item.ins, "手动触发派发") {
		return
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

// RecoverQueued 启动恢复:把重启前残留的 queued 实例(任意 trigger_type)重新入优先队列。
//
// pq 是纯内存,重启即丢;而 queued 实例不被 reaper(只看 waiting_receive/running)/
// RetryPump(只看 failed)捞,无人推进会永久滞留——违背 SubmitManual "落库即不丢"的承诺。
// auto/retry 触发路径(Create(queued) 与 Dispatch 之间)崩溃同样残留 queued,故一并恢复。
// 按 priority desc / created_at asc 重建 seq 入队,保证恢复后顺序稳定。
// 应在 main 启动、RecoverStaleActive 之后、RunManualDispatcher 之前调用。
func (s *Scheduler) RecoverQueued() error {
	list, err := s.store.Instance.ListQueued()
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
		s.log.Info("已恢复重启前排队的实例", "count", n)
	}
	return nil
}

// RecoverStaleActive 启动清理:把重启前未终结(waiting_receive/running)的实例做失败转移。
//
// 与 reaper 同语义:UpdateResult(failed) + scheduleRetry——后者对有重试余力的实例设 next_retry_time,
// 由 RetryPump 接管重派(重启不丢);无余力则定格终态 failed。取代旧的 bulk MarkStaleActiveAsFailed:
// 旧版只标 failed 不衔接重试,导致配了 RetryCount 的 job 在重启窗口内的在飞实例被静默放弃。
// at-least-once:重派可能与原 worker 的迟到回报并存,业务需幂等。应在 main 启动、RecoverQueued 之前调用。
func (s *Scheduler) RecoverStaleActive() error {
	list, err := s.store.Instance.ListGeneralizedActive(s.limit)
	if err != nil {
		return err
	}
	jobs := s.loadJobs(list) // 批量预加载,消除 scheduleRetry 的逐实例 Job.Get
	for i := range list {
		ins := list[i]
		ins.Status = domain.StatusFailed
		ins.Result = "服务重启前未完成"
		job := jobs[ins.JobID]
		rows, err := s.store.Instance.UpdateResultWithCallback(ins.ID, domain.StatusFailed, "服务重启前未完成", s.cbBuilder.Build(&ins, job, domain.StatusFailed))
		if err != nil {
			s.log.Error("重启清理实例失败", "instance_id", ins.ID, "err", err)
			continue
		}
		if rows == 0 {
			continue // 已被并发终结(worker 迟到回报等),不重复 appendLog/scheduleRetry
		}
		s.appendLogRaw(&ins, "REAP", "error", "失败转移: 服务重启前未完成")
		s.scheduleRetry(&ins, job)
	}
	if n := len(list); n > 0 {
		s.log.Info("已清理重启前未终结的实例(有重试余力的将由 RetryPump 重派)", "count", n)
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

// loadJobs 批量预加载给定实例集合涉及的 job(map[jobID]*Job),供 reaper/retry 消除逐实例
// Job.Get 的 N+1 查询。加载失败返回空 map(调用方按 nil job 兜底为"job 不存在")。
func (s *Scheduler) loadJobs(list []domain.Instance) map[int64]*domain.Job {
	ids := make(map[int64]struct{}, len(list))
	for i := range list {
		ids[list[i].JobID] = struct{}{}
	}
	if len(ids) == 0 {
		return nil
	}
	flat := make([]int64, 0, len(ids))
	for id := range ids {
		flat = append(flat, id)
	}
	jobs, err := s.store.Job.ListByIDs(flat)
	if err != nil {
		s.log.Error("批量加载 job 失败", "err", err)
		return nil
	}
	m := make(map[int64]*domain.Job, len(jobs))
	for i := range jobs {
		m[jobs[i].ID] = &jobs[i]
	}
	return m
}

func (s *Scheduler) reapOnce(reg *workerreg.Registry) {
	// 1. 扫描 waiting_receive/running 卡死实例
	list, err := s.store.Instance.ListGeneralizedActive(s.limit)
	if err != nil {
		s.log.Error("reaper 扫描失败", "err", err)
		return
	}
	if len(list) > 0 {
		jobs := s.loadJobs(list) // 批量预加载,消除 N+1
		now := time.Now()
		for i := range list {
			ins := list[i]
			job := jobs[ins.JobID]
			if job == nil {
				s.finalizeReaped(&ins, "job 不存在,失败转移", nil)
				continue
			}
			if reason := s.stallReason(&ins, job, reg, now); reason != "" {
				s.finalizeReaped(&ins, reason, job)
			}
		}
	}

	// 2. 扫描 worker 无法处理已解绑的实例(queued 且 worker_address=null 且超 30s)
	unboundList, err := s.store.Instance.ListUnboundQueued(30 * time.Second)
	if err != nil {
		s.log.Error("reaper 扫描解绑实例失败", "err", err)
		return
	}
	if len(unboundList) > 0 {
		unboundJobs := s.loadJobs(unboundList)
		for i := range unboundList {
			ins := unboundList[i]
			job := unboundJobs[ins.JobID]
			if job == nil {
				s.finalizeReaped(&ins, "job 不存在,失败转移", nil)
				continue
			}
			s.finalizeReaped(&ins, "worker 无法处理已解绑", job)
		}
	}
}

// finalizeReaped 标记实例 failed + 释放槽 + 调度重试(设 next_retry_time)。
// job 透传给 scheduleRetry(reaper 路径已 loadJobs,避免重复 Job.Get);nil 时 scheduleRetry 自查。
func (s *Scheduler) finalizeReaped(ins *domain.Instance, reason string, job *domain.Job) {
	ins.Status = domain.StatusFailed
	ins.Result = reason                                    // payload 快照
	cb := s.cbBuilder.Build(ins, job, domain.StatusFailed) // job 可能为 nil(reaper 兜底分支),Build 返回 nil
	rows, err := s.store.Instance.UpdateResultWithCallback(ins.ID, domain.StatusFailed, reason, cb)
	if err != nil {
		s.log.Error("reaper 标记失败", "instance_id", ins.ID, "err", err)
		return
	}
	if rows == 0 {
		return // 已被并发终结(worker 迟到回报 / RecoverStaleActive),不重复 Release/scheduleRetry
	}
	s.appendLogRaw(ins, "REAP", "error", "失败转移: "+reason)
	s.ReleaseInFlight(ins.ID)
	s.scheduleRetry(ins, job)
}

// stallReason 判定实例是否卡死。空串=未卡死。
//
// 兜底优先级:worker 未绑定 → worker 失联 → 执行超时。执行超时仅在 job.TimeoutSec>0 时生效:
// TimeoutSec=0 表示"不限执行时长"(长任务语义),此时若 worker 持续心跳(在线)却永不推进,
// 实例会停在 waiting_receive/running 不被回收——故生产强烈建议为 job 配置合理的 TimeoutSec,
// 否则唯一能兜底的只有 worker 心跳真的停掉(失联判定)。
func (s *Scheduler) stallReason(ins *domain.Instance, job *domain.Job, reg *workerreg.Registry, now time.Time) string {
	if ins.WorkerAddress == "" {
		// 选后即绑后,正常派发不会出现「已派发态却无 worker 绑定」:MarkDispatched 先写 worker_address
		// 再置 waiting_receive。命中此分支必为异常(worker 对未绑定实例乱回报 / SetStatus / 迁移脏数据),
		// 应立即回收(→ reaper failed → scheduleRetry)。不再给 30s 宽限:它既兜不住崩溃恢复
		// (RecoverStaleActive 在重启时接管,此时 TriggerTime 已远超窗口),反而延迟真正卡死实例的检测。
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
	if len(list) == 0 {
		return
	}
	jobs := s.loadJobs(list) // 批量预加载,消除 N+1
	for i := range list {
		ins := list[i]
		got, err := s.store.Instance.ClearNextRetryTime(ins.ID) // 原子去重
		if err != nil || !got {
			continue
		}
		job := jobs[ins.JobID]
		if job == nil {
			continue // job 不存在
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
	if err := s.store.Instance.CreateWithCallback(retryIns, func() *domain.Callback {
		return s.cbBuilder.Build(retryIns, job, domain.StatusQueued)
	}); err != nil {
		s.log.Error("创建重试实例失败", "orig", orig.ID, "err", err)
		s.releaseByJob(job.ID)
		return
	}
	s.appendLogRaw(retryIns, "RETRY", "info",
		"重试派发 retry_index="+strconv.Itoa(retryIns.RetryIndex)+" (from "+strconv.FormatInt(orig.ID, 10)+")")
	s.bindHeld(retryIns.ID, job.ID)
	// 选后即绑派发(同 execute,详见 dispatchToWorker)。
	if s.dispatchToWorker(ctx, job, retryIns, "重试派发") {
		return
	}
}

// scheduleRetry failed 实例若仍有重试余力,设 next_retry_time 由 RetryPump 重派。
// job 为调用方预加载的 *Job(reaper/recover 路径已批量 loadJobs,避免重复查询);nil 时内部自查。
func (s *Scheduler) scheduleRetry(ins *domain.Instance, job *domain.Job) {
	if job == nil {
		var err error
		job, err = s.store.Job.Get(ins.AppID, ins.JobID)
		if err != nil {
			return
		}
	}
	if job.RetryCount <= 0 || ins.RetryIndex >= job.RetryCount {
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

// dispatchToWorker 执行「选后即绑」派发:PickWorker → MarkDispatched → Send。
// 选定后立即 MarkDispatched 绑定 worker_address(先于 POST),消除「worker 回报 running 早于绑定」的竞态。
// 任一步失败由 failDispatch 统一善后(UpdateResult failed + STATUS 日志 + 释放槽 + 衔接 RetryPump 重试)并返回 true;
// 调用方收到 true 应直接 return。prefix 标识来源(「派发」/「手动触发派发」/「重试派发」),用于日志文案。
// RetryCount=0 时 failDispatch 内 scheduleRetry 定格 failed 无副作用,故对不重试的 job 行为不变。
func (s *Scheduler) dispatchToWorker(ctx context.Context, job *domain.Job, ins *domain.Instance, prefix string) (failed bool) {
	addr, protocol, ok := s.executor.PickWorker(job, ins)
	if !ok {
		s.failDispatch(ins, job, prefix, "无可用 worker(tag 不匹配或全部离线)")
		return true
	}
	ins.WorkerAddress = addr // payload 快照用(DB 由 MarkDispatched 写)
	rows, err := s.store.Instance.MarkDispatchedWithCallback(ins.ID, addr, s.cbBuilder.Build(ins, job, domain.StatusWaitingReceive))
	if err != nil {
		s.log.Error("标记派发失败", "instance_id", ins.ID, "err", err)
		s.failDispatch(ins, job, prefix, "绑定 worker 失败: "+err.Error())
		return true
	}
	if rows == 0 {
		// 终态守护触发:实例在 Get 后被并发 stop/cancel 写终态,MarkDispatched 未改 status。
		// 此时不可 Send(否则 worker 会执行一个已被停止的实例),也不可 failDispatch/scheduleRetry
		// (会把已终态实例重新拉进重试链)。仅释放本次派发占用的飞行槽并记日志后中止。
		s.log.Warn(prefix+"中止:实例已被并发置终态", "instance_id", ins.ID)
		s.appendLogRaw(ins, "STATUS", "warn", prefix+"中止:实例已被并发置终态")
		s.releaseByInstance(ins.ID)
		return true
	}
	s.appendLogRaw(ins, "DISPATCH", "info", prefix+"到 worker "+addr)
	if err := s.executor.Send(ctx, addr, protocol, job, ins); err != nil {
		s.failDispatch(ins, job, prefix, err.Error())
		return true
	}
	return false
}

// failDispatch 派发失败统一善后:标记 failed(清 worker 绑定)+ 记 STATUS 日志 + 释放槽 + 衔接重试。
func (s *Scheduler) failDispatch(ins *domain.Instance, job *domain.Job, prefix, reason string) {
	ins.Status = domain.StatusFailed
	ins.Result = reason // payload 快照
	// 选后即绑下 Send 失败时 worker_address 已先于 POST commit;派发失败该绑定无意义,
	// 用 FailDispatchWithCallback 在同事务清 worker_address/start_time + 置 failed,避免
	// "failed 实例仍指向某 worker"的展示残留(与 worker 回报 failed 保留 worker 供审计区分)。
	if _, err := s.store.Instance.FailDispatchWithCallback(ins.ID, reason, s.cbBuilder.Build(ins, job, domain.StatusFailed)); err != nil {
		s.log.Error("标记派发失败失败", "instance_id", ins.ID, "err", err)
	}
	s.appendLogRaw(ins, "STATUS", "error", prefix+"失败→failed: "+reason)
	s.releaseByInstance(ins.ID)
	s.scheduleRetry(ins, job)
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
