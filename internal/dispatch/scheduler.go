package dispatch

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"tp-job/internal/domain"
	"tp-job/internal/instancelog"
	"tp-job/internal/repository"
	"tp-job/internal/schedtime"
	"tp-job/internal/workerreg"
)

// Scheduler domain 调度器:周期扫描到期 Job → 认领(AdvanceNextRun 乐观锁)→ 经 Executor 派发。
//
// 任务级并发槽随实例生命周期(派发成功后绑定到实例,worker 回报终态 / reaper 转移才经
// ReleaseInFlight 释放)。定时触发固定串行(同 job 有实例在飞则跳过本次到期,不推进游标)。
//
// 失败兜底两条:
//   - RunInstanceReaper:扫 waiting_receive/running 实例,worker 失联/未绑定 → failed;执行超 TimeoutSec → timeout;均重派。
//   - RunRetryPump:扫 failed/timeout 且 next_retry_time 到期的实例,按 retryIndex+1 重派(DB 驱动,重启不丢)。
type Scheduler struct {
	store     *repository.Store
	executor  domain.Executor
	il        *instancelog.Logger
	log       *slog.Logger
	cbBuilder CallbackBuilder // 实例状态变更回调构造(nil=Noop,走原路径)

	interval time.Duration
	limit    int

	// 手动触发优先队列
	pqMu    sync.Mutex
	pq      pqHeap
	pqIndex map[int64]*manualItem // instanceID → 堆内 item 指针(调整优先级定位用);Push 写、Pop/re-push 删
	pqSeq   int64
	wake    chan struct{}

	slotMu sync.Mutex
	slots  map[int64]int      // jobID -> 在飞实例计数(auto+manual 共享)
	held   map[int64]heldSlot // instanceID -> 绑定信息(供终态按实例释放槽 + worker 在飞计数)

	// wg 跟踪由 Start 启动的循环及派发子协程(execute/runManualHeld),供 Wait 优雅关闭:
	// main cancel 后等它们退出再关 DB,避免关闭期 DB 写入与 sqlDB.Close 竞态。
	wg sync.WaitGroup

	// timers 跟踪 SubmitManualDelayed 的延迟入队 timer(delay>0),Wait 前 stopTimers 统一取消,
	// 避免关闭/测试结束后回调悬挂触发、持有 ins/job 指针妨碍 GC(pushPending 本身不碰 DB,无竞态)。
	timerMu sync.Mutex
	timers  map[int64]*time.Timer

	// warmup reaper 启动宽限:此窗口内 stallReason 跳过"worker 失联"判定——服务重启后 workerreg 是空的,
	// worker 需一个心跳周期才重新注册,直接判失联会误杀所有"重启前在飞"的正常实例(worker 迟到 success 被
	// 终态守护拒绝 → 重复执行)。零值=不启用(测试/未配置路径,保持旧行为);生产由 SetReaperWarmup 注入。
	// startedAt 取 NewScheduler 时刻作为 warmup 起算点(覆盖装配到 reaper 首轮的全程)。
	warmup    time.Duration
	startedAt time.Time

	// receiveTimeout:实例停在 waiting_receive(已派发但 worker 从未回报 running/终态)超过此阈值即判
	// failed 重派——worker 收到 /run 后迟迟未拉起执行(繁忙队列堆积/卡住/掉单/上报丢失)。仅 waiting_receive
	// 生效(running 已确认在执行,走执行超时 TimeoutSec)。零值=关闭:兼容 waiting_receive→success 直跳、
	// 从不报 running 的旧 worker(开启会误杀其长任务)。生产由 SetReceiveTimeout 注入(默认 60s)。
	receiveTimeout time.Duration

	// reg worker 注册表句柄(Start 时注入):派发成功 AcquireInflight / 终态 ReleaseInflight,
	// 维护 worker 在飞计数供 PickFull 负载感知选址(在飞少的优先)。nil(测试/未 Start)时跳过计数,
	// bindWorkerAddr/releaseByInstance 均守护 nil,不影响任务级并发槽逻辑。
	reg *workerreg.Registry
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
		wake:    make(chan struct{}, 1),
		slots:   make(map[int64]int), held: make(map[int64]heldSlot),
		pqIndex: make(map[int64]*manualItem),
		timers:  make(map[int64]*time.Timer),
		startedAt: time.Now(),
	}
}

// SetReaperWarmup 设置 reaper 启动宽限(装配层从 cfg.Worker.WarmupSeconds 注入,默认 30s)。
// 不经 NewScheduler 签名传入:保持 NewScheduler 调用点(含 ~25 处测试)不变,warmup 零值=不启用=旧行为。
// <=0 关闭 warmup(仅测试或显式关闭用);生产不应关闭——关闭即重新引入"重启误杀在飞实例"的竞态。
func (s *Scheduler) SetReaperWarmup(d time.Duration) {
	s.warmup = d
}

// SetReceiveTimeout 设置 waiting_receive 接收超时(装配层从 cfg.Worker.ReceiveTimeoutSeconds 注入,默认 60s)。
// <=0 关闭——兼容从不报 running、waiting_receive→success 直跳的旧 worker(开启会误杀其长任务:实例全程
// waiting_receive 直到 success,若执行耗时长于 receiveTimeout 会被误判 failed)。生产不应关闭。
func (s *Scheduler) SetReceiveTimeout(d time.Duration) {
	s.receiveTimeout = d
}

// inWarmup 是否处于 reaper 启动宽限窗口(warmup 内 stallReason 跳过 worker 失联判定)。
func (s *Scheduler) inWarmup(now time.Time) bool {
	return s.warmup > 0 && now.Sub(s.startedAt) < s.warmup
}

// Start 启动四个后台循环(定时调度 / 手动派发 / reaper / retry),全部纳入 wg 跟踪。reg 为 reaper
// 判定 worker 在线性所需。main 应在 HTTP 启动前调用;优雅关闭时 cancel ctx 后调 Wait。
func (s *Scheduler) Start(ctx context.Context, reg *workerreg.Registry) {
	s.reg = reg // 存句柄:派发 AcquireInflight / 终态 ReleaseInflight(负载感知选址);派发循环在 Start 后才跑,来得及
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

// goTrack 启动一个受 wg 跟踪的后台 goroutine,带 panic 自愈:fn panic 时 recover+记录+短暂退避后重启 fn,
// 防单次异常数据/第三方驱动 bug 导致调度/reaper/retry 循环永久停止(静默停摆比崩进程更危险)。
// fn 正常返回(ctx 取消)则退出。派发子协程(execute/runManualHeld/retryInstance)自带 recover 且正常 return,
// 不会触发重启(它们是一次性任务,panic 已被自身 recover 兜底)。
func (s *Scheduler) goTrack(fn func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			if s.runTracked(fn) {
				return // fn 正常结束(ctx 取消);panic 路径 runTracked 返回 false,for 循环重启
			}
		}
	}()
}

// runTracked 执行 fn:正常返回 true;panic 时 recover+log+1s 退避后返回 false(供 goTrack 决定是否重启)。
// 退避防 panic 死循环刷屏;优雅关闭时正常路径不经此 sleep(fn 随 ctx 取消正常返回)。
func (s *Scheduler) runTracked(fn func()) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("后台循环 panic,1s 后重启(防调度/reaper/retry 静默停止)", "panic", r)
			time.Sleep(time.Second)
		} else {
			ok = true
		}
	}()
	fn()
	return
}

// cbBuild 返回构造回调的闭包(供 *WithCallback 在 tx 内用最新行构造 payload);回调未启用时返回 nil,
// 使仓储走无回调快捷路径(免事务开销)。job 可能为 nil(reaper 兜底分支),Build 对 nil job 返回 nil。
func (s *Scheduler) cbBuild(job *domain.Job, eventStatus string) func(*domain.Instance) *domain.Callback {
	if s.cbBuilder == nil || !s.cbBuilder.Enabled() {
		return nil
	}
	return func(latest *domain.Instance) *domain.Callback {
		return s.cbBuilder.Build(latest, job, eventStatus)
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
	if err := s.store.Instance.CreateWithCallback(ins, s.cbBuild(job, domain.StatusQueued)); err != nil {
		s.log.Error("创建实例失败", "job_id", job.ID, "err", err)
		s.releaseByJob(job.ID)
		return
	}
	s.appendLog(job, ins, "CREATE", "info", "实例创建")
	s.bindHeld(ins.ID, job.AppID, job.ID) // 先绑:保证后续任一终态路径能经 ReleaseInFlight 释放

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
	pqIdx    int // 该 item 在堆数组中的下标,供 heap.Fix 定位;由 Swap/Push 维护
}

// pqHeap 手动触发优先队列(指针堆)。指针 + item 内 pqIdx 使"调整堆内实例优先级"可 O(log n)
// 定位重排(heap.Fix)。pqIdx 不变量:恒等于元素当前下标——Swap 对双方写、Push 设初值 len 维护。
// (container/heap 的 up/down/Pop/Fix 所有位置变更只经 Swap,从不直接按下标搬数据,故此维护充分。)
type pqHeap []*manualItem

func (h pqHeap) Len() int { return len(h) }
func (h pqHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority
	}
	return h[i].seq < h[j].seq
}
func (h pqHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].pqIdx = i
	h[j].pqIdx = j
}
func (h *pqHeap) Push(x any) {
	item := x.(*manualItem)
	item.pqIdx = len(*h) // 初始下标=append 前长度;后续 up 的 Swap 持续维护
	*h = append(*h, item)
}
func (h *pqHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // 助 GC(re-push 同指针复用时旧槽置空,append 放回同一指针不冲突)
	*h = old[:n-1]
	return x
}

// SubmitManual 立即手动触发(落库 queued + 入优先队列)。返回 error(落库失败时非 nil)。
// 不关心 instanceId 的内部调用方用此;需立即拿到 instanceId 的外部触发(OpenAPI runJob)用 SubmitManualDelayed。
// 重启后残留的 queued 实例由 RecoverQueued 恢复。
func (s *Scheduler) SubmitManual(job *domain.Job, priority int, instanceParams, source string) error {
	_, err := s.SubmitManualDelayed(job, priority, instanceParams, 0, source)
	return err
}

// SubmitManualDelayed 手动触发并返回实例 ID。delay>0 时延迟入队——立即落库返回 ID(客户端可立即
// 拿到 instanceId),到点才真正入优先队列派发;对齐 PowerJob OpenAPI runJob 的 delay 语义。
//
// 延迟入队用进程内 timer:进程运行期完全正确;重启时未触发的延迟丢失,但实例已落库 queued,
// 由 RecoverQueued 兜底(重启后立即入队派发——延迟语义丢失但实例不丢,at-least-once)。
func (s *Scheduler) SubmitManualDelayed(job *domain.Job, priority int, instanceParams string, delay time.Duration, source string) (int64, error) {
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
	if err := s.store.Instance.CreateWithCallback(ins, s.cbBuild(job, domain.StatusQueued)); err != nil {
		return 0, fmt.Errorf("创建 queued 实例失败: %w", err)
	}
	// source 区分触发来源(api/openapi-runJob/openapi-runJob2);instanceParams 只记长度不记内容
	// (可能含业务敏感数据/token),priority/delayMS 非敏感直接记值,便于触发后从日志回溯。
	s.appendLog(job, ins, "CREATE", "info",
		fmt.Sprintf("手动触发排队 [来源=%s 优先级=%d 延迟=%dms 参数长度=%d]",
			source, priority, delay.Milliseconds(), len(instanceParams)))
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
	item := &manualItem{job: job, ins: ins, priority: priority, seq: s.pqSeq}
	heap.Push(&s.pq, item)
	s.pqIndex[ins.ID] = item
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
			//
			// re-push 前 Get 刷新 priority:SetPriority 可能在本 item 已 pop(脱离 pqIndex)期间改了 DB,
			// 不刷新会以旧 priority 重排,使该次调整延迟生效到下次重启。re-push 是并发满的异常路径
			// (不高频),一次 DB Get 可接受。顺带:若实例在 pop 期间已被 stop/cancel 置终态,直接丢弃
			// 不 re-push(免一次无意义入堆→再 pop→runManualHeld 终态跳过)。此处不释放槽——本分支
			// tryAcquire 本就失败(slots 未++)、实例一直 queued 从未进在飞集合,无槽可归;误调
			// releaseByJob 会减掉别人占的 slots,致 MaxConcurrency 失效超发。
			if cur, err := s.store.Instance.Get(item.ins.ID); err == nil {
				if domain.StatusTerminal(cur.Status) {
					select {
					case <-ctx.Done():
						return
					case <-s.wake:
					case <-time.After(100 * time.Millisecond):
					}
					continue
				}
				item.priority = cur.Priority
			}
			s.pqMu.Lock()
			heap.Push(&s.pq, item) // 同指针 re-push;Push 内重置 pqIdx 无残留;复用原 seq 保 FIFO 原位
			s.pqIndex[item.ins.ID] = item
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

func (s *Scheduler) runManualHeld(ctx context.Context, item *manualItem) {
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
	s.bindHeld(item.ins.ID, item.job.AppID, item.job.ID)
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

func (s *Scheduler) popPending() (*manualItem, bool) {
	s.pqMu.Lock()
	defer s.pqMu.Unlock()
	if s.pq.Len() == 0 {
		return nil, false
	}
	item := heap.Pop(&s.pq).(*manualItem)
	delete(s.pqIndex, item.ins.ID)
	return item, true
}

// UpdateQueuedPriority 调整一个 queued 实例在手动派发优先队列里的优先级,即时生效。
// 命中(实例仍在堆)则改 item.priority + heap.Fix 按 pqIdx 重排;未命中(已 pop 派发中 / 已终态 /
// 从未入队)返回 false——调用方(service.SetPriority)据此仅落 DB(DB 为权威,重启 RecoverQueued 据此重排;
// re-push 路径会从 DB 刷新 priority,故无 stale 残留)。必须持 pqMu:heap.Fix 非线程安全,
// 且 pqIndex 读写需与 Push/Pop 互斥。
func (s *Scheduler) UpdateQueuedPriority(instanceID int64, priority int) bool {
	s.pqMu.Lock()
	defer s.pqMu.Unlock()
	item, ok := s.pqIndex[instanceID]
	if !ok {
		return false
	}
	if item.priority != priority { // 无变化免 Fix
		item.priority = priority
		heap.Fix(&s.pq, item.pqIdx)
	}
	return true
}

// RecoverQueued 启动恢复:把重启前残留的 queued 实例(任意 trigger_type)重新入优先队列。
//
// pq 是纯内存,重启即丢;而 queued 实例不被 reaper(只看 waiting_receive/running)/
// RetryPump(只看 failed)捞,无人推进会永久滞留——违背 SubmitManual "落库即不丢"的承诺。
// auto/retry 触发路径(Create(queued) 与 Dispatch 之间)崩溃同样残留 queued,故一并恢复。
// 按 priority desc / created_at asc 重建 seq 入队,保证恢复后顺序稳定。
// 应在 main 启动、RecoverStaleActive 之后、RunManualDispatcher 之前调用。
func (s *Scheduler) RecoverQueued() error {
	list, err := s.store.Instance.ListQueued(5000)
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
		item := &manualItem{job: job, ins: &ins, priority: ins.Priority, seq: s.pqSeq}
		heap.Push(&s.pq, item)
		s.pqIndex[ins.ID] = item // 写索引:否则重启恢复的实例 UpdateQueuedPriority 查不到
	}
	if n := s.pq.Len(); n > 0 {
		s.log.Info("已恢复重启前排队的实例", "count", n)
	}
	if len(list) >= 5000 {
		s.log.Warn("重启前 queued 实例可能超 5000(仅恢复前 5000 防内存压力),剩余下次重启恢复;建议排查并发积压", "recovered", len(list))
	}
	return nil
}

// RecoverStaleActive 启动清理:把重启前未终结(waiting_receive/running)的实例做失败转移。
//
// 与 reaper 同语义:UpdateResult(failed) + scheduleRetry——后者对有重试余力的实例设 next_retry_time,
// 由 RetryPump 接管重派(重启不丢);无余力则定格终态 failed。取代旧的 bulk MarkStaleActiveAsFailed:
// 旧版只标 failed 不衔接重试,导致配了 RetryCount 的 job 在重启窗口内的在飞实例被静默放弃。
// at-least-once:重派可能与原 worker 的迟到回报并存,业务需幂等。应在 main 启动、RecoverQueued 之前调用。
func (s *Scheduler) RecoverStaleActive(grace time.Duration) error {
	// 仅清理"重启前已超 grace"的活跃实例(大概率真失联);近期实例交 reaper 按真实失联(心跳/TimeoutSec)
	// 判定——避免重启即批量失败转移仍在正常执行的长任务(worker 迟到 success 被终态守护拒绝 → 重复执行)。
	// grace<=0(配置漏填)用默认 10min,不退回"不限全清"——避免误配静默回退到本次修复要消灭的旧 bug。
	if grace <= 0 {
		grace = 10 * time.Minute
	}
	olderThan := time.Now().Add(-grace)
	list, err := s.store.Instance.ListGeneralizedActive(olderThan, s.limit)
	if err != nil {
		return err
	}
	jobs := s.loadJobs(list) // 批量预加载,消除 scheduleRetry 的逐实例 Job.Get
	for i := range list {
		ins := list[i]
		job := jobs[ins.JobID]
		rows, err := s.store.Instance.UpdateResultWithCallback(ins.ID, domain.StatusFailed, "服务重启前未完成", s.cbBuild(job, domain.StatusFailed))
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
		s.log.Info("已清理重启前超 grace 的未终结实例(近期实例交 reaper 判定;有余力者由 RetryPump 重派)", "count", n, "grace", grace)
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

// heldSlot 实例在飞时绑定的槽信息:jobID 用于按 job 释放并发槽(s.slots);appID+addr 用于
// 按 worker 释放服务端在飞计数(reg.ReleaseInflight,负载感知选址)。addr 在 bindHeld 时留空
// (此时 worker 尚未选定),派发 Send 成功后由 bindWorkerAddr 回填。
type heldSlot struct {
	appID int64
	jobID int64
	addr  string
}

// bindHeld 将"已 tryAcquire 的槽"绑定到实例(不重复 +1,仅记映射),供终态按实例释放。
// worker 此时未选定(PickWorker 在 dispatchToWorker 内),addr 留空;Send 成功后 bindWorkerAddr 回填。
func (s *Scheduler) bindHeld(insID, appID, jobID int64) {
	s.slotMu.Lock()
	s.held[insID] = heldSlot{appID: appID, jobID: jobID}
	s.slotMu.Unlock()
}

// bindWorkerAddr 派发 Send 成功后补充记录实例绑定的 worker 地址 + Acquire 该 worker 在飞计数。
// addr 为 PickFull 选定、MarkDispatched/Send 使用的原值(与 reg inflight key 一致);appID 从 held 取
// (bindHeld 时已写入 job.AppID),无需调用方传——避免传错 + 减参数。
//
// Acquire 在 slotMu 锁内与 addr 写回原子完成(仅 held 命中时):彻底消除 TOCTOU——若 Acquire 放锁外,
// Unlock 与 Acquire 之间并发 releaseByInstance 会读到已回填的 addr、先 Release(计数 0 no-op)并删 held,
// 随后本 Acquire 落下成无配对 Release 的 +1 泄漏(实例已终态无人再 Release)。held 未命中(已被并发释放)
// 则跳过:releaseByInstance 已按"未 Acquire"自洽(此时 addr 仍空未 Release)。锁序 slotMu→reg.mu 单向
// (reg 不回调 scheduler;releaseByInstance 同序且其 Release 在释 slotMu 后),无死锁。
func (s *Scheduler) bindWorkerAddr(insID int64, addr string) {
	s.slotMu.Lock()
	h, ok := s.held[insID]
	if ok {
		h.addr = addr
		s.held[insID] = h
		if s.reg != nil && addr != "" {
			s.reg.AcquireInflight(h.appID, addr)
		}
	}
	s.slotMu.Unlock()
}

// releaseByInstance 按实例释放其绑定的槽(幂等:未绑定则 no-op)。
func (s *Scheduler) releaseByInstance(insID int64) {
	s.slotMu.Lock()
	h, ok := s.held[insID]
	if ok {
		delete(s.held, insID)
		if s.slots[h.jobID] <= 1 {
			delete(s.slots, h.jobID)
		} else {
			s.slots[h.jobID]--
		}
	}
	s.slotMu.Unlock()
	// worker 在飞计数 -1:仅当派发时 Acquire 过(addr 非空=Send 成功绑过 worker)才 Release。
	// reg nil(测试/未 Start)或 addr 空(派发前失败,未绑 worker)跳过,ReleaseInflight 自身亦幂等。
	if ok && s.reg != nil && h.addr != "" {
		s.reg.ReleaseInflight(h.appID, h.addr)
	}
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
// waiting_receive/running,形成"任务吊死"。reaper 周期扫描,对"绑定 worker 已失联"的实例标记
// failed、对"执行超过 job.TimeoutSec"的实例标记 timeout,并触发服务端重试。
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
	list, err := s.store.Instance.ListGeneralizedActive(time.Time{}, s.limit)
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
				s.finalizeReaped(&ins, domain.StatusFailed, "job 不存在,失败转移", nil)
				continue
			}
			if reason, status := s.stallReason(&ins, job, reg, now); reason != "" {
				s.finalizeReaped(&ins, status, reason, job)
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
				s.finalizeReaped(&ins, domain.StatusFailed, "job 不存在,失败转移", nil)
				continue
			}
			s.finalizeReaped(&ins, domain.StatusFailed, "worker 无法处理已解绑", job)
		}
	}
}

// finalizeReaped 标记实例终态(failed: worker 失联/未绑定/重启清理;timeout: 执行超 TimeoutSec)
// + 释放槽 + 调度重试(设 next_retry_time)。job 透传给 scheduleRetry(reaper 路径已 loadJobs,避免
// 重复 Job.Get);nil 时 scheduleRetry 自查。
func (s *Scheduler) finalizeReaped(ins *domain.Instance, status, reason string, job *domain.Job) {
	// payload 用 tx 内 latest 行(含本次写入的 status/result),无需预先内存赋值;job 可能为 nil,
	// cbBuild→Build 对 nil job 返回 nil(不插回调)。
	rows, err := s.store.Instance.UpdateResultWithCallback(ins.ID, status, reason, s.cbBuild(job, status))
	if err != nil {
		s.log.Error("reaper 标记失败", "instance_id", ins.ID, "err", err)
		return
	}
	if rows == 0 {
		return // 已被并发终结(worker 迟到回报 / RecoverStaleActive),不重复 Release/scheduleRetry
	}
	s.appendLogRaw(ins, "REAP", "error", "失败转移("+status+"): "+reason)
	s.ReleaseInFlight(ins.ID)
	s.scheduleRetry(ins, job)
}

// stallReason 判定实例是否卡死。返回 (卡死原因, 应置终态);reason 空串=未卡死(status 亦为空)。
// 终态取值:worker 未绑定/失联 → failed;执行超时 → timeout。
//
// 兜底优先级:worker 未绑定 → worker 失联 → 执行超时。执行超时仅在 job.TimeoutSec>0 时生效:
// TimeoutSec=0 表示"不限执行时长"(长任务语义),此时若 worker 持续心跳(在线)却永不推进,
// 实例会停在 waiting_receive/running 不被回收——故生产强烈建议为 job 配置合理的 TimeoutSec,
// 否则唯一能兜底的只有 worker 心跳真的停掉(失联判定)或服务重启(start_time 超 grace 后
// RecoverStaleActive 清理)。grace 内的这类卡死实例会滞留到 start_time 超 grace 的下次重启。
func (s *Scheduler) stallReason(ins *domain.Instance, job *domain.Job, reg *workerreg.Registry, now time.Time) (reason, status string) {
	if ins.WorkerAddress == "" {
		// 选后即绑后,正常派发不会出现「已派发态却无 worker 绑定」:MarkDispatched 先写 worker_address
		// 再置 waiting_receive。命中此分支必为异常(worker 对未绑定实例乱回报 / SetStatus / 迁移脏数据),
		// 应立即回收(→ reaper failed → scheduleRetry)。不再给 30s 宽限:它既兜不住崩溃恢复
		// (RecoverStaleActive 在重启时接管,此时 TriggerTime 已远超窗口),反而延迟真正卡死实例的检测。
		return "实例缺少 worker 绑定", domain.StatusFailed
	}
	// warmup 守卫:服务重启后 workerreg 是空的,worker 需一个心跳周期才重新注册——此期间 IsOnline 必为
	// false,若直接判失联会误杀所有"重启前在飞"的正常实例(worker 迟到 success 被终态守护拒绝 → 重复执行)。
	// warmup 内跳过"worker 失联"判定,仅保留下面的执行超时(TimeoutSec)判定。warmup 由配置
	// worker.warmup_seconds 控制(默认 30s ≥ 典型 10s 心跳 + 余量);零值=未启用(测试/旧路径)。
	if !s.inWarmup(now) && reg != nil && !reg.IsOnline(ins.AppID, ins.WorkerAddress) {
		return "worker 失联(心跳超时)", domain.StatusFailed
	}
	// 接收超时:实例停在 waiting_receive(已派发但 worker 从未回报 running/终态)超过 receiveTimeout——
	// worker 收到 /run 后迟迟未拉起执行(繁忙队列堆积/卡住/掉单/上报丢失)。仅 waiting_receive 生效:
	// running 已确认 worker 在执行,走下面的执行超时(TimeoutSec)。warmup 内跳过(同失联判定,避免重启
	// 误杀:重启后 worker 可能正补报重启前 pending 的 running)。receiveTimeout<=0 关闭(兼容不报 running 的旧 worker)。
	// 这是"worker 繁忙但心跳正常"卡死场景的核心兜底:不再干等满整个 TimeoutSec,超 receiveTimeout 即 failed 重派。
	if !s.inWarmup(now) && ins.Status == domain.StatusWaitingReceive &&
		s.receiveTimeout > 0 && ins.StartTime != nil &&
		now.Sub(*ins.StartTime) > s.receiveTimeout {
		return "worker 接收超时(>" + strconv.Itoa(int(s.receiveTimeout.Seconds())) + "s 未进入 running)", domain.StatusFailed
	}
	if job.TimeoutSec > 0 && ins.StartTime != nil {
		if now.Sub(*ins.StartTime) > time.Duration(job.TimeoutSec)*time.Second {
			return "实例执行超时(>" + strconv.Itoa(job.TimeoutSec) + "s)", domain.StatusTimeout
		}
	}
	return "", ""
}

// ===== DB 驱动重试 RetryPump =====

// RunRetryPump 周期扫描 failed/timeout 且 next_retry_time 到期的实例,按 retryIndex+1 重派。直到 ctx 取消。
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
	// 去重:已存在同 root 且 RetryIndex+1 的重试实例时跳过——OpenAPI Retry(设 next_retry_time)与
	// RetryPump(创建实例)并发触发同一 orig 会产生两个相同 RetryIndex 的重试实例,破坏重试链语义。
	// 查询失败按"不存在"继续(最坏重复创建,at-least-once 允许)。
	if exists, err := s.store.Instance.ExistsRetryChild(domain.RootOf(orig), int64(orig.RetryIndex+1)); err != nil {
		s.log.Error("检查重试实例存在性失败", "orig", orig.ID, "err", err)
	} else if exists {
		s.log.Info("已存在同 RetryIndex 重试实例,跳过重复创建", "orig", orig.ID, "retry_index", orig.RetryIndex+1)
		return
	}
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
	if err := s.store.Instance.CreateWithCallback(retryIns, s.cbBuild(job, domain.StatusQueued)); err != nil {
		s.log.Error("创建重试实例失败", "orig", orig.ID, "err", err)
		s.releaseByJob(job.ID)
		return
	}
	s.appendLogRaw(retryIns, "RETRY", "info",
		"重试派发 retry_index="+strconv.Itoa(retryIns.RetryIndex)+" (from "+strconv.FormatInt(orig.ID, 10)+")")
	s.bindHeld(retryIns.ID, job.AppID, job.ID)
	// 异步派发(同 execute/manual):慢/挂起 worker 不阻塞 retryOnce 单轮;关闭期纳入 wg 跟踪。
	// panic 时释放该实例绑定的槽(同 execute 兜底)。
	s.goTrack(func() {
		defer func() {
			if r := recover(); r != nil {
				s.log.Error("重试派发 panic", "orig", orig.ID, "panic", r)
				s.releaseByInstance(retryIns.ID)
			}
		}()
		s.dispatchToWorker(ctx, job, retryIns, "重试派发")
	})
}

// scheduleRetry failed/timeout 实例若仍有重试余力,设 next_retry_time 由 RetryPump 重派。
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
	// 退避策略由 Options.RetryJitter 控制:空/"0"=固定重试(每次等 base,不指数退避);
	// 非空=指数退避(base×2^retryIndex,封顶 RetryMaxBackoffSec/默认 30min) + 抖动
	// ("1"=纯退避无抖动;"min:max"=退避值×random[min,max] 防惊群)。
	interval := computeRetryInterval(job.RetryIntervalSec, ins.RetryIndex, job.ParseOptions())
	if err := s.store.Instance.SetNextRetryTime(ins.ID, time.Now().Add(interval)); err != nil {
		s.log.Error("设定重试时间失败", "instance_id", ins.ID, "err", err)
	}
}

// retryBackoff 计算实例重试退避间隔:base * 2^retryIndex,clamp 到 [1s, maxBackoff]。
// base 取 retryIntervalSec(默认 0→1s);用循环翻倍而非位移,避免大 retryIndex 下 int64 溢出。
// retryIndex 为当前实例已重试次数(即将创建第 retryIndex+1 次重试)。
// maxBackoff<=0 时默认 30min;base 超过 maxBackoff 时以 base 为上限(不压缩用户意图)。
// 例:base=10s,max=30min → 第1次重试等 10s,第2次 20s,第3次 40s...
func retryBackoff(retryIntervalSec, retryIndex int, maxBackoff time.Duration) time.Duration {
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Minute
	}
	base := time.Duration(retryIntervalSec) * time.Second
	if base < time.Second {
		base = time.Second
	}
	if base > maxBackoff {
		maxBackoff = base
	}
	d := base
	for i := 0; i < retryIndex && d < maxBackoff; i++ {
		d *= 2
	}
	if d <= 0 || d > maxBackoff {
		d = maxBackoff
	}
	return d
}

// applyJitter 按 jitter 范围对 interval 加随机抖动,防大量任务同退避值同时重试(惊群)。
// jitter 格式 "min:max"(如 "0.5:1"),语义:最终间隔 = interval × random[min,max)。
// 空/非法格式不抖动(返回原 interval);抖动后不低于 1s。
func applyJitter(interval time.Duration, jitter string) time.Duration {
	minF, maxF, ok := parseJitterRange(jitter)
	if !ok {
		return interval
	}
	factor := minF + (maxF-minF)*rand.Float64() // [min, max)
	d := time.Duration(float64(interval) * factor)
	if d < time.Second {
		d = time.Second
	}
	return d
}

// parseJitterRange 解析抖动因子 → (min, max, true)。支持:
//   - 范围 "min:max"(如 "0.5:1"):min/max 倍率,校验 0<min<=max
//   - 单值(如 "1"):min=max=该值(等同 "1:1",无抖动)
// 空/非法返回 false。
func parseJitterRange(s string) (min, max float64, ok bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, 0, false
	}
	if !strings.Contains(s, ":") {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil || v <= 0 {
			return 0, 0, false
		}
		return v, v, true
	}
	parts := strings.SplitN(s, ":", 2)
	min, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	max, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err1 != nil || err2 != nil || min <= 0 || max < min {
		return 0, 0, false
	}
	return min, max, true
}

// computeRetryInterval 计算重试等待间隔(纯函数,便于测试):
//   - 抖动因子空/"0" → 固定重试:每次等 base(RetryIntervalSec,默认 1s),不随 retryIndex 增长
//   - 非空 → 指数退避(base×2^retryIndex,封顶 RetryMaxBackoffSec/默认 30min) + 抖动:
//     "1"=纯指数退避无抖动(1/2/4/8/16×base);"min:max"=退避值×random[min,max] 防惊群
func computeRetryInterval(retryIntervalSec, retryIndex int, opts domain.JobOptions) time.Duration {
	base := time.Duration(retryIntervalSec) * time.Second
	if base < time.Second {
		base = time.Second
	}
	jitter := strings.TrimSpace(opts.RetryJitter)
	if jitter == "" || jitter == "0" {
		return base
	}
	maxBackoff := time.Duration(opts.RetryMaxBackoffSec) * time.Second
	interval := retryBackoff(retryIntervalSec, retryIndex, maxBackoff)
	return applyJitter(interval, jitter)
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
	// payload 用 tx 内 latest 行(MarkDispatched 已写 worker_address),无需预先内存赋值。
	rows, err := s.store.Instance.MarkDispatchedWithCallback(ins.ID, addr, s.cbBuild(job, domain.StatusWaitingReceive))
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
	// 派发成功:回填 worker 地址 + Acquire 在飞计数(PickFull 据此负载感知选址,下次优先选空闲 worker,
	// 避免重试/重派反复打到同一繁忙 worker)。
	s.bindWorkerAddr(ins.ID, addr)
	return false
}

// failDispatch 派发失败统一善后:标记 failed(清 worker 绑定)+ 记 STATUS 日志 + 释放槽 + 衔接重试。
func (s *Scheduler) failDispatch(ins *domain.Instance, job *domain.Job, prefix, reason string) {
	// 选后即绑下 Send 失败时 worker_address 已先于 POST commit;派发失败该绑定无意义,
	// 用 FailDispatchWithCallback 在同事务清 worker_address/start_time + 置 failed,避免
	// "failed 实例仍指向某 worker"的展示残留(与 worker 回报 failed 保留 worker 供审计区分)。
	rows, err := s.store.Instance.FailDispatchWithCallback(ins.ID, reason, s.cbBuild(job, domain.StatusFailed))
	if err != nil {
		s.log.Error("标记派发失败失败", "instance_id", ins.ID, "err", err)
	}
	s.appendLogRaw(ins, "STATUS", "error", prefix+"失败→failed: "+reason)
	s.releaseByInstance(ins.ID)
	// 仅本次真把实例置 failed(rows>0)时才衔接重试。rows==0 说明守护生效——实例已被并发置终态
	// (如 worker 在 Send 失败前已回报 success),不应给一个已终态实例排重试(留脏 next_retry_time)。
	if rows > 0 {
		s.scheduleRetry(ins, job)
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
