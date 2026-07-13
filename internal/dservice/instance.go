package dservice

import (
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"tp-job/internal/dispatch"
	"tp-job/internal/domain"
	"tp-job/internal/instancelog"
	"tp-job/internal/repository"
)

var ErrInstanceNotFound = errors.New("实例不存在")
var ErrInstanceValidate = errors.New("实例参数校验失败")
// ErrInstanceNotRetryable 实例当前不可重试(状态非 failed/timeout,或无重试余力)。属业务校验,映射 HTTP 400。
var ErrInstanceNotRetryable = errors.New("实例当前不可重试")
// ErrInstanceNotQueued 实例当前非 queued 状态,无法调整优先级(push 架构下已派发则调整无意义)。属业务校验,映射 HTTP 400。
var ErrInstanceNotQueued = errors.New("实例当前非 queued 状态,无法调整优先级")

// InstanceService 实例业务:状态上报(终态守护 + 释放槽)、查询、日志读取。
type InstanceService struct {
	st        *repository.Store
	sch       *dispatch.Scheduler
	il        *instancelog.Logger
	cbBuilder dispatch.CallbackBuilder
}

func NewInstanceService(st *repository.Store, sch *dispatch.Scheduler, il *instancelog.Logger, cbBuilder dispatch.CallbackBuilder) *InstanceService {
	return &InstanceService{st: st, sch: sch, il: il, cbBuilder: cbBuilder}
}

// ReportStatus worker/管理端上报状态。
//   - 白名单校验(防脏数据)
//   - 终态守护(store 层):已终态不覆盖
//   - 进入终态时 ReleaseInFlight 释放该实例占用的任务级槽(幂等)
//   - 真状态变化(rows>0)时写 STATUS 事件到实例日志(状态变迁 old→new),补全单实例时间线
func (s *InstanceService) ReportStatus(id int64, status, result string) error {
	if !domain.StatusValid(status) {
		return fmt.Errorf("%w: 非法 status %q", ErrInstanceValidate, status)
	}
	// 入口 Get 一次:供 oldStatus(变迁日志)与 statusCallbackFrom 复用,避免 cbBuilder 启用时二次查询。
	// NotFound 静默——保持原"实例不存在即静默"语义(UpdateResultWithCallback 对不存在 id 本就 rows==0+nil err)。
	ins, err := s.st.Instance.Get(id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	oldStatus := ins.Status
	cb := s.statusCallbackFrom(ins, status)
	rows, err := s.st.Instance.UpdateResultWithCallback(id, status, result, cb)
	if err != nil {
		return err
	}
	if rows > 0 && oldStatus != status {
		// 终态守护 rows==0(迟到回报/终态重放)不写;oldStatus!=status 防同状态重复上报(postJSONRetry 重试
		// running 等)写"running→running"噪音——SQLite 同值 UPDATE 仍 RowsAffected=1(changes 计匹配行非变更行),
		// MySQL 默认计变更行返 0,加此条件使跨驱动一致且只在真变迁时记。
		level := "info"
		if status == domain.StatusFailed || status == domain.StatusTimeout {
			level = "warn"
		}
		s.logStatus(ins, level, fmt.Sprintf("状态变迁 %s→%s result=%s", oldStatus, status, truncate(result, 80)))
	}
	if domain.StatusTerminal(status) {
		s.sch.ReleaseInFlight(id)
	}
	return nil
}

// statusCallback 返回构造状态变更回调的闭包(供 *WithCallback tx 内用最新行构造 payload)。
// 回调未启用时返回 nil(仓储走无回调快捷路径)。启用时:先 Get ins 做终态短路(已终态则不构造——
// UpdateResultWithCallback 终态守护会让 rows==0,无需查 job+构造,高频心跳/终态重放场景省开销),
// 闭包内用 tx 给的 latest 行 + 预查 job 构造 payload(eventStatus 是本次值,其余字段取 latest
// 事件瞬间 DB 真实值,避免读快照 stale)。
func (s *InstanceService) statusCallback(id int64, status, result string) func(*domain.Instance) *domain.Callback {
	if s.cbBuilder == nil || !s.cbBuilder.Enabled() {
		return nil
	}
	ins, err := s.st.Instance.Get(id)
	if err != nil {
		return nil
	}
	// 终态短路:守护会让 rows==0,无需构造(高频心跳/终态重放场景省 Job.Get + Build)。
	if domain.StatusTerminal(ins.Status) {
		return nil
	}
	if ins.Status == status {
		return nil // 同状态短路:重复上报不重复通知(同 statusCallbackFrom,防 SQLite rows>0 误触)
	}
	// ⚠ job 必须在事务外预查:build 闭包在 *WithCallback 事务回调内被调用,若闭包内用 s.st.Job
	// (根 db)查 job 会从连接池拿到另一条连接——该查询不在本事务内,读不到事务未提交数据,破坏隔离
	// (MaxOpenConns=1 时代是直接死锁:唯一连接被事务占着自己等自己,曾导致 reportInstanceStatus
	// 一触发即整服务卡死、Ctrl+C 都关不掉;现多连接不再卡死,但读旧值更隐蔽)。instance.AppID/JobID
	// 创建后不可变,事务前查与事务内 latest 同值,无 TOCTOU。
	job, _ := s.st.Job.Get(ins.AppID, ins.JobID)
	return func(latest *domain.Instance) *domain.Callback {
		return s.cbBuilder.Build(latest, job, status)
	}
}

// statusCallbackFrom 复用入参 ins(ReportStatus 已 Get)构造状态变更回调,不再二次查询。
// 语义同 statusCallback;Stop/Cancel 仍用 statusCallback(id,...)(二者无入参 ins,改动面最小)。
func (s *InstanceService) statusCallbackFrom(ins *domain.Instance, status string) func(*domain.Instance) *domain.Callback {
	if s.cbBuilder == nil || !s.cbBuilder.Enabled() {
		return nil
	}
	if domain.StatusTerminal(ins.Status) {
		return nil // 终态短路:守护会让 rows==0,无需构造
	}
	if ins.Status == status {
		// 同状态短路:重复上报(postJSONRetry 重试 running 等)不重复通知。SQLite 同值 UPDATE 仍 RowsAffected=1
		// 会误触 callback 插入,此处按"非真变迁"静默,与 ReportStatus 日志 oldStatus!=status 判定一致。
		return nil
	}
	job, _ := s.st.Job.Get(ins.AppID, ins.JobID) // 事务外预查(同 statusCallback 死锁约束)
	return func(latest *domain.Instance) *domain.Callback {
		return s.cbBuilder.Build(latest, job, status)
	}
}

// SetStatus 管理员强制写入状态(纠错:可复活或改终态),不守护、不释放槽。
// 无条件写 STATUS 审计日志——这是唯一能逆状态机的入口(可把 success 改回 queued 复活),
// SetStatus 不返 rows、SetStatusWithCallback(nil) 也返 rows=0,无法按 rows 判断;"调过"即值得记。
func (s *InstanceService) SetStatus(id int64, status, result string) error {
	if !domain.StatusValid(status) {
		return fmt.Errorf("%w: 非法 status %q", ErrInstanceValidate, status)
	}
	if err := s.st.Instance.SetStatus(id, status, result); err != nil {
		return err
	}
	if ins, err := s.st.Instance.Get(id); err == nil {
		s.logStatus(ins, "warn", fmt.Sprintf("管理员强制置态→%s result=%s", status, truncate(result, 80)))
	}
	return nil
}

// Stop 标记实例 stopped 并释放其并发槽(供 OpenAPI stopInstance)。
//
// fire-and-forget 限制:tp-job 无 worker 控制通道,此操作仅改服务端状态 + 腾出并发位,
// 不真正中断 worker 上已在跑的执行——worker 会继续跑完,其迟到回报被"终态守护"拒绝(实例保持 stopped),
// 但 ReportStatus 仍会再次 ReleaseInFlight(幂等)。故 Stop 一个在飞实例后,直到该 worker 执行结束前,
// 同 job 实际并发可能临时超过 MaxConcurrency(槽已腾、旧执行未止);业务需自行幂等。
// 之所以仍立即释放槽:若改为不释放,worker 崩溃不回报时该实例已 stopped(reaper 只扫活跃态不捞),
// 槽将永久泄漏、永久拉低 job 的 MaxConcurrency——临时超限(可自愈)远优于永久泄漏。
func (s *InstanceService) Stop(id int64) error {
	cb := s.statusCallback(id, domain.StatusStopped, "OpenAPI stop")
	rows, err := s.st.Instance.SetStatusWithCallback(id, domain.StatusStopped, "OpenAPI stop", cb)
	if err != nil {
		return err
	}
	if rows > 0 {
		if ins, e := s.st.Instance.Get(id); e == nil {
			s.logStatus(ins, "warn", "管理员停止→stopped")
		}
	}
	s.sch.ReleaseInFlight(id)
	return nil
}

// Cancel 标记实例 canceled 并释放其并发槽(供 OpenAPI cancelInstance)。语义/限制同 Stop,见其注释。
func (s *InstanceService) Cancel(id int64) error {
	cb := s.statusCallback(id, domain.StatusCanceled, "OpenAPI cancel")
	rows, err := s.st.Instance.SetStatusWithCallback(id, domain.StatusCanceled, "OpenAPI cancel", cb)
	if err != nil {
		return err
	}
	if rows > 0 {
		if ins, e := s.st.Instance.Get(id); e == nil {
			s.logStatus(ins, "warn", "管理员取消→canceled")
		}
	}
	s.sch.ReleaseInFlight(id)
	return nil
}

// Retry 立即重试一个 failed/timeout 实例:有重试余力则设 next_retry_time=now 交 RetryPump 重派
// (供 OpenAPI retryInstance)。非可重试态(failed/timeout)或无余力返回 error。
func (s *InstanceService) Retry(id int64) error {
	ins, err := s.Get(id)
	if err != nil {
		return err
	}
	if !domain.StatusRetryable(ins.Status) {
		return fmt.Errorf("%w: 仅 failed/timeout 实例可重试,当前状态: %s", ErrInstanceNotRetryable, ins.Status)
	}
	job, err := s.st.Job.Get(ins.AppID, ins.JobID)
	if err != nil {
		return errors.New("job 不存在,无法重试")
	}
	if job.RetryCount <= 0 || ins.RetryIndex >= job.RetryCount {
		return fmt.Errorf("%w: 实例无重试余力(retry_index=%d, retry_count=%d)", ErrInstanceNotRetryable, ins.RetryIndex, job.RetryCount)
	}
	return s.st.Instance.SetNextRetryTime(id, time.Now())
}

// SetPriority 调整实例优先级(仅 queued 实例可调,供管理端 POST .../priority)。非 queued 返回
// ErrInstanceNotQueued(→400)。push 架构下优先级唯一作用域=派发顺序,实例一旦 waiting_receive/
// running 已 POST 出去,调整无意义故拒绝。
//
// 编排:DB 先(权威源,WHERE status=queued 守护;重启 RecoverQueued 据此重排),写成功(rows>0=确认仍
// queued)再同步内存堆即时重排。竞态:Get 校验后实例可能被并发 pop 派发——DB WHERE status=queued 二次
// 守护,已派发改态则 rows=0 no-op 且跳过内存同步;re-push 路径会从 DB 刷新 priority,故无 stale 残留。
// 详见 scheduler.RunManualDispatcher re-push 注释。
func (s *InstanceService) SetPriority(id int64, priority int) error {
	ins, err := s.Get(id)
	if err != nil {
		return err
	}
	if ins.Status != domain.StatusQueued {
		return fmt.Errorf("%w: 仅 queued 实例可调整优先级,当前状态: %s", ErrInstanceNotQueued, ins.Status)
	}
	rows, err := s.st.Instance.UpdatePriority(id, priority)
	if err != nil {
		return err
	}
	if rows > 0 { // DB 确认仍 queued 才同步内存;实例若刚被 pop 派发则 UpdateQueuedPriority 未命中 no-op
		s.sch.UpdateQueuedPriority(id, priority)
	}
	return nil
}

func (s *InstanceService) Get(id int64) (*domain.Instance, error) {
	ins, err := s.st.Instance.Get(id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrInstanceNotFound
		}
		return nil, err
	}
	return ins, nil
}

// GetInApp 取实例并校验归属 app:不匹配返回 ErrInstanceNotFound(防身份枚举,统一 404 而非 403)。
// 供受 AppScope 保护的 /api 路由补"实例归属"校验——AppScope 只验路径 :appId==会话 AppID,
// 不校验 :iid 是否真属该 app;此处补齐,避免 app 角色越权读取其他 app 的实例/日志。
func (s *InstanceService) GetInApp(appID, id int64) (*domain.Instance, error) {
	ins, err := s.Get(id)
	if err != nil {
		return nil, err
	}
	if ins.AppID != appID {
		return nil, ErrInstanceNotFound
	}
	return ins, nil
}

// LogQuery 日志读取参数。
type LogQuery struct {
	Offset int
	Limit  int
}

// Logs 读实例日志文件(按行,带 offset/limit 分页)。
//
// 文件名 {instanceID}_{rootInstanceID}.log 由 dispatch/worker 写入(首次实例 rootInstanceID=0),
// 读路径须与之对齐——传 ins.RootInstanceID,而非 domain.RootOf(后者是给 callback payload 算
// "逻辑链首"的,首次实例会回退到 ins.ID,落到不存在的 {N}_{N} 文件)。同链路串联只靠文件名格式
// (ssh/外部程序按名分析),程序内不提供聚合读取。
func (s *InstanceService) Logs(id int64, q LogQuery) ([]string, int, error) {
	ins, err := s.Get(id)
	if err != nil {
		return nil, 0, err
	}
	iq := instancelog.LogQuery{Offset: q.Offset, Limit: q.Limit}
	return s.il.Read(ins.AppID, ins.ID, ins.RootInstanceID, iq)
}

// LogsInApp 同 Logs,但先经 GetInApp 校验实例归属 app,防 app 角色越权读他人执行日志。
func (s *InstanceService) LogsInApp(appID, id int64, q LogQuery) ([]string, int, error) {
	ins, err := s.GetInApp(appID, id)
	if err != nil {
		return nil, 0, err
	}
	iq := instancelog.LogQuery{Offset: q.Offset, Limit: q.Limit}
	return s.il.Read(ins.AppID, ins.ID, ins.RootInstanceID, iq)
}

// logStatus 写一条 STATUS 事件到实例日志文件。rootID 取 ins.RootInstanceID(与
// dispatch.appendLog 写入侧对齐,见 Logs 注释);LogEntry 字段与 appendLogRaw 一致(显式 Time)。
func (s *InstanceService) logStatus(ins *domain.Instance, level, msg string) {
	if s.il == nil || ins == nil {
		return
	}
	s.il.Append(ins.AppID, ins.ID, ins.RootInstanceID, instancelog.LogEntry{
		Time: time.Now(), Kind: "STATUS", Level: level, Message: msg,
	})
}

// truncate 按 rune 截断到 maxRunes,超长追加省略号(按 rune 避免切断 UTF-8 多字节字符)。
// 用于把可能很长的 result 截到日志友好长度。
func truncate(s string, maxRunes int) string {
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return string(r[:maxRunes]) + "…"
}
