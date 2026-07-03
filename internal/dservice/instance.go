package dservice

import (
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"task-schedule/internal/dispatch"
	"task-schedule/internal/domain"
	"task-schedule/internal/instancelog"
	"task-schedule/internal/repository"
)

var ErrInstanceNotFound = errors.New("实例不存在")
var ErrInstanceValidate = errors.New("实例参数校验失败")

// InstanceService 实例业务:状态上报(终态守护 + 释放槽)、查询、日志读取。
type InstanceService struct {
	st  *repository.Store
	sch *dispatch.Scheduler
	il  *instancelog.Logger
}

func NewInstanceService(st *repository.Store, sch *dispatch.Scheduler, il *instancelog.Logger) *InstanceService {
	return &InstanceService{st: st, sch: sch, il: il}
}

// ReportStatus worker/管理端上报状态。
//   - 白名单校验(防脏数据)
//   - 终态守护(store 层):已终态不覆盖
//   - 进入终态时 ReleaseInFlight 释放该实例占用的任务级槽(幂等)
func (s *InstanceService) ReportStatus(id int64, status, result string) error {
	if !domain.StatusValid(status) {
		return fmt.Errorf("%w: 非法 status %q", ErrInstanceValidate, status)
	}
	if err := s.st.Instance.UpdateResult(id, status, result); err != nil {
		return err
	}
	if domain.StatusTerminal(status) {
		s.sch.ReleaseInFlight(id)
	}
	return nil
}

// SetStatus 管理员强制写入状态(纠错:可复活或改终态),不守护、不释放槽。
func (s *InstanceService) SetStatus(id int64, status, result string) error {
	if !domain.StatusValid(status) {
		return fmt.Errorf("%w: 非法 status %q", ErrInstanceValidate, status)
	}
	return s.st.Instance.SetStatus(id, status, result)
}

// Stop 标记实例 stopped 并释放其并发槽(供 OpenAPI stopInstance)。
//
// fire-and-forget 限制:task-schedule 无 worker 控制通道,此操作仅改服务端状态 + 腾出并发位,
// 不真正中断 worker 上已在跑的执行——worker 会继续跑完,其迟到回报被"终态守护"拒绝(实例保持 stopped),
// 但 ReportStatus 仍会再次 ReleaseInFlight(幂等)。故 Stop 一个在飞实例后,直到该 worker 执行结束前,
// 同 job 实际并发可能临时超过 MaxConcurrency(槽已腾、旧执行未止);业务需自行幂等。
// 之所以仍立即释放槽:若改为不释放,worker 崩溃不回报时该实例已 stopped(reaper 只扫活跃态不捞),
// 槽将永久泄漏、永久拉低 job 的 MaxConcurrency——临时超限(可自愈)远优于永久泄漏。
func (s *InstanceService) Stop(id int64) error {
	if err := s.st.Instance.SetStatus(id, domain.StatusStopped, "OpenAPI stop"); err != nil {
		return err
	}
	s.sch.ReleaseInFlight(id)
	return nil
}

// Cancel 标记实例 canceled 并释放其并发槽(供 OpenAPI cancelInstance)。语义/限制同 Stop,见其注释。
func (s *InstanceService) Cancel(id int64) error {
	if err := s.st.Instance.SetStatus(id, domain.StatusCanceled, "OpenAPI cancel"); err != nil {
		return err
	}
	s.sch.ReleaseInFlight(id)
	return nil
}

// Retry 立即重试一个 failed 实例:有重试余力则设 next_retry_time=now 交 RetryPump 重派
// (供 OpenAPI retryInstance)。非 failed 或无余力返回 error。
func (s *InstanceService) Retry(id int64) error {
	ins, err := s.Get(id)
	if err != nil {
		return err
	}
	if ins.Status != domain.StatusFailed {
		return fmt.Errorf("仅 failed 实例可重试,当前状态: %s", ins.Status)
	}
	job, err := s.st.Job.Get(ins.AppID, ins.JobID)
	if err != nil {
		return errors.New("job 不存在,无法重试")
	}
	if job.RetryCount <= 0 || ins.RetryIndex >= job.RetryCount {
		return fmt.Errorf("实例无重试余力(retry_index=%d, retry_count=%d)", ins.RetryIndex, job.RetryCount)
	}
	return s.st.Instance.SetNextRetryTime(id, time.Now())
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
	Group  bool // true:聚合同 root 全部(含重试);false:仅本实例
	Offset int
	Limit  int
}

// Logs 读实例日志文件。group=true 时按 instanceID 排序聚合同 root 全部。
func (s *InstanceService) Logs(id int64, q LogQuery) ([]string, int, error) {
	ins, err := s.Get(id)
	if err != nil {
		return nil, 0, err
	}
	rootID := domain.RootOf(ins)
	iq := instancelog.LogQuery{Offset: q.Offset, Limit: q.Limit}
	if q.Group {
		return s.il.ReadGroup(ins.AppID, rootID, iq)
	}
	return s.il.Read(ins.AppID, ins.ID, rootID, iq)
}

// LogsInApp 同 Logs,但先经 GetInApp 校验实例归属 app,防 app 角色越权读他人执行日志。
func (s *InstanceService) LogsInApp(appID, id int64, q LogQuery) ([]string, int, error) {
	ins, err := s.GetInApp(appID, id)
	if err != nil {
		return nil, 0, err
	}
	rootID := domain.RootOf(ins)
	iq := instancelog.LogQuery{Offset: q.Offset, Limit: q.Limit}
	if q.Group {
		return s.il.ReadGroup(ins.AppID, rootID, iq)
	}
	return s.il.Read(ins.AppID, ins.ID, rootID, iq)
}
