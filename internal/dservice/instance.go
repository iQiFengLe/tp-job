package dservice

import (
	"errors"
	"fmt"

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
