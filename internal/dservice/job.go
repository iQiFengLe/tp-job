package dservice

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"gorm.io/gorm"

	"task-schedule/internal/dispatch"
	"task-schedule/internal/domain"
	"task-schedule/internal/repository"
	"task-schedule/internal/schedtime"
)

// JobService job 业务。
type JobService struct {
	st  *repository.Store
	sch *dispatch.Scheduler
}

func NewJobService(st *repository.Store, sch *dispatch.Scheduler) *JobService {
	return &JobService{st: st, sch: sch}
}

// computeNextRun 按 job 当前调度配置推算下次执行时间;disabled 或 api/run_at 返回 nil。
// Create 与 Update 共用,确保启用/改调度后 next_run 一致地由配置驱动。
func computeNextRun(job *domain.Job) (*time.Time, error) {
	if !job.Enabled {
		return nil, nil
	}
	return schedtime.NextByKind(job.ScheduleKind, job.ScheduleExpr, time.Now())
}

// Create 校验 + 推算 NextRunTime + 落库。
func (s *JobService) Create(job *domain.Job) error {
	if err := validateJob(job); err != nil {
		return fmt.Errorf("%w: %v", ErrJobValidate, err)
	}
	next, err := computeNextRun(job)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrJobValidate, err)
	}
	job.NextRunTime = next
	return s.st.Job.Create(job)
}

func validateJob(j *domain.Job) error {
	if j.Name == "" {
		return errors.New("name 不能为空")
	}
	if j.ExecuteType == "" {
		j.ExecuteType = "http"
	}
	if j.MaxConcurrency < 1 {
		j.MaxConcurrency = 1
	}
	if j.MaxWaitSeconds < 0 {
		j.MaxWaitSeconds = 0
	}
	switch j.ScheduleKind {
	case "cron", "fix_rate", "fix_delay", "delay", "run_at", "api":
	default:
		return fmt.Errorf("非法 schedule_kind: %s", j.ScheduleKind)
	}
	// 生效窗口:若都指定,起始不能晚于截止
	if j.StartTime != nil && j.EndTime != nil && j.StartTime.After(*j.EndTime) {
		return errors.New("start_time 不能晚于 end_time")
	}
	// 回调 URL(可选):必须是合法 http(s) URL
	if j.CallbackURL != "" {
		u, err := url.Parse(j.CallbackURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return errors.New("callback_url 必须是合法 http(s) URL")
		}
	}
	// 启用 + 自动调度类型必须有合法表达式
	if j.Enabled && (j.ScheduleKind == "cron" || j.ScheduleKind == "fix_rate" ||
		j.ScheduleKind == "fix_delay" || j.ScheduleKind == "delay") {
		if _, err := schedtime.NextByKind(j.ScheduleKind, j.ScheduleExpr, time.Now()); err != nil {
			return err
		}
	}
	return nil
}

// Update 部分更新(指针字段)。调度相关字段(schedule_kind/schedule_expr/enabled)变化时
// 重算 next_run_time——否则把 api 改 cron、启用 disabled job 等会因 next_run 未刷新而永不触发。
func (s *JobService) Update(appID, id int64, fields map[string]any) error {
	if len(fields) == 0 {
		return nil
	}
	if hasScheduleChange(fields) {
		cur, err := s.Get(appID, id)
		if err != nil {
			return err
		}
		applyJobFields(cur, fields) // 合并待更新字段到 job 副本,据此重算与校验
		if err := validateJob(cur); err != nil {
			return fmt.Errorf("%w: %v", ErrJobValidate, err)
		}
		next, err := computeNextRun(cur)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrJobValidate, err)
		}
		if next != nil {
			fields["next_run_time"] = *next
		} else {
			fields["next_run_time"] = nil
		}
	}
	return s.st.Job.Update(id, fields)
}

// hasScheduleChange 判断更新是否影响调度(需重算 next_run)。
func hasScheduleChange(fields map[string]any) bool {
	for _, k := range []string{"schedule_kind", "schedule_expr", "enabled"} {
		if _, ok := fields[k]; ok {
			return true
		}
	}
	return false
}

// applyJobFields 把 fields(db 列名→值)覆盖到 job 副本,供 Update 重算/校验读取合并后的状态。
func applyJobFields(j *domain.Job, fields map[string]any) {
	for k, v := range fields {
		switch k {
		case "name":
			if s, ok := v.(string); ok {
				j.Name = s
			}
		case "execute_type":
			if s, ok := v.(string); ok {
				j.ExecuteType = s
			}
		case "job_params":
			if s, ok := v.(string); ok {
				j.JobParams = s
			}
		case "tag":
			if s, ok := v.(string); ok {
				j.Tag = s
			}
		case "timeout_sec":
			j.TimeoutSec = toInt(v)
		case "schedule_kind":
			if s, ok := v.(string); ok {
				j.ScheduleKind = s
			}
		case "schedule_expr":
			if s, ok := v.(string); ok {
				j.ScheduleExpr = s
			}
		case "start_time":
			if t, ok := v.(*time.Time); ok {
				j.StartTime = t
			}
		case "end_time":
			if t, ok := v.(*time.Time); ok {
				j.EndTime = t
			}
		case "max_concurrency":
			j.MaxConcurrency = toInt(v)
		case "max_wait_seconds":
			j.MaxWaitSeconds = toInt(v)
		case "retry_count":
			j.RetryCount = toInt(v)
		case "retry_interval_sec":
			j.RetryIntervalSec = toInt(v)
		case "default_priority":
			j.DefaultPriority = toInt(v)
		case "enabled":
			if b, ok := v.(bool); ok {
				j.Enabled = b
			}
		}
	}
}

func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case int32:
		return int(n)
	}
	return 0
}

func (s *JobService) Delete(appID, id int64) error {
	return s.st.Job.Delete(appID, id)
}

func (s *JobService) Get(appID, id int64) (*domain.Job, error) {
	j, err := s.st.Job.Get(appID, id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrJobNotFound
		}
		return nil, err
	}
	return j, nil
}

func (s *JobService) List(appID int64, page, size int) ([]domain.Job, int64, error) {
	return s.st.Job.List(appID, page, size)
}

// Trigger 手动触发:经 SubmitManual 入优先队列,受 MaxConcurrency 限制。
// 落库失败时透传 SubmitManual 的 error,供调用方据实响应,而非空报 triggered。
func (s *JobService) Trigger(appID, id int64, priority int, instanceParams string) error {
	job, err := s.Get(appID, id)
	if err != nil {
		return err
	}
	return s.sch.SubmitManual(job, priority, instanceParams)
}

// TriggerReturnInstance 同 Trigger,但返回创建的实例 ID 并支持 delayMS 延迟(对齐 PowerJob OpenAPI
// runJob):外部业务客户端需立即拿到 instanceId 追踪执行;delayMS>0 时立即返回 ID、延迟到点派发。
func (s *JobService) TriggerReturnInstance(appID, id int64, priority int, instanceParams string, delayMS int64) (int64, error) {
	job, err := s.Get(appID, id)
	if err != nil {
		return 0, err
	}
	return s.sch.SubmitManualDelayed(job, priority, instanceParams, time.Duration(delayMS)*time.Millisecond)
}
