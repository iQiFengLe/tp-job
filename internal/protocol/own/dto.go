// Package own 是自有管理端 REST 协议(/api/*)的 DTO 与翻译层。
//
// handler 收 HTTP → translator 把 dto 翻成 domain → 调 dservice → 结果经 translator 翻回 dto。
// DTO 是对外契约,与 domain 解耦:domain 字段调整不直接破坏 API。
package own

import (
	"errors"
	"time"

	"task-schedule/internal/domain"
	"task-schedule/internal/workerreg"
)

// ===== App =====

type CreateAppReq struct {
	AppName  string `json:"app_name" binding:"required"`
	Password string `json:"password" binding:"required"`
	Status   int8   `json:"status"`
}

type UpdateAppReq struct {
	AppName  *string `json:"app_name"`
	Password *string `json:"password"`
	Status   *int8   `json:"status"`
}

type AppView struct {
	ID        int64     `json:"id"`
	AppName   string    `json:"app_name"`
	Status    int8      `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ===== Job =====

type CreateJobReq struct {
	Name             string `json:"name" binding:"required"`
	ExecuteType      string `json:"execute_type"`           // 留空默认 http
	JobParams        string `json:"job_params"`
	Tag              string `json:"tag"`
	TimeoutSec       int    `json:"timeout_sec"`
	ScheduleKind     string `json:"schedule_kind" binding:"required"` // cron | fix_rate | fix_delay | delay | run_at | api
	ScheduleExpr     string `json:"schedule_expr"`
	StartTime        *int64 `json:"start_time,omitempty"` // 毫秒戳;不传/<=0=无界
	EndTime          *int64 `json:"end_time,omitempty"`
	MaxConcurrency   int    `json:"max_concurrency"`
	MaxWaitSeconds   int    `json:"max_wait_seconds"`
	RetryCount       int    `json:"retry_count"`
	RetryIntervalSec int    `json:"retry_interval_sec"`
	DefaultPriority  int    `json:"default_priority"`
	CallbackURL      string `json:"callback_url,omitempty"`
	Enabled          *bool  `json:"enabled"`
}

type UpdateJobReq struct {
	Name             *string `json:"name"`
	ExecuteType      *string `json:"execute_type"`
	JobParams        *string `json:"job_params"`
	Tag              *string `json:"tag"`
	TimeoutSec       *int    `json:"timeout_sec"`
	ScheduleKind     *string `json:"schedule_kind"`
	ScheduleExpr     *string `json:"schedule_expr"`
	StartTime        *int64 `json:"start_time"` // 毫秒戳;nil=不改,<=0=清空,>0=设值
	EndTime          *int64 `json:"end_time"`
	MaxConcurrency   *int    `json:"max_concurrency"`
	MaxWaitSeconds   *int    `json:"max_wait_seconds"`
	RetryCount       *int    `json:"retry_count"`
	RetryIntervalSec *int    `json:"retry_interval_sec"`
	DefaultPriority  *int    `json:"default_priority"`
	CallbackURL      *string `json:"callback_url"`
	Enabled          *bool   `json:"enabled"`
}

type JobView struct {
	ID      int64  `json:"id"`
	AppID   int64  `json:"app_id"`
	Name    string `json:"name"`

	ExecuteType string `json:"execute_type"`
	JobParams   string `json:"job_params,omitempty"`
	Tag         string `json:"tag,omitempty"`
	TimeoutSec  int    `json:"timeout_sec,omitempty"`

	ScheduleKind string     `json:"schedule_kind,omitempty"`
	ScheduleExpr string     `json:"schedule_expr,omitempty"`
	NextRunTime  *time.Time `json:"next_run_time,omitempty"`
	StartTime    int64 `json:"start_time,omitempty"` // 毫秒戳;0=无界
	EndTime      int64 `json:"end_time,omitempty"`

	MaxConcurrency   int  `json:"max_concurrency,omitempty"`
	MaxWaitSeconds   int  `json:"max_wait_seconds,omitempty"`
	RetryCount       int  `json:"retry_count,omitempty"`
	RetryIntervalSec int  `json:"retry_interval_sec,omitempty"`
	DefaultPriority  int    `json:"default_priority,omitempty"`
	CallbackURL      string `json:"callback_url,omitempty"`
	Enabled          bool   `json:"enabled"`

	FromID   string `json:"from_id,omitempty"`   // 来源 ID(自建=uuid,PowerJob=原 jobID)
	FromType string `json:"from_type,omitempty"` // 来源类型(SELF / powerjob)

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ===== PowerJob 同步导入 =====

// ImportPowerJobReq 从外部 PowerJob server 拉取任务定义导入到当前 app。
type ImportPowerJobReq struct {
	ServerAddress string `json:"server_address" binding:"required"` // 如 http://host:7700
	AppName       string `json:"app_name" binding:"required"`       // PowerJob app 名
	Password      string `json:"password,omitempty"`                // 可选 app 密码(/appInfo/list 不可用时回退 assert 用)
	Token         string `json:"token,omitempty"`                   // 可选 POWERJOB-TOKEN(4.3.3+)
	DryRun        bool   `json:"dry_run"`                           // true=仅预览不落库
}

// ImportPowerJobItem 预览/结果明细。Error 非空表示该条转换/解析失败被跳过。
type ImportPowerJobItem struct {
	Name         string `json:"name"`
	ScheduleKind string `json:"schedule_kind"`
	ScheduleExpr string `json:"schedule_expr"`
	Enabled      bool   `json:"enabled"`
	Conflict     bool   `json:"conflict"` // true=当前 app 已有同源 job(将更新)
	Error        string `json:"error,omitempty"`
}

// ImportPowerJobResp 导入结果。dry_run 时 Imported/Updated 为"预览将…",Skipped 为解析失败数。
type ImportPowerJobResp struct {
	Fetched  int                  `json:"fetched"`  // PowerJob 返回的 job 总数
	Imported int                  `json:"imported"` // 新增数(dry_run=将新增)
	Updated  int                  `json:"updated"`  // 更新数(dry_run=将更新)
	Skipped  int                  `json:"skipped"`  // 跳过数(解析失败)
	Preview  []ImportPowerJobItem `json:"preview"`
}

// ===== Instance =====

type InstanceView struct {
	ID             int64      `json:"id"`
	JobID          int64      `json:"job_id"`
	AppID          int64      `json:"app_id"`
	Status         string     `json:"status"`
	TriggerType    string     `json:"trigger_type,omitempty"`
	ScheduleKind   string     `json:"schedule_kind,omitempty"` // 来自关联 job(实例本身无此字段),列表批量填
	Priority       int        `json:"priority,omitempty"`
	RetryIndex     int        `json:"retry_index,omitempty"`
	RootInstanceID int64      `json:"root_instance_id,omitempty"`
	Tag            string     `json:"tag,omitempty"`
	WorkerAddress  string     `json:"worker_address,omitempty"`
	JobInstanceParams string  `json:"job_instance_params,omitempty"`
	Result         string     `json:"result,omitempty"`
	TriggerTime    time.Time  `json:"trigger_time"`
	StartTime      *time.Time `json:"start_time,omitempty"`
	EndTime        *time.Time `json:"end_time,omitempty"`
	DurationMS     int64      `json:"duration_ms,omitempty"`
}

// ===== Worker(在线节点,读 workerreg 内存注册表)=====

// WorkerView 在线 worker 视图。workerreg 不入库,此为内存快照。
type WorkerView struct {
	WorkerAddress   string    `json:"worker_address"`
	Protocol        string    `json:"protocol"` // http | powerjob
	Tags            []string  `json:"tags,omitempty"`
	AcceptNotTagJob bool      `json:"accept_not_tag_job"`
	Score           int       `json:"score,omitempty"`         // 选址分(高=空闲)
	CpuLoad         float64   `json:"cpu_load,omitempty"`
	CpuProcessors   int       `json:"cpu_processors,omitempty"`
	LastHeartbeat   time.Time `json:"last_heartbeat"`
}

// WorkerToView 隐藏内部指针,平铺 metrics 关键字段。
func WorkerToView(w *workerreg.WorkerInfo) WorkerView {
	var tags []string
	if len(w.Tags) > 0 {
		tags = append(tags, w.Tags...)
	}
	return WorkerView{
		WorkerAddress:   w.WorkerAddress,
		Protocol:        w.Protocol,
		Tags:            tags,
		AcceptNotTagJob: w.AcceptNotTagJob,
		Score:           w.Metrics.Score,
		CpuLoad:         w.Metrics.CpuLoad,
		CpuProcessors:   w.Metrics.CpuProcessors,
		LastHeartbeat:   w.LastHeartbeat,
	}
}

// ===== 翻译 =====

// AppToView 隐藏 Password(永不外泄)。
func AppToView(a *domain.App) AppView {
	return AppView{ID: a.ID, AppName: a.AppName, Status: a.Status, CreatedAt: a.CreatedAt, UpdatedAt: a.UpdatedAt}
}

func JobToView(j *domain.Job) JobView {
	return JobView{
		ID: j.ID, AppID: j.AppID, Name: j.Name,
		ExecuteType: j.ExecuteType, JobParams: j.JobParams, Tag: j.Tag, TimeoutSec: j.TimeoutSec,
		ScheduleKind: j.ScheduleKind, ScheduleExpr: j.ScheduleExpr, NextRunTime: j.NextRunTime,
		StartTime: timeToMs(j.StartTime), EndTime: timeToMs(j.EndTime),
		MaxConcurrency: j.MaxConcurrency, MaxWaitSeconds: j.MaxWaitSeconds,
		RetryCount: j.RetryCount, RetryIntervalSec: j.RetryIntervalSec,
		DefaultPriority: j.DefaultPriority, CallbackURL: j.CallbackURL, Enabled: j.Enabled,
		FromID: j.FromID, FromType: j.FromType,
		CreatedAt: j.CreatedAt, UpdatedAt: j.UpdatedAt,
	}
}

func InstanceToView(ins *domain.Instance) InstanceView {
	return InstanceView{
		ID: ins.ID, JobID: ins.JobID, AppID: ins.AppID, Status: ins.Status,
		TriggerType: ins.TriggerType, Priority: ins.Priority, RetryIndex: ins.RetryIndex,
		RootInstanceID: ins.RootInstanceID, Tag: ins.Tag, WorkerAddress: ins.WorkerAddress,
		JobInstanceParams: ins.JobInstanceParams, Result: ins.Result,
		TriggerTime: ins.TriggerTime, StartTime: ins.StartTime, EndTime: ins.EndTime, DurationMS: ins.DurationMS,
	}
}

// CreateJobReqToJob 把创建请求翻成 domain.Job(尚未算 next_run_time,由 service 完成)。
func CreateJobReqToJob(appID int64, req CreateJobReq) (*domain.Job, error) {
	if req.Name == "" {
		return nil, errors.New("name 不能为空")
	}
	if req.ScheduleKind == "" {
		return nil, errors.New("schedule_kind 不能为空")
	}
	execType := req.ExecuteType
	if execType == "" {
		execType = "http"
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	maxConc := req.MaxConcurrency
	if maxConc < 1 {
		maxConc = 1
	}
	maxWait := req.MaxWaitSeconds
	if maxWait < 0 {
		maxWait = 0
	}
	return &domain.Job{
		AppID: appID, Name: req.Name,
		ExecuteType: execType, JobParams: req.JobParams, Tag: req.Tag, TimeoutSec: req.TimeoutSec,
		ScheduleKind: req.ScheduleKind, ScheduleExpr: req.ScheduleExpr,
		StartTime: msToTimePtr(req.StartTime), EndTime: msToTimePtr(req.EndTime),
		MaxConcurrency: maxConc, MaxWaitSeconds: maxWait,
		RetryCount: req.RetryCount, RetryIntervalSec: req.RetryIntervalSec,
		DefaultPriority: req.DefaultPriority, CallbackURL: req.CallbackURL, Enabled: enabled,
	}, nil
}

// UpdateJobReqToFields 把部分更新请求翻成 store 可直接用的 fields map。
// nil 字段不写入;调度相关字段变化由 service 决定是否重算 next_run(此处只管字段透传)。
func UpdateJobReqToFields(req UpdateJobReq) map[string]any {
	f := map[string]any{}
	if req.Name != nil {
		f["name"] = *req.Name
	}
	if req.ExecuteType != nil {
		f["execute_type"] = *req.ExecuteType
	}
	if req.JobParams != nil {
		f["job_params"] = *req.JobParams
	}
	if req.Tag != nil {
		f["tag"] = *req.Tag
	}
	if req.TimeoutSec != nil {
		f["timeout_sec"] = *req.TimeoutSec
	}
	if req.ScheduleKind != nil {
		f["schedule_kind"] = *req.ScheduleKind
	}
	if req.ScheduleExpr != nil {
		f["schedule_expr"] = *req.ScheduleExpr
	}
	if req.StartTime != nil {
		f["start_time"] = msToTimePtr(req.StartTime) // <=0 → nil(清空,gorm map 写 NULL);>0 → 设值
	}
	if req.EndTime != nil {
		f["end_time"] = msToTimePtr(req.EndTime)
	}
	if req.MaxConcurrency != nil {
		f["max_concurrency"] = *req.MaxConcurrency
	}
	if req.MaxWaitSeconds != nil {
		f["max_wait_seconds"] = *req.MaxWaitSeconds
	}
	if req.RetryCount != nil {
		f["retry_count"] = *req.RetryCount
	}
	if req.RetryIntervalSec != nil {
		f["retry_interval_sec"] = *req.RetryIntervalSec
	}
	if req.DefaultPriority != nil {
		f["default_priority"] = *req.DefaultPriority
	}
	if req.CallbackURL != nil {
		f["callback_url"] = *req.CallbackURL
	}
	if req.Enabled != nil {
		f["enabled"] = *req.Enabled
	}
	return f
}

// msToTimePtr 毫秒戳 → *time.Time;nil/非正 → nil(无界)。update fields 里 nil 由 gorm map 写 NULL(=清空)。
func msToTimePtr(ms *int64) *time.Time {
	if ms == nil || *ms <= 0 {
		return nil
	}
	t := time.UnixMilli(*ms)
	return &t
}

// timeToMs *time.Time → 毫秒戳;nil → 0(无界)。
func timeToMs(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.UnixMilli()
}
