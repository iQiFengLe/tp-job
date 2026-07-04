package domain

import "time"

// Job 任务定义。当前执行模式唯一为 http 派发(ExecuteType=http):服务端选一个在线 worker,
// POST 固定 body(见 DispatchBody)交付执行。不再支持用户自定义 webhook URL/headers/body。
//
// 调度按 ScheduleKind(cron/fix_rate/fix_delay/delay/run_at/api)+ ScheduleExpr 推算 NextRunTime。
type Job struct {
	ID    int64  `gorm:"primaryKey;autoIncrement" json:"id"`
	AppID int64  `gorm:"index:idx_app_job;not null" json:"app_id"` // int 外键
	Name  string `gorm:"type:varchar(128);not null" json:"name"`

	// —— 执行(当前唯一 http 派发;字段保留供未来扩展)——
	ExecuteType string `gorm:"type:varchar(16);not null;default:http" json:"execute_type"` // http
	JobParams   string `gorm:"type:text" json:"job_params,omitempty"`                      // 任务参数(字符串),随每次执行下发
	Tag         string `gorm:"type:varchar(128)" json:"tag,omitempty"`                     // 任务标签;worker 匹配用(Instance.Tag 可覆盖)
	TimeoutSec  int    `gorm:"default:0" json:"timeout_sec,omitempty"`                      // 实例执行超时(reaper 据此)

	// —— 调度 ——
	ScheduleKind string     `gorm:"type:varchar(16)" json:"schedule_kind,omitempty"`  // cron | fix_rate | fix_delay | delay | run_at | api
	ScheduleExpr string     `gorm:"type:varchar(128)" json:"schedule_expr,omitempty"` // cron 串 / 毫秒数 / run_at 时间
	NextRunTime  *time.Time `gorm:"index" json:"next_run_time,omitempty"`

	// —— 生效窗口(可选;nil=无界。仅约束自动调度,手动触发不受限)——
	StartTime *time.Time `gorm:"index" json:"start_time,omitempty"` // 生效起始;此前不自动调度(游标跳到 start_time)
	EndTime   *time.Time `gorm:"index" json:"end_time,omitempty"`   // 生效截止;此后 next_run 置空停摆(保持 enabled)

	// —— 回调(可选)——
	CallbackURL string `gorm:"type:varchar(512)" json:"callback_url,omitempty"` // 实例状态变化时 POST 通知此 URL,至少一次

	// —— 并发 / 排队 / 重试 ——
	MaxConcurrency   int  `gorm:"default:1" json:"max_concurrency,omitempty"`
	MaxWaitSeconds   int  `gorm:"default:0" json:"max_wait_seconds,omitempty"`
	RetryCount       int  `gorm:"default:0" json:"retry_count,omitempty"`
	RetryIntervalSec int  `gorm:"default:0" json:"retry_interval_sec,omitempty"`
	DefaultPriority  int  `gorm:"default:0" json:"default_priority,omitempty"`
	Enabled          bool `gorm:"default:true" json:"enabled"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"` // 最后更新时间
}

func (Job) TableName() string { return "job" }
