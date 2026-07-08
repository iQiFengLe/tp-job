package domain

import (
	"encoding/json"
	"strings"
	"time"
)

// Job 任务定义。当前执行模式唯一为 http 派发(ExecuteType=http):服务端选一个在线 worker,
// POST 固定 body(见 DispatchBody)交付执行。不再支持用户自定义 webhook URL/headers/body。
//
// 调度按 ScheduleKind(cron/fix_rate/fix_delay/delay/run_at/api)+ ScheduleExpr 推算 NextRunTime。
type Job struct {
	ID    int64  `gorm:"primaryKey;autoIncrement" json:"id"`
	AppID int64  `gorm:"index:idx_app_job;uniqueIndex:idx_job_from,priority:1;not null" json:"app_id"` // int 外键
	Name        string `gorm:"type:varchar(128);not null" json:"name"`
	Description string `gorm:"type:text" json:"description,omitempty"` // 任务描述(可选);PowerJob 同步自 jobDescription,自建可手填

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

	// —— 扩展选项(JSON,见 JobOptions)——
	Options string `gorm:"type:text" json:"options,omitempty"` // 可扩展配置(重试抖动/退避上限等),不单独建列

	// —— 并发 / 排队 / 重试 ——
	MaxConcurrency   int  `gorm:"default:1" json:"max_concurrency,omitempty"`
	MaxWaitSeconds   int  `gorm:"default:0" json:"max_wait_seconds,omitempty"` // 排队等待超时(秒;⚠ 当前未实现,预留——配置后无效果)
	RetryCount       int  `gorm:"default:0" json:"retry_count,omitempty"`
	RetryIntervalSec int  `gorm:"default:0" json:"retry_interval_sec,omitempty"`
	DefaultPriority  int  `gorm:"default:0" json:"default_priority,omitempty"`
	Enabled          bool `json:"enabled"` // 无 DB default:Create 由 Go 侧显式赋值,RETURNING 回写一致(避 gorm 零值覆盖)

	// 来源标识:复合唯一键 (app_id, from_id, from_type)——每 app 独立(同源 job 可分存多 app)。
	// 自建 job = uuid + "SELF";PowerJob 同步 = "pj:<server指纹>:<原jobID>" + "powerjob"(含来源 server 命名空间,
	// 跨 PowerJob server 同 ID job 不互相覆盖)。用于幂等同步 upsert 判重与来源展示。
	FromID   string `gorm:"type:varchar(64);not null;uniqueIndex:idx_job_from,priority:2" json:"from_id,omitempty"`
	FromType string `gorm:"type:varchar(32);not null;uniqueIndex:idx_job_from,priority:3;default:SELF" json:"from_type,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"` // 最后更新时间
}

func (Job) TableName() string { return "job" }

// JobOptions Job.Options 的 JSON 结构(可扩展,当前仅重试退避相关)。
type JobOptions struct {
	RetryJitter        string `json:"retry_jitter,omitempty"`         // 抖动范围 "min:max"(如 "0.5:1");空=不抖动。语义:最终间隔=退避值×random[min,max]
	RetryMaxBackoffSec int    `json:"retry_max_backoff_sec,omitempty"` // 退避上限(秒);0=默认 30min
}

// ParseOptions 解析 Options JSON;空/非法兜底零值(不阻断重试)。
func (j *Job) ParseOptions() JobOptions {
	if strings.TrimSpace(j.Options) == "" {
		return JobOptions{}
	}
	var o JobOptions
	if err := json.Unmarshal([]byte(j.Options), &o); err != nil {
		return JobOptions{}
	}
	return o
}

// JSON 序列化为 Options 列存储串;空选项返回 ""(避免存 "{}")。
func (o JobOptions) JSON() string {
	b, err := json.Marshal(o)
	if err != nil {
		return ""
	}
	if s := string(b); s != "{}" {
		return s
	}
	return ""
}
