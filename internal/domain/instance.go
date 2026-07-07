package domain

import "time"

// 实例状态机(8 态)。领域层只认这些 string;外部协议(PowerJob 数字码等)在协议适配层翻译。
//
// 流转:queued(排队) → waiting_receive(已派发等 worker 接收) → running(执行中) → success/failed;
// reaper 把卡在 waiting_receive/running 的实例标 failed 重派;终态不可回退。
const (
	StatusQueued         = "queued"          // 排队(并发超限)
	StatusWaitingReceive = "waiting_receive" // 已派发,等 worker 接收/拉起
	StatusRunning        = "running"         // 运行中(worker 上报)
	StatusSuccess        = "success"         // 成功
	StatusFailed         = "failed"          // 失败(含执行超时,result 注明)
	StatusSkipped        = "skipped"         // 跳过(排队等待超时;⚠ 当前未实现,预留——无代码路径产出此状态)
	StatusCanceled       = "canceled"        // 取消
	StatusStopped        = "stopped"         // 手动取消
)

// StatusTerminal 是否终态(写入后不可回退,防 worker 乱序/重复上报覆盖)。
func StatusTerminal(s string) bool {
	switch s {
	case StatusSuccess, StatusFailed, StatusSkipped, StatusCanceled, StatusStopped:
		return true
	}
	return false
}

// StatusValid 是否合法状态值(拒绝任意字符串脏数据)。
func StatusValid(s string) bool {
	switch s {
	case StatusQueued, StatusWaitingReceive, StatusRunning, StatusSuccess,
		StatusFailed, StatusSkipped, StatusCanceled, StatusStopped:
		return true
	}
	return false
}

// TerminalStatuses 返回终态切片,用于 SQL "终态不可回退" 守护的 NOT IN 列表。
func TerminalStatuses() []string {
	return []string{StatusSuccess, StatusFailed, StatusSkipped, StatusCanceled, StatusStopped}
}

// Instance 一次任务执行的实例。
//
// RootInstanceID 为"归属首个实例"的分组键:自身即首次时为 0;重试新开实例时沿用链首 id
// (RootOf 计算赋值)。同一逻辑触发的所有重试共享 root,按 ID 自增排序即时间序。
type Instance struct {
	ID             int64  `gorm:"primaryKey;autoIncrement" json:"id"`
	JobID          int64  `gorm:"index;not null" json:"job_id"`
	AppID          int64  `gorm:"index;not null" json:"app_id"`
	Status         string `gorm:"type:varchar(16);index;default:queued" json:"status"`
	TriggerType    string `gorm:"type:varchar(16)" json:"trigger_type,omitempty"` // auto | manual | retry
	Priority       int    `gorm:"default:0" json:"priority,omitempty"`
	RetryIndex     int    `gorm:"default:0" json:"retry_index,omitempty"`
	RootInstanceID int64  `gorm:"index;default:0" json:"root_instance_id,omitempty"` // 归属首个实例 id;0=自身即首次

	// —— 派发 / 参数 ——
	JobInstanceParams string `gorm:"type:text" json:"job_instance_params,omitempty"`  // 实例级参数(本次执行特有),随派发下发
	Tag               string `gorm:"type:varchar(128)" json:"tag,omitempty"`          // 实例标签;派发匹配 worker 用(空则回退 Job.Tag)
	WorkerAddress     string `gorm:"type:varchar(128)" json:"worker_address,omitempty"` // 承接该实例的 worker(派发时绑定)

	// —— 结果 ——
	HTTPStatus    int        `json:"http_status,omitempty"`
	ResponseBody  string     `gorm:"type:text" json:"response_body,omitempty"`
	Result        string     `gorm:"type:text" json:"result,omitempty"`
	NextRetryTime *time.Time `gorm:"index" json:"next_retry_time,omitempty"` // DB 驱动重试

	TriggerTime time.Time  `json:"trigger_time"`
	StartTime   *time.Time `json:"start_time,omitempty"`
	EndTime     *time.Time `json:"end_time,omitempty"`
	DurationMS  int64      `json:"duration_ms,omitempty"`
	CreatedAt   time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt   time.Time  `gorm:"autoUpdateTime" json:"updated_at"` // 最后更新时间
}

func (Instance) TableName() string { return "job_instance" }

// RootOf 计算"归属首个实例 id":实例自身为 root(RootInstanceID==0)时返回自己的 ID,否则沿用。
// 重试创建新实例时:NewInstance.RootInstanceID = RootOf(原实例)——永远指向链首,非上个。
func RootOf(ins *Instance) int64 {
	if ins == nil {
		return 0
	}
	if ins.RootInstanceID == 0 {
		return ins.ID
	}
	return ins.RootInstanceID
}
