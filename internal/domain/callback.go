package domain

import "time"

// Callback 实例状态变更回调记录。每次实例状态变化时,若 job 配了 callback_url,
// 由调度器/dservice 在写实例状态的同一事务内插入一条;CallbackPump(dispatch/callback.go)
// 周期扫描 pending 并 POST 到 URL,至少一次。
//
// 一个实例每次状态变化各产生一条记录(queued/waiting_receive/running/终态),故独立成表
// 而非复用 Instance 字段。Payload 在事件发生瞬间快照,避免 pump 期重查到已变化的状态。
type Callback struct {
	ID             int64  `gorm:"primaryKey;autoIncrement" json:"id"`
	InstanceID     int64  `gorm:"index:idx_cb_instance" json:"instance_id"`
	JobID          int64  `gorm:"index" json:"job_id"`
	AppID          int64  `gorm:"index" json:"app_id"`
	RootInstanceID int64  `json:"root_instance_id,omitempty"`                  // 链首,接收方按逻辑触发聚合
	TriggerType    string `gorm:"type:varchar(16)" json:"trigger_type,omitempty"` // auto/manual/retry 快照
	RetryIndex     int    `json:"retry_index,omitempty"`

	EventStatus string `gorm:"type:varchar(16)" json:"event_status"` // 触发本次回调的实例状态
	URL         string `gorm:"type:varchar(512)" json:"url"`         // callback_url 快照(事件发生时值)
	Payload     string `gorm:"type:text" json:"payload"`             // 预序列化 JSON,事件瞬间快照

	Attempt   int        `gorm:"default:0" json:"attempt"`                                              // 已尝试投递次数
	State     string     `gorm:"type:varchar(8);default:pending;index:idx_cb_due,priority:1;index:idx_cb_purge,priority:1" json:"state"` // pending|sent|dead
	NextRetryAt *time.Time `gorm:"index:idx_cb_due,priority:2" json:"next_retry_at,omitempty"`            // pump:state=pending 且 <=now
	LastError string     `gorm:"type:text" json:"last_error,omitempty"`

	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime;index:idx_cb_purge,priority:2" json:"updated_at"`
}

func (Callback) TableName() string { return "instance_callback" }

// Callback 投递状态。
const (
	CallbackPending = "pending" // 待投递
	CallbackSent    = "sent"    // 投递成功(2xx)
	CallbackDead    = "dead"    // 达 MaxAttempts 仍未成功,放弃
)
