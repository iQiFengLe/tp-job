package powerjob

import (
	"encoding/base64"
	"encoding/json"

	"task-schedule/internal/domain"
	"task-schedule/internal/wire"
)

// AskResponse Actor ask 的统一响应(对齐 PowerJob):data 为 base64(业务对象 JSON)。
type AskResponse struct {
	Success bool   `json:"success"`
	Data    string `json:"data,omitempty"`
	Message string `json:"message,omitempty"`
}

// AskSucceed 构造成功响应:data 为 obj 序列化后的 base64。
func AskSucceed(obj any) AskResponse {
	raw, err := json.Marshal(obj)
	if err != nil {
		return AskSucceedNil()
	}
	return AskResponse{Success: true, Data: base64.StdEncoding.EncodeToString(raw)}
}

// AskSucceedNil 成功响应,data = base64("null")。
func AskSucceedNil() AskResponse {
	return AskResponse{Success: true, Data: base64.StdEncoding.EncodeToString([]byte("null"))}
}

// AskFailed 构造失败响应。
func AskFailed(msg string) AskResponse {
	return AskResponse{Success: false, Message: msg}
}

// ResultDTO REST 接口(/assert /acquire)的统一响应。
type ResultDTO struct {
	Success bool   `json:"success"`
	Data    any    `json:"data,omitempty"`
	Message string `json:"message,omitempty"`
}

func ResultOK(data any) ResultDTO     { return ResultDTO{Success: true, Data: data} }
func ResultFail(msg string) ResultDTO { return ResultDTO{Success: false, Message: msg} }

// HeartbeatReq PowerJob worker 心跳(systemMetrics 字段名对齐 PowerJob)。
type HeartbeatReq struct {
	AppName         string               `json:"appName"`
	WorkerAddress   string               `json:"workerAddress"`
	SystemMetrics   domain.SystemMetrics `json:"systemMetrics"`
	Tag             string               `json:"tag"`  // PowerJob 单 tag(兼容)
	Tags            []string             `json:"tags"` // 通用 tags
	AcceptNotTagJob bool                 `json:"acceptNotTagJob"`
}

// ReportInstanceStatusReq worker 回报实例状态(官方数字码)。
// InstanceID/JobID 用 wire.FlexInt64:多语言 worker 上报的数值 ID 实测类型不一致
// (reportLog 发字符串、reportInstanceStatus 发数字),兼容两种写法,对齐 Jackson 宽松解析。
type ReportInstanceStatusReq struct {
	InstanceID     wire.FlexInt64 `json:"instanceId"`
	JobID          wire.FlexInt64 `json:"jobId"`
	InstanceStatus int            `json:"instanceStatus"`
	Result         string         `json:"result"`
}

// LogReportReq worker 批量上报日志。
type LogReportReq struct {
	InstanceLogContents []LogContent `json:"instanceLogContents"`
}

// LogContent 单条日志(PowerJob 原生 LogLevel int:1=DEBUG 2=INFO 3=WARN 4=ERROR)。
type LogContent struct {
	InstanceID wire.FlexInt64 `json:"instanceId"`
	LogLevel   int            `json:"logLevel"` // PowerJob WorkerLog 标准字段名(非 level)
	LogContent string         `json:"logContent"`
	LogTime    int64          `json:"logTime"`
}

// QueryClusterReq worker 查询 job 的执行集群(在线 worker 列表)。
type QueryClusterReq struct {
	AppID wire.FlexInt64 `json:"appId"`
	JobID wire.FlexInt64 `json:"jobId"`
}
