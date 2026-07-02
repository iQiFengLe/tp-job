// Package worker 是简化 worker 协议(/worker/*,无 token)的 DTO 与 handler。
//
// worker 启动后周期心跳上报(workerAddress + systemMetrics + tags),服务端据此选址派发;
// 执行后回调上报 status / logs。靠 appName 归属 + /worker/* 网络隔离保护(对齐 PowerJob /server/*)。
package worker

import (
	"task-schedule/internal/domain"
)

// HeartbeatReq 心跳上报。SystemMetrics 复用 domain 类型(纯数据,稳定可复用)。
type HeartbeatReq struct {
	AppName         string             `json:"appName"`
	WorkerAddress   string             `json:"workerAddress"`
	SystemMetrics   domain.SystemMetrics `json:"systemMetrics"`
	Tags            []string           `json:"tags"`
	AcceptNotTagJob bool               `json:"acceptNotTagJob"`
	Protocol        string             `json:"protocol"` // http | powerjob;留空按 http
}

// ReportStatusReq worker 回报实例状态(领域 string 状态码)。
type ReportStatusReq struct {
	Status string `json:"status"`
	Result string `json:"result"`
}

// ReportLogReq worker 上报一条执行日志。
type ReportLogReq struct {
	Level   string `json:"level"`   // info/warn/error;空按 info
	Message string `json:"message"`
	Time    int64  `json:"time"`    // 毫秒时间戳;<=0 取服务端当前时间
}
