// Package powerjob 是 PowerJob Server 协议兼容层(/server/*)。
//
// 让标准 PowerJob Java worker(不改源码)接入:assert/acquire/workerHeartbeat/
// reportInstanceStatus/reportLog/queryJobCluster。wire 格式对齐 PowerJob(base64 AskResponse、
// 官方数字状态码),内部翻译为 domain。无鉴权(对齐 PowerJob Server,靠 /server/* 网络隔离)。
package powerjob

import "task-schedule/internal/domain"

// PowerJob 官方 InstanceStatus 数字码(1/2/3/4/5/9/10 与 tech.powerjob.common.enums.InstanceStatus 一致;
// 6 为本服务自定义扩展码 skipped,仅服务端写,worker 永不上报)。
const (
	WireWaitingDispatch      = 1
	WireWaitingWorkerReceive = 2
	WireRunning              = 3
	WireFailed               = 4
	WireSucceed              = 5
	WireSkipped              = 6
	WireCanceled             = 9
	WireStopped              = 10
)

// WireToDomain 把 PowerJob 数字状态码翻译成领域 string。非法返回 ("", false)。
func WireToDomain(s int) (string, bool) {
	switch s {
	case WireWaitingDispatch:
		return domain.StatusQueued, true
	case WireWaitingWorkerReceive:
		return domain.StatusWaitingReceive, true
	case WireRunning:
		return domain.StatusRunning, true
	case WireFailed:
		return domain.StatusFailed, true
	case WireSucceed:
		return domain.StatusSuccess, true
	case WireSkipped:
		return domain.StatusSkipped, true
	case WireCanceled:
		return domain.StatusCanceled, true
	case WireStopped:
		return domain.StatusStopped, true
	}
	return "", false
}

// DomainToWire 领域 string → PowerJob 数字码(派发/查询时用)。
func DomainToWire(s string) int {
	switch s {
	case domain.StatusQueued:
		return WireWaitingDispatch
	case domain.StatusWaitingReceive:
		return WireWaitingWorkerReceive
	case domain.StatusRunning:
		return WireRunning
	case domain.StatusFailed:
		return WireFailed
	case domain.StatusSuccess:
		return WireSucceed
	case domain.StatusSkipped:
		return WireSkipped
	case domain.StatusCanceled:
		return WireCanceled
	case domain.StatusStopped:
		return WireStopped
	}
	return WireFailed
}

// IsValidWireReport worker 上报的合法值:{1,2,3,4,5,9,10};6(skipped)是服务端自定义码,worker 永不上报。
func IsValidWireReport(s int) bool {
	switch s {
	case WireWaitingDispatch, WireWaitingWorkerReceive, WireRunning,
		WireFailed, WireSucceed, WireCanceled, WireStopped:
		return true
	}
	return false
}
