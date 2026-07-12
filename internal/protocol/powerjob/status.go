// Package powerjob 是 PowerJob 协议兼容层(/server/* + /openApi/*)。
//
// 让遵循 PowerJob 协议的自研 http worker / 业务系统接入(心跳/状态/日志上报 + OpenAPI),
// wire 格式对齐 PowerJob(base64 AskResponse、官方数字状态码),内部翻译为 domain。
// 注意:不支持官方 Java processor(无 SDK);派发面向自研 http worker。无鉴权(对齐 PowerJob Server,
// 靠 /server/* /openApi/* 网络隔离)。
package powerjob

import "dida/internal/domain"

// PowerJob 官方 InstanceStatus 数字码(1/2/3/4/5/9/10 与 tech.powerjob.common.enums.InstanceStatus 一致;
// 6=skipped、7=timeout 为本服务自定义扩展码,仅服务端写,worker 永不上报)。
const (
	WireWaitingDispatch      = 1
	WireWaitingWorkerReceive = 2
	WireRunning              = 3
	WireFailed               = 4
	WireSucceed              = 5
	WireSkipped              = 6
	WireTimeout              = 7
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
	case WireTimeout:
		return domain.StatusTimeout, true
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
	case domain.StatusTimeout:
		return WireTimeout
	case domain.StatusCanceled:
		return WireCanceled
	case domain.StatusStopped:
		return WireStopped
	}
	// 未知态(DB 脏数据/未来新增状态)返回 failed(确定终态),让客户端 fail-fast 停止轮询+告警;
	// running 会让客户端认为活跃而死等(unknown 本不该发生,failed 更可观测)。
	return WireFailed
}

// IsValidWireReport worker 上报的合法值:{1,2,3,4,5,9,10};6(skipped)、7(timeout)是服务端自定义码,worker 永不上报。
func IsValidWireReport(s int) bool {
	switch s {
	case WireWaitingDispatch, WireWaitingWorkerReceive, WireRunning,
		WireFailed, WireSucceed, WireCanceled, WireStopped:
		return true
	}
	return false
}
