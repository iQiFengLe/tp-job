// Package worker 是简化 worker 协议(/worker/*,无 token)的 DTO 与 handler。
//
// worker 启动后周期心跳上报(workerAddress + systemMetrics + tags),服务端据此选址派发;
// 执行后回调上报 status / logs。靠 appName 归属 + /worker/* 网络隔离保护(对齐 PowerJob /server/*)。
package worker

import (
	"tp-job/internal/domain"
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
// WorkerAddress 为上报者自报地址,须与实例绑定的 worker_address 一致(归属校验,防伪造 id 篡改他人实例)。
//
// 协议约定(影响服务端卡死回收判定):
//   - 收到 /run 开始执行时尽快回报 running——让服务端把实例从 waiting_receive 推进到 running,
//     标记"已接收并开始执行"。服务端据此区分"在执行"(running)与"卡死未接收"(持续 waiting_receive);
//     后者超 worker.receive_timeout_seconds 即判 failed 重派。从不报 running 的旧 worker 仍兼容
//     (配 receive_timeout_seconds=0 关闭接收超时),但实例全程停在 waiting_receive 直到终态,卡死时
//     只能等执行超时(TimeoutSec)——故新接入 worker 强烈建议及时报 running。
//   - running 与终态(success/failed)均应 at-least-once 重试上报:服务端终态守护(已终态不覆盖)+
//     running→running 幂等,重复上报无副作用。running 丢失会致服务端不知 worker 已在执行 → 接收超时
//     误杀重派 → 重复执行,故 running 与终态一样必须可靠送达。logs 为非关键日志,失败可丢。
type ReportStatusReq struct {
	WorkerAddress string `json:"workerAddress"`
	Status        string `json:"status"`
	Result        string `json:"result"`
}

// ReportLogReq worker 上报一条执行日志。
type ReportLogReq struct {
	Level   string `json:"level"`   // info/warn/error;空按 info
	Message string `json:"message"`
	Time    int64  `json:"time"`    // 毫秒时间戳;<=0 取服务端当前时间
}
