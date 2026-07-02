package domain

import "context"

// DispatchBody 简化 http 协议(http worker)的派发请求体:
// 服务端 POST 此结构到 worker.workerAddress。
type DispatchBody struct {
	JobParams         string `json:"jobParams"`
	JobInstanceParams string `json:"jobInstanceParams"`
	JobID             int64  `json:"jobId"`
	JobInstanceID     int64  `json:"jobInstanceId"`
}

// DispatchResult Executor.Dispatch 的返回。
type DispatchResult struct {
	Accepted      bool   // true=worker 已接收(实例应进入 waiting_receive);false=派发失败(实例应落 failed)
	WorkerAddress string // Accepted=true 时绑定的 worker 地址(scheduler 写入 instance.WorkerAddress)
	Reason        string // Accepted=false 时失败原因(记录到实例 result)
}

// Executor 抽象"一次实例的派发"。调度器对接口编程;
// 实现按 worker.protocol 构造请求体(http 用 DispatchBody,powerjob 用官方 runJob)。
//
// 阶段 0 仅为接口骨架,具体实现在阶段 2(WorkerDispatchExecutor)落地。
type Executor interface {
	Dispatch(ctx context.Context, job *Job, ins *Instance) DispatchResult
}
