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

// Executor 抽象"一次实例的派发"。调度器对接口编程;
// 实现按 worker.protocol 构造请求体(http 用 DispatchBody,powerjob 用官方 runJob)。
//
// 阶段 0 仅为接口骨架,具体实现在阶段 2(WorkerDispatchExecutor)落地。
type Executor interface {
	// PickWorker 选 worker(不发请求)。返回 addr+protocol;ok=false=无可用 worker。
	PickWorker(job *Job, ins *Instance) (addr, protocol string, ok bool)
	// Send 对已选定(addr+protocol)的 worker 发 POST。失败返回 error,由调用方善后(UpdateResult failed)。
	Send(ctx context.Context, addr, protocol string, job *Job, ins *Instance) error
}
