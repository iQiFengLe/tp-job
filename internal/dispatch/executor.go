// Package dispatch 实现 domain.Executor:WorkerDispatchExecutor。
//
// 派发流程:workerreg.PickFull 选 worker(tag 匹配 + score 择优)→ 按 worker.protocol 构造请求体
// (http 用 DispatchBody,powerjob 用官方 runJob)→ POST 到 worker.workerAddress → 2xx=已接收。
// executor 只管"派出去",不写实例状态——状态推进由 scheduler 负责(它更懂槽/排队/终态守护)。
package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"task-schedule/internal/domain"
	"task-schedule/internal/workerreg"
)

// server→worker 派发的 HTTP 路径(按 worker.protocol 选择);worker 端须注册同一路径,集中定义便于定位与演进。
const (
	pathHTTPRun        = "/run"
	pathPowerJobRunJob = "/worker/runJob"
)

// PowerJobRunReq 是 PowerJob 协议 runJob 请求体的最小形状(派发面向自研 http worker,仅必要字段)。
// 不含 processorInfo/processorType/timeParams:本服务不支持官方 Java processor(无 SDK),
// /server/* 仅供遵循 PowerJob 协议的自研 worker / 业务系统接入。字段对齐官方 ServerScheduleJobReq 子集。
type PowerJobRunReq struct {
	AllWorkerAddress []string `json:"allWorkerAddress"`
	JobID            int64    `json:"jobId"`
	InstanceId       int64    `json:"instanceId"`
	JobParams        string   `json:"jobParams"`
	InstanceParams   string   `json:"instanceParams"`
	ExecuteType      string   `json:"executeType"`
	TimeExpression   string   `json:"timeExpression"`
}

// Executor 派发实现。
type Executor struct {
	reg    *workerreg.Registry
	client *http.Client
}

// New 创建 Executor。dispatchTimeout 为单次 POST 的超时(应远小于实例执行超时)。
func New(reg *workerreg.Registry, dispatchTimeout time.Duration) *Executor {
	if dispatchTimeout <= 0 {
		dispatchTimeout = 10 * time.Second
	}
	return &Executor{
		reg: reg,
		client: &http.Client{
			Timeout: dispatchTimeout,
			// 禁止跟随重定向:worker 接收端不应 302,否则可被诱导 POST 到内网元数据/内部端点,
			// 绕过 worker.allowed_cidrs 的注册期校验(SSRF 纵深防御缺口)。返回原响应即可。
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// PickWorker 选 worker(不发请求)。tag 优先取实例,其次任务;按 score 择优。
// 返回 addr+protocol(不返回完整 WorkerInfo,避免 domain.Executor 接口反向依赖 workerreg)。
func (e *Executor) PickWorker(job *domain.Job, ins *domain.Instance) (addr, protocol string, ok bool) {
	tag := ins.Tag
	if tag == "" {
		tag = job.Tag
	}
	w, ok := e.reg.PickFull(job.AppID, tag)
	if !ok {
		return "", "", false
	}
	return w.WorkerAddress, w.Protocol, true
}

// Send 对已选定的 worker(addr+protocol)发 POST。失败返回 error,由调用方善后(UpdateResult failed)。
func (e *Executor) Send(ctx context.Context, addr, protocol string, job *domain.Job, ins *domain.Instance) error {
	return e.push(ctx, addr, protocol, job, ins)
}

// push 按 protocol 构造请求体并 POST 到 addr。
func (e *Executor) push(ctx context.Context, addr, protocol string, job *domain.Job, ins *domain.Instance) error {
	var (
		url  string
		body []byte
		err  error
	)
	// addr 归一化为 host:port 再拼 URL/塞 AllWorkerAddress(registry 以原值作 key,发请求用归一化值;
	// worker 正常上报 host:port 时二者一致)。用具名变量 host,避免覆盖入参语义造成可读性陷阱。
	host := workerreg.NormalizeAddress(addr)
	switch protocol {
	case workerreg.ProtocolPowerJob:
		req := PowerJobRunReq{
			AllWorkerAddress: []string{host},
			JobID:            job.ID,
			InstanceId:       ins.ID,
			JobParams:        job.JobParams,
			InstanceParams:   ins.JobInstanceParams,
			ExecuteType:      job.ExecuteType,
			TimeExpression:   job.ScheduleExpr,
		}
		body, err = json.Marshal(req)
		if err != nil {
			return fmt.Errorf("序列化 runJob 失败: %w", err)
		}
		// PowerJob 多语言 HTTP 协议:server→worker 派发 POST /worker/runJob,body=ServerScheduleJobReq
		// (见官方「多语言支持/HTTP」文档)。非 /taskTracker/runJob——那是 akka 时代的 actor 路径,
		// HTTP 规范已弃用;按官方规范实现的多语言 worker(.NET/Python 等)只认 /worker/runJob,派到旧路径会 404。
		url = "http://" + host + pathPowerJobRunJob
	default: // http
		body, err = json.Marshal(domain.DispatchBody{
			JobParams:         job.JobParams,
			JobInstanceParams: ins.JobInstanceParams,
			JobID:             job.ID,
			JobInstanceID:     ins.ID,
		})
		if err != nil {
			return fmt.Errorf("序列化 dispatch body 失败: %w", err)
		}
		url = "http://" + host + pathHTTPRun
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("构造请求失败: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("派发失败: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("worker 返回非 2xx: %d", resp.StatusCode)
	}
	return nil
}
