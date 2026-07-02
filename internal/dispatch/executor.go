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

// PowerJobRunReq 是 PowerJob 协议 runJob 请求体的最小形状。
// 阶段 3 的 protocol/powerjob 会提供完整 wire DTO + translator;此处只内联必要字段,
// 避免 dispatch 反向依赖尚未落地的协议包。字段对齐官方 ServerScheduleJobReq。
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

// Dispatch 实现 domain.Executor。
func (e *Executor) Dispatch(ctx context.Context, job *domain.Job, ins *domain.Instance) domain.DispatchResult {
	tag := ins.Tag
	if tag == "" {
		tag = job.Tag
	}
	w, ok := e.reg.PickFull(job.AppID, tag)
	if !ok {
		return domain.DispatchResult{Reason: "无可用 worker(tag 不匹配或全部离线)"}
	}
	if err := e.push(ctx, w, job, ins); err != nil {
		return domain.DispatchResult{Reason: err.Error()}
	}
	return domain.DispatchResult{Accepted: true, WorkerAddress: w.WorkerAddress}
}

// push 按 worker.protocol 构造请求体并 POST。
func (e *Executor) push(ctx context.Context, w workerreg.WorkerInfo, job *domain.Job, ins *domain.Instance) error {
	var (
		url  string
		body []byte
		err  error
	)
	addr := workerreg.NormalizeAddress(w.WorkerAddress)
	switch w.Protocol {
	case workerreg.ProtocolPowerJob:
		req := PowerJobRunReq{
			AllWorkerAddress: []string{addr},
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
		url = "http://" + addr + "/taskTracker/runJob"
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
		url = "http://" + addr + "/run"
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
