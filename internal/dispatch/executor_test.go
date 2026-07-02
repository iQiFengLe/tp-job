package dispatch

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"task-schedule/internal/domain"
	"task-schedule/internal/workerreg"
)

func TestDispatchHTTPAccepted(t *testing.T) {
	var got domain.DispatchBody
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		_ = json.Unmarshal(body, &got)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer srv.Close()

	reg := workerreg.New(time.Minute, nil)
	reg.Heartbeat(workerreg.WorkerInfo{
		AppID: 1, WorkerAddress: srv.Listener.Addr().String(),
		Metrics: domain.SystemMetrics{Score: 1}, Protocol: workerreg.ProtocolHTTP,
	})

	ex := New(reg, time.Second)
	job := &domain.Job{ID: 7, AppID: 1, JobParams: "p", ExecuteType: "http"}
	ins := &domain.Instance{ID: 90, JobID: 7, AppID: 1, JobInstanceParams: "ip"}

	res := ex.Dispatch(context.Background(), job, ins)
	if !res.Accepted {
		t.Fatalf("应被接收, reason=%s", res.Reason)
	}
	if res.WorkerAddress != srv.Listener.Addr().String() {
		t.Fatalf("应回传 worker 地址, got %s", res.WorkerAddress)
	}
	mu.Lock()
	defer mu.Unlock()
	if got.JobID != 7 || got.JobInstanceID != 90 || got.JobParams != "p" || got.JobInstanceParams != "ip" {
		t.Fatalf("worker 收到的 body 不符: %+v", got)
	}
}

func TestDispatchWorker2xxFail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	reg := workerreg.New(time.Minute, nil)
	reg.Heartbeat(workerreg.WorkerInfo{AppID: 1, WorkerAddress: srv.Listener.Addr().String(),
		Metrics: domain.SystemMetrics{Score: 1}, Protocol: workerreg.ProtocolHTTP})

	ex := New(reg, time.Second)
	res := ex.Dispatch(context.Background(), &domain.Job{AppID: 1}, &domain.Instance{AppID: 1})
	if res.Accepted {
		t.Fatal("500 应判失败")
	}
	if res.WorkerAddress != "" {
		t.Fatalf("失败不应回传地址, got %s", res.WorkerAddress)
	}
}

// 无匹配 worker(tag 不符):失败,不发起请求。
func TestDispatchNoWorker(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; w.WriteHeader(200) }))
	defer srv.Close()

	reg := workerreg.New(time.Minute, nil)
	reg.Heartbeat(workerreg.WorkerInfo{AppID: 1, WorkerAddress: srv.Listener.Addr().String(),
		Metrics: domain.SystemMetrics{Score: 1}, Tags: []string{"gpu"}, Protocol: workerreg.ProtocolHTTP})

	ex := New(reg, time.Second)
	// 任务 tag=cpu,worker 只接 gpu,且不 acceptNotTagJob → 不命中
	res := ex.Dispatch(context.Background(), &domain.Job{AppID: 1, Tag: "cpu"}, &domain.Instance{AppID: 1})
	if res.Accepted || hits != 0 {
		t.Fatalf("无匹配 worker 应不派发, Accepted=%v hits=%d", res.Accepted, hits)
	}
}

// Instance.Tag 覆盖 Job.Tag。
func TestDispatchInstanceTagOverridesJob(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()

	reg := workerreg.New(time.Minute, nil)
	reg.Heartbeat(workerreg.WorkerInfo{AppID: 1, WorkerAddress: srv.Listener.Addr().String(),
		Metrics: domain.SystemMetrics{Score: 1}, Tags: []string{"gpu"}, Protocol: workerreg.ProtocolHTTP})

	ex := New(reg, time.Second)
	// Job.Tag=cpu 但 Instance.Tag=gpu → 用 gpu 匹配,命中
	res := ex.Dispatch(context.Background(), &domain.Job{AppID: 1, Tag: "cpu"}, &domain.Instance{AppID: 1, Tag: "gpu"})
	if !res.Accepted {
		t.Fatalf("Instance.Tag=gpu 应覆盖 Job.Tag=cpu 并命中, reason=%s", res.Reason)
	}
}

// PowerJob 协议走 /taskTracker/runJob。
func TestDispatchPowerJobPath(t *testing.T) {
	var path string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		path = r.URL.Path
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer srv.Close()

	reg := workerreg.New(time.Minute, nil)
	reg.Heartbeat(workerreg.WorkerInfo{AppID: 1, WorkerAddress: srv.Listener.Addr().String(),
		Metrics: domain.SystemMetrics{Score: 1}, Protocol: workerreg.ProtocolPowerJob})

	ex := New(reg, time.Second)
	res := ex.Dispatch(context.Background(), &domain.Job{AppID: 1, ExecuteType: "http"}, &domain.Instance{AppID: 1})
	if !res.Accepted {
		t.Fatal("应被接收")
	}
	mu.Lock()
	defer mu.Unlock()
	if path != "/taskTracker/runJob" {
		t.Fatalf("PowerJob 协议应 POST /taskTracker/runJob, got %s", path)
	}
}
