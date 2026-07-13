// 示例 http worker:演示 /worker/* 简化协议的最小接入。
//
// 它做三件事:
//  1. 周期 POST {server}/worker/heartbeat 上报 appName + 本地地址 + systemMetrics + tags(注册自己)。
//  2. 本地起 HTTP 服务监听 POST /run:服务端派发任务时 POST {jobParams,jobInstanceParams,jobId,jobInstanceId} 到此。
//  3. 执行(此处仅回显参数)后回调 {server}/worker/instances/:iid/status 上报 success,并附一条日志。
//
// 用法:
//
//	# 先用管理台/登录创建一个 app(如 demo),再启动 worker 指向它:
//	go run ./examples/http-worker -server http://127.0.0.1:8080 -app demo -addr :9001 -tags gpu
//
// 然后在管理台为该 app 创建 api 任务并触发,即可看到实例 waiting_receive → running → success。
//
// ⚠ /worker/* 无鉴权(靠 appName + 网络隔离);本示例仅用于本地联调,勿直接暴露公网。
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"time"
)

func main() {
	server := flag.String("server", "http://127.0.0.1:8080", "调度服务地址")
	app := flag.String("app", "demo", "接入 appName(须已在管理台创建)")
	addr := flag.String("addr", ":9001", "worker 本地监听地址(服务端将 POST /run 到此)")
	advertise := flag.String("advertise", "", "服务端可达的本 worker 地址(host:port);空则取 127.0.0.1:<addr 端口>")
	tags := flag.String("tags", "", "worker 标签,逗号分隔(任务按 tag 匹配选址);如 gpu,highmem")
	heartbeat := flag.Duration("heartbeat", 10*time.Second, "心跳周期")
	flag.Parse()

	workerAddr := *advertise
	if workerAddr == "" {
		workerAddr = "127.0.0.1" + *addr // 本地联调:服务端与 worker 同机
	}

	// 1. 心跳上报(后台)
	go func() {
		ticker := time.NewTicker(*heartbeat)
		defer ticker.Stop()
		beat := func() { postHeartbeat(*server, *app, workerAddr, *tags) }
		beat() // 立即上报一次,缩短首次注册延迟
		for range ticker.C {
			beat()
		}
	}()

	// 2. 本地 HTTP 服务:接收 /run 派发
	mux := http.NewServeMux()
	mux.HandleFunc("/run", func(w http.ResponseWriter, r *http.Request) {
		runHandler(*server, workerAddr, w, r)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })

	log.Printf("worker 启动: app=%s addr=%s(advertise=%s) server=%s tags=%q", *app, *addr, workerAddr, *server, *tags)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

// runHandler 处理服务端派发:解析 body → 立即 ACK 2xx(服务端据此置 waiting_receive)→ 异步:
// 先回报 running(确认"已接收并开始执行",支撑服务端 waiting_receive 接收超时判定)→ 执行(回显)
// → 回报 success(指数退避重试,终态守护保证幂等)+ 日志。
//
// 立即 ACK 是关键:服务端 POST /run 仅交付任务(2xx=已接收),worker 异步执行后回调上报状态推进。
// running/success 均在 ACK 之后异步发出,不与"派发→waiting_receive"竞态(亦被终态守护兜住)。
func runHandler(server, workerAddr string, w http.ResponseWriter, r *http.Request) {
	var body struct {
		JobParams         string `json:"jobParams"`
		JobInstanceParams string `json:"jobInstanceParams"`
		JobID             int64  `json:"jobId"`
		JobInstanceID     int64  `json:"jobInstanceId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad body: "+err.Error(), http.StatusBadRequest)
		return
	}
	log.Printf("收到派发: jobId=%d instanceId=%d jobParams=%q instanceParams=%q",
		body.JobID, body.JobInstanceID, body.JobParams, body.JobInstanceParams)
	w.WriteHeader(http.StatusNoContent) // 立即 ACK:服务端置实例 waiting_receive

	// 异步执行 + 回报(不阻塞 ACK)
	go func() {
		// 先回报 running(at-least-once 重试):让服务端把实例从 waiting_receive 推进到 running,作为"已接收
		// 并开始执行"的信号——据此区分"在正常执行"(running)与"卡死未接收"(持续 waiting_receive,后者超
		// receive_timeout 即判 failed 重派)。running 必须可靠送达:丢失会致服务端不知 worker 已在执行 → 接收
		// 超时误杀重派 → 重复执行(与"卡死兜底"初衷相悖)。重复 running 上报幂等(终态守护 + running→running 无害)。
		postJSONRetry(server+"/worker/instances/"+fmt.Sprint(body.JobInstanceID)+"/status", map[string]any{
			"workerAddress": workerAddr,
			"status":        "running",
			"result":        "started",
		})

		time.Sleep(time.Second) // 模拟执行
		result := fmt.Sprintf("done: jobParams=%s instanceParams=%s", body.JobParams, body.JobInstanceParams)
		// 日志上报:服务端对齐 PowerJob 始终 200(不反压),失败可丢
		postJSON(server+"/worker/instances/"+fmt.Sprint(body.JobInstanceID)+"/logs", map[string]any{
			"level":   "info",
			"message": "示例 worker 执行完成: " + result,
			"time":    time.Now().UnixMilli(),
		})
		// 终态上报用指数退避重试:终态守护保证幂等(重复 success 不覆盖),上报丢失不致实例卡死。
		postJSONRetry(server+"/worker/instances/"+fmt.Sprint(body.JobInstanceID)+"/status", map[string]any{
			"workerAddress": workerAddr, // 归属校验:须与实例绑定的 worker 一致
			"status":        "success",  // 终态:running → success
			"result":        result,
		})
	}()
}

// postHeartbeat 上报心跳注册自己。
func postHeartbeat(server, app, addr, tags string) {
	body := map[string]any{
		"appName":         app,
		"workerAddress":   addr,
		"acceptNotTagJob": true,
		"protocol":        "http",
		"tags":            splitTags(tags),
		"systemMetrics": map[string]any{
			"cpuProcessors": runtime.NumCPU(),
			"cpuLoad":       0.5,
			"score":         10, // 选址按 score 降序;多 worker 时分数高者优先
		},
	}
	if err := postJSON(server+"/worker/heartbeat", body); err != nil {
		log.Printf("心跳失败: %v", err)
	}
}

func splitTags(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}

func postJSON(url string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s -> %d", url, resp.StatusCode)
	}
	return nil
}

// postJSONRetry 带指数退避重试的 POST,用于"服务端必须收到"的关键状态上报(running / 终态 success/failed)。
// 服务端 reportStatus 终态守护(已终态不覆盖)+ running→running 幂等,保证重复上报无副作用,对抗"上报请求
// 丢失":running 丢失致服务端不知 worker 已在执行→接收超时误杀重派;终态丢失致实例卡死直到超时。重试耗尽
// 后放弃——服务端 reaper(接收超时/worker 失联)兜底失败转移。logs 等非关键上报用 postJSON 即可。
func postJSONRetry(url string, body any) {
	// 切片驱动:索引 0 立即首次尝试(0 延迟),其后为重试退避间隔。切片长度即总尝试次数,消除"常量必须
	// 与切片长度同步"的耦合(改尝试次数只需改切片,不会因常量与长度不一致而越界 panic)。
	backoffs := []time.Duration{0, time.Second, 2 * time.Second}
	for i, b := range backoffs {
		time.Sleep(b)
		err := postJSON(url, body)
		if err == nil {
			return
		}
		log.Printf("关键状态上报失败(尝试 %d/%d): %v -> %s", i+1, len(backoffs), err, url)
	}
}
