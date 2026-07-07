package powerjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Client 是 PowerJob OpenAPI 的只读客户端,用于从外部 PowerJob server 同步任务定义。
// 与本包 server 兼容层(/openApi/*)方向相反:此处本服务作为客户端去连真正的 PowerJob。
//
// http.Client 应注入带连接超时的 Transport(见 dispatch.NewDialTransport),由调用方装配。
type Client struct {
	http *http.Client
}

// NewClient 构造客户端;h 为 nil 时用 http.DefaultClient(不建议生产用,失去连接超时控制)。
func NewClient(h *http.Client) *Client {
	if h == nil {
		h = http.DefaultClient
	}
	return &Client{http: h}
}

// pjResult 兼容真实 PowerJob ResultDTO(用 code/msg/data)与本服务兼容层(用 success)。
// 成功判定刻意宽松(不依赖某版本 code 的具体成功值):
//   - success==true → 成功;
//   - 否则 code ∈ {"200","0"} → 成功;
//   - 否则 data 非空且非 null → 成功(容错,适配未知版本)。
type pjResult struct {
	Code    json.RawMessage `json:"code,omitempty"`
	Msg     string          `json:"msg,omitempty"`
	Message string          `json:"message,omitempty"`
	Success *bool           `json:"success,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (r pjResult) ok() bool {
	if r.Success != nil && *r.Success {
		return true
	}
	if len(r.Code) > 0 {
		s := strings.TrimSpace(strings.Trim(string(r.Code), `"`))
		if s == "200" || s == "0" {
			return true
		}
	}
	return len(r.Data) > 0 && string(r.Data) != "null"
}

func (r pjResult) errMsg() string {
	if r.Msg != "" {
		return r.Msg
	}
	return r.Message
}

// FetchJobs 拉取 PowerJob 某 app 的全部 job。appName→appId 优先走 GET /appInfo/list
// (PowerJob 4.3.x,无需 app 密码);不可用或未命中时回退 POST /openApi/assert(appName[,password])。
// token 非空时带 POWERJOB-TOKEN header(OpenAPI 可选认证)。只拉取,不按 TimeExpressionType 过滤。
func (c *Client) FetchJobs(ctx context.Context, addr, appName, password, token string) ([]JobInfoDTO, error) {
	if err := validateAddr(addr); err != nil {
		return nil, err
	}
	if appName == "" {
		return nil, errors.New("appName 不能为空")
	}
	base := strings.TrimRight(addr, "/")

	appID, err := c.resolveAppID(ctx, base, appName, password, token)
	if err != nil {
		return nil, err
	}
	rawJobs, err := c.postForm(ctx, base+"/openApi/fetchAllJob",
		url.Values{"appId": {strconv.FormatInt(appID, 10)}}, token)
	if err != nil {
		return nil, fmt.Errorf("fetchAllJob 失败: %w", err)
	}
	var raws []fetchJobDTO
	if err := json.Unmarshal(rawJobs, &raws); err != nil {
		return nil, fmt.Errorf("解析 job 列表失败: %w", err)
	}
	return toJobInfoDTOs(raws), nil
}

// fetchJobDTO 仅声明同步需要的字段。真实 PowerJob 的 JobInfoDTO 含大量额外字段
// (gmtCreate/gmtModified/processorInfo 等),且某些版本对 Date 字段配了 date-format 返回
// 字符串而非毫秒;用共享的 JobInfoDTO(int64 时间)直接解析会类型冲突。此处只取所需,忽略其余。
type fetchJobDTO struct {
	ID                 int64           `json:"id"`
	JobName            string          `json:"jobName"`
	JobParams          string          `json:"jobParams,omitempty"`
	TimeExpressionType int             `json:"timeExpressionType,omitempty"`
	TimeExpression     string          `json:"timeExpression,omitempty"`
	Concurrency        int             `json:"concurrency,omitempty"`
	InstanceTimeLimit  int64           `json:"instanceTimeLimit,omitempty"`
	InstanceRetryNum   int             `json:"instanceRetryNum,omitempty"`
	StartTime          json.RawMessage `json:"startTime,omitempty"`
	EndTime            json.RawMessage `json:"endTime,omitempty"`
	Tag                string          `json:"tag,omitempty"`
	Status             int             `json:"status,omitempty"`
}

// toJobInfoDTOs 把宽松解析结果转成共享 JobInfoDTO(供 convertPJJob 用)。
// 时间字段(Date)在配 date-format 的 PowerJob 上是字符串,无法还原毫秒 → 0(=无界,等同不约束)。
func toJobInfoDTOs(raws []fetchJobDTO) []JobInfoDTO {
	out := make([]JobInfoDTO, len(raws))
	for i, r := range raws {
		out[i] = JobInfoDTO{
			ID: r.ID, JobName: r.JobName, JobParams: r.JobParams,
			TimeExpressionType: r.TimeExpressionType, TimeExpression: r.TimeExpression,
			Concurrency: r.Concurrency, InstanceTimeLimit: r.InstanceTimeLimit,
			InstanceRetryNum: r.InstanceRetryNum, Tag: r.Tag, Status: r.Status,
			StartTime: rawToMs(r.StartTime), EndTime: rawToMs(r.EndTime),
		}
	}
	return out
}

// rawToMs 把时间字段(毫秒数字 或 数字字符串)解析为 int64 毫秒;不可解析(date-format 字符串等)→ 0。
func rawToMs(raw json.RawMessage) int64 {
	if len(raw) == 0 {
		return 0
	}
	var n int64
	if json.Unmarshal(raw, &n) == nil {
		return n
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		if v, _ := strconv.ParseInt(s, 10, 64); v > 0 {
			return v
		}
	}
	return 0
}

// resolveAppID 解析 appName→appId:优先 /appInfo/list(无需 app 密码);失败回退 assert(appName[,password])。
func (c *Client) resolveAppID(ctx context.Context, base, appName, password, token string) (int64, error) {
	if id, err := c.appIDFromList(ctx, base, appName, token); err == nil && id > 0 {
		return id, nil
	}
	form := url.Values{"appName": {appName}}
	if password != "" {
		form.Set("password", password)
	}
	raw, err := c.postForm(ctx, base+"/openApi/assert", form, token)
	if err != nil {
		return 0, fmt.Errorf("解析 appId 失败(试 /appInfo/list 与 assert 均失败;若 app 设了密码请在表单填写): %w", err)
	}
	id, perr := parseAppID(raw)
	if perr != nil {
		return 0, fmt.Errorf("PowerJob 未返回有效 appId(app(%s)可能不存在): %w", appName, perr)
	}
	return id, nil
}

// appIDFromList GET /appInfo/list(PowerJob 4.3.x,无需 app 密码),按 appName 匹配 appId。
func (c *Client) appIDFromList(ctx context.Context, base, appName, token string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/appInfo/list", nil)
	if err != nil {
		return 0, err
	}
	if token != "" {
		req.Header.Set("POWERJOB-TOKEN", token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, err
	}
	if resp.StatusCode >= 400 {
		return 0, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var r pjResult
	if err := json.Unmarshal(body, &r); err != nil {
		return 0, err
	}
	if !r.ok() {
		return 0, errors.New(r.errMsg())
	}
	var apps []struct {
		ID      int64  `json:"id"`
		AppName string `json:"appName"`
	}
	if err := json.Unmarshal(r.Data, &apps); err != nil {
		return 0, err
	}
	for _, a := range apps {
		if a.AppName == appName {
			return a.ID, nil
		}
	}
	return 0, fmt.Errorf("app(%s) 未在 /appInfo/list 中找到", appName)
}

// postForm 发表单请求,返回成功响应里的 data 原始 JSON。失败时把 PowerJob msg 透出。
func (c *Client) postForm(ctx context.Context, fullURL string, form url.Values, token string) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if token != "" {
		req.Header.Set("POWERJOB-TOKEN", token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20)) // 8MB 上限,防恶意响应打爆内存
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	var r pjResult
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("响应非 JSON: %s", truncate(string(body), 200))
	}
	if !r.ok() {
		return nil, errors.New(r.errMsg())
	}
	return r.Data, nil
}

// parseAppID 解析 assert 返回的 appId(数字或字符串形式)。
func parseAppID(data json.RawMessage) (int64, error) {
	var n int64
	if err := json.Unmarshal(data, &n); err == nil && n > 0 {
		return n, nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		if v, _ := strconv.ParseInt(s, 10, 64); v > 0 {
			return v, nil
		}
	}
	return 0, fmt.Errorf("非法 appId: %s", truncate(string(data), 64))
}

func validateAddr(addr string) error {
	if addr == "" {
		return errors.New("server_address 不能为空")
	}
	u, err := url.Parse(addr)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("server_address 必须是合法 http(s) URL")
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
