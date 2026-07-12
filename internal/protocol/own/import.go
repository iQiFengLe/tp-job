package own

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strconv"
	"strings"
	"time"

	"tp-job/internal/domain"
	"tp-job/internal/protocol/powerjob"
	"tp-job/internal/schedtime"
)

// 本文件实现 PowerJob → tp-job 的任务同步落库逻辑(handler 在 handler.go)。
// 转换 + 落库放在 own 包而非 dservice:powerjob 包(server 兼容层)已依赖 dservice,
// 反向依赖会循环 import;own 依赖 powerjob 不构成循环。

// pjScheduleKind PowerJob TimeExpressionType → tp-job ScheduleKind。
// 1=API 2=CRON 3=FIX_RATE 4=FIX_DELAY;其余/未知 → api(不自动调度,避免误触发)。
func pjScheduleKind(t int) string {
	switch t {
	case 2:
		return "cron"
	case 3:
		return "fix_rate"
	case 4:
		return "fix_delay"
	default:
		return "api"
	}
}

// pjMs PowerJob 毫秒戳 → *time.Time;<=0 → nil(无界)。
func pjMs(ms int64) *time.Time {
	if ms <= 0 {
		return nil
	}
	t := time.UnixMilli(ms)
	return &t
}

// fingerprint 把 PowerJob server 地址规范化后取 sha256 前 12 hex,作为 FromID 的来源命名空间。
// 规范化:scheme://host:port(host 小写、去 trailing slash),保证 http://H:7700/ 与 http://h:7700
// 落到同一 key;不同 server(host/port/scheme 任一不同)落到不同 key——避免跨 PowerJob server 同 ID job 互相覆盖。
// parse 失败回退 hash 原始串(FetchJobs 入口已 validateAddr 挡非法,此处兜底防御)。
func fingerprint(addr string) string {
	normalized := addr
	if u, err := url.Parse(addr); err == nil && u.Host != "" {
		scheme := u.Scheme
		if scheme == "" {
			scheme = "http"
		}
		hostPort := strings.ToLower(u.Hostname())
		if u.Port() != "" {
			hostPort = hostPort + ":" + u.Port()
		}
		normalized = scheme + "://" + hostPort
	}
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:6]) // 12 hex 字符
}

// convertPJJob 把 PowerJob JobInfoDTO 转成 domain.Job(落到 appID,标 powerjob 来源)。
// FromID 含来源 server 指纹:"pj:<serverKey>:<原jobID>"——跨 PowerJob server 同 ID 不冲突。
// 丢弃 processorInfo/processorType——执行模型不同(PowerJob Java processor vs 本服务 http 派发),
// 同步只搬调度定义;执行需当前 app 下挂匹配 tag 的 http worker。
func convertPJJob(pj powerjob.JobInfoDTO, appID int64, serverKey string) *domain.Job {
	name := pj.JobName
	if name == "" {
		name = "powerjob-" + strconv.FormatInt(pj.ID, 10)
	}
	conc := pj.Concurrency
	if conc < 1 {
		conc = 1
	}
	return &domain.Job{
		AppID:          appID,
		Name:           name,
		Description:    pj.JobDescription,
		ExecuteType:    "http",
		JobParams:      pj.JobParams,
		Tag:            pj.Tag,
		TimeoutSec:     int(pj.InstanceTimeLimit / 1000),
		ScheduleKind:   pjScheduleKind(pj.TimeExpressionType),
		ScheduleExpr:   pj.TimeExpression,
		StartTime:      pjMs(pj.StartTime),
		EndTime:        pjMs(pj.EndTime),
		MaxConcurrency: conc,
		RetryCount:     pj.InstanceRetryNum,
		Enabled:        pj.Status == 1,
		FromID:         "pj:" + serverKey + ":" + strconv.FormatInt(pj.ID, 10),
		FromType:       "powerjob",
	}
}

// isAutoKind 是否自动调度类型(需校验表达式合法性)。api/run_at 不自动调度。
func isAutoKind(kind string) bool {
	switch kind {
	case "cron", "fix_rate", "fix_delay", "delay":
		return true
	}
	return false
}

// jobToUpdateFields 把转换后的 job 拆成 Jobs.Update 可消费的 fields(调度字段;id/from 不动)。
func jobToUpdateFields(j *domain.Job) map[string]any {
	return map[string]any{
		"name":            j.Name,
		"description":     j.Description,
		"job_params":      j.JobParams,
		"tag":             j.Tag,
		"timeout_sec":     j.TimeoutSec,
		"schedule_kind":   j.ScheduleKind,
		"schedule_expr":   j.ScheduleExpr,
		"start_time":      j.StartTime,
		"end_time":        j.EndTime,
		"max_concurrency": j.MaxConcurrency,
		"retry_count":     j.RetryCount,
		"enabled":         j.Enabled,
	}
}

// importJobs 把 PowerJob 拉到的 job 列表转换并落库到 appID(同源已存在则 upsert 更新)。
// serverKey 为来源 PowerJob server 指纹(进 FromID 命名空间,见 fingerprint)。dryRun=true 仅预览不落库。
// 判重用一次批量 ListByFrom(消除逐条 GetByFrom 的 N+1);单条表达式非法计入 Skipped,不中断整体。
func (d Deps) importJobs(appID int64, serverKey string, pjs []powerjob.JobInfoDTO, dryRun bool) ImportPowerJobResp {
	resp := ImportPowerJobResp{Fetched: len(pjs), Preview: make([]ImportPowerJobItem, 0, len(pjs))}
	now := time.Now()

	// 一轮转换 + 收集 fromID(批量判重用)
	jobs := make([]*domain.Job, len(pjs))
	fromIDs := make([]string, 0, len(pjs))
	for i := range pjs {
		jobs[i] = convertPJJob(pjs[i], appID, serverKey)
		fromIDs = append(fromIDs, jobs[i].FromID)
	}

	// 批量查现有来源 job(1 次 IN 查询替代 N 次 GetByFrom)。整批失败时逐条标 Skipped 不中断。
	existing, err := d.Store.Job.ListByFrom(appID, "powerjob", fromIDs)
	if err != nil {
		for i := range jobs {
			resp.Preview = append(resp.Preview, ImportPowerJobItem{
				Name: jobs[i].Name, ScheduleKind: jobs[i].ScheduleKind,
				ScheduleExpr: jobs[i].ScheduleExpr, Enabled: jobs[i].Enabled,
				Error: "查询失败: " + err.Error(),
			})
			resp.Skipped++
		}
		return resp
	}

	for i := range jobs {
		job := jobs[i]
		item := ImportPowerJobItem{
			Name: job.Name, ScheduleKind: job.ScheduleKind,
			ScheduleExpr: job.ScheduleExpr, Enabled: job.Enabled,
		}
		existingJob := existing[job.FromID]
		item.Conflict = existingJob != nil

		// 自动调度类型的表达式预检(Quartz cron 由双引擎兼容;非法/无未来触发则跳过)。
		if isAutoKind(job.ScheduleKind) {
			if _, e := schedtime.NextByKind(job.ScheduleKind, job.ScheduleExpr, now); e != nil {
				item.Error = "表达式非法或无未来触发: " + e.Error()
				resp.Skipped++
				resp.Preview = append(resp.Preview, item)
				continue
			}
		}

		if dryRun {
			if item.Conflict {
				resp.Updated++
			} else {
				resp.Imported++
			}
			resp.Preview = append(resp.Preview, item)
			continue
		}

		if item.Conflict {
			// Update 经 dservice.Update:调度字段变化时重算 next_run_time。
			if e := d.Jobs.Update(appID, existingJob.ID, jobToUpdateFields(job)); e != nil {
				item.Error = "更新失败: " + e.Error()
				resp.Skipped++
			} else {
				resp.Updated++
			}
		} else {
			// Create 会补来源校验 + 算 next_run;job.FromID 已设,不会被自建 uuid 覆盖。
			if e := d.Jobs.Create(job); e != nil {
				item.Error = "创建失败: " + e.Error()
				resp.Skipped++
			} else {
				resp.Imported++
			}
		}
		resp.Preview = append(resp.Preview, item)
	}
	return resp
}
