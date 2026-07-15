package own

import (
	"testing"

	"tp-job/internal/protocol/powerjob"
)

// TestImportJobsCreateUpsert 覆盖:dry_run 不落库、首次新增、重复 upsert 更新、Quartz cron 算 next_run、来源标记。
func TestImportJobsCreateUpsert(t *testing.T) {
	d, _ := newDeps(t)
	appID := createDemoApp(t, d)

	pjs := []powerjob.JobInfoDTO{
		{ID: 101, JobName: "cron-job", TimeExpressionType: 2, TimeExpression: "0 0 9 * * ? *", Status: 1, Concurrency: 2},
		{ID: 102, JobName: "api-job", TimeExpressionType: 1, Status: 1},
	}

	// dry_run:不落库,只预览
	resp := d.importJobs(appID, "test", pjs, true)
	if resp.Fetched != 2 || resp.Imported != 2 || resp.Updated != 0 || resp.Skipped != 0 {
		t.Fatalf("dry_run 期望 fetched=2 imported=2 updated=0 skipped=0,得 %+v", resp)
	}
	if jobs, _, _ := d.Jobs.List(appID, 1, 10); len(jobs) != 0 {
		t.Errorf("dry_run 不应落库,但有 %d job", len(jobs))
	}

	// 正式导入:2 个新增
	resp = d.importJobs(appID, "test", pjs, false)
	if resp.Imported != 2 || resp.Updated != 0 || resp.Skipped != 0 {
		t.Fatalf("首次导入期望 imported=2,得 %+v", resp)
	}
	jobList, _, _ := d.Jobs.List(appID, 1, 10)
	if len(jobList) != 2 {
		t.Fatalf("期望落库 2 job,得 %d", len(jobList))
	}
	// 来源标记 + Quartz cron 算出 next_run_time(双引擎验证)
	for _, j := range jobList {
		if j.FromType != "powerjob" || j.FromID == "" {
			t.Errorf("来源标记错: from_id=%q from_type=%q", j.FromID, j.FromType)
		}
		if j.ScheduleKind == "cron" && j.NextRunTime == nil {
			t.Errorf("Quartz cron job 应算出 next_run_time: %+v", j)
		}
	}

	// 再次导入 → upsert(同 from_id 已存在 → 更新,不新增)。改 concurrency 验证走更新路径。
	pjs[0].Concurrency = 5
	resp = d.importJobs(appID, "test", pjs, false)
	if resp.Updated != 2 || resp.Imported != 0 {
		t.Fatalf("重复导入期望 updated=2 imported=0,得 %+v", resp)
	}
	if jobList, _, _ := d.Jobs.List(appID, 1, 10); len(jobList) != 2 {
		t.Errorf("upsert 后应仍 2 job(不重复创建),得 %d", len(jobList))
	}
}

// TestImportJobsBadCronSkipped 非法 cron(两引擎皆败)计入 Skipped,不阻断其他 job。
func TestImportJobsBadCronSkipped(t *testing.T) {
	d, _ := newDeps(t)
	appID := createDemoApp(t, d)

	pjs := []powerjob.JobInfoDTO{
		{ID: 1, JobName: "bad", TimeExpressionType: 2, TimeExpression: "not-a-cron", Status: 1},
		{ID: 2, JobName: "good", TimeExpressionType: 2, TimeExpression: "0 0 9 * * ? *", Status: 1},
	}
	resp := d.importJobs(appID, "test", pjs, false)
	if resp.Skipped != 1 || resp.Imported != 1 {
		t.Fatalf("期望 skipped=1 imported=1,得 %+v", resp)
	}
	// 预览里 bad 项应有 error 文案
	var badItem *ImportPowerJobItem
	for i := range resp.Preview {
		if resp.Preview[i].Name == "bad" {
			badItem = &resp.Preview[i]
		}
	}
	if badItem == nil || badItem.Error == "" {
		t.Errorf("bad 项应含 error 说明,得 %+v", badItem)
	}
}

// TestImportJobsExpiredCronImported 合法但已过期的一次性 cron(无未来触发)应照常导入(next_run=nil 不触发),
// 不计入 Skipped,并给 warning——对齐 PowerJob 允许保存过期 job。
func TestImportJobsExpiredCronImported(t *testing.T) {
	d, _ := newDeps(t)
	appID := createDemoApp(t, d)

	pjs := []powerjob.JobInfoDTO{
		{ID: 1, JobName: "expired", TimeExpressionType: 2, TimeExpression: "0 0 9 1 1 ? 2020", Status: 1},
		{ID: 2, JobName: "good", TimeExpressionType: 2, TimeExpression: "0 0 9 * * ? *", Status: 1},
	}
	resp := d.importJobs(appID, "test", pjs, false)
	if resp.Skipped != 0 || resp.Imported != 2 {
		t.Fatalf("过期 cron 不应跳过:期望 skipped=0 imported=2,得 %+v", resp)
	}
	var expired *ImportPowerJobItem
	for i := range resp.Preview {
		if resp.Preview[i].Name == "expired" {
			expired = &resp.Preview[i]
		}
	}
	if expired == nil || expired.Error != "" || expired.Warning == "" {
		t.Errorf("expired 项应有 warning 无 error,得 %+v", expired)
	}
	// 落库校验:过期 job 的 next_run=nil(不触发),good job 应算出 next_run
	jobList, _, _ := d.Jobs.List(appID, 1, 10)
	for _, j := range jobList {
		if j.Name == "expired" && j.NextRunTime != nil {
			t.Errorf("过期 cron 导入后 next_run 应 nil,得 %v", j.NextRunTime)
		}
		if j.Name == "good" && j.NextRunTime == nil {
			t.Errorf("正常 cron 应算出 next_run_time")
		}
	}
}

// createDemoApp 走 handler 建 app,返回 appID(复用 handler_test 的 req/bodyData)。
func createDemoApp(t *testing.T, d Deps) int64 {
	t.Helper()
	w := req(t, "POST", "/api/apps", CreateAppReq{AppName: "demo", Password: "p"}, d)
	if w.Code != 200 {
		t.Fatalf("createApp 应 200,得 %d: %s", w.Code, w.Body.String())
	}
	return int64(bodyData(t, w)["data"].(map[string]any)["id"].(float64))
}
