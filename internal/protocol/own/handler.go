// Package own 的 handler:把 /api/* 绑定到 dservice(管理端 REST)。
//
// 鉴权(SessionAuth + 权限矩阵)在阶段 4 由 api 装配层注入中间件;此处 handler 仅做
// HTTP↔dto 绑定 + 越权防护(路径 :appId 与身份校验在中间件)。
package own

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"tp-job/internal/auth"
	"tp-job/internal/dservice"
	"tp-job/internal/protocol/powerjob"
	"tp-job/internal/repository"
	"tp-job/internal/workerreg"
)

// Deps own 协议依赖。
type Deps struct {
	Apps       *dservice.AppService
	Jobs       *dservice.JobService
	Instances  *dservice.InstanceService
	Store      *repository.Store
	AdminUsers *dservice.AdminUserService // /account/* 用;其余路由可空

	// Reg worker 心跳注册表(读在线 worker 列表)。为 nil 时 listWorkers 返回空(单测场景)。
	Reg *workerreg.Registry

	// PowerJobClient 用于"从 PowerJob 同步任务"(admin 主动拉取外部 PowerJob server)。
	// 为 nil 时 import-powerjob 端点返回 503(未装配,如单测)。
	PowerJobClient *powerjob.Client

	// Auth 非 nil 时,Register 给路由按权限矩阵挂鉴权中间件(SessionAuth + RequireAdmin/AppScope);
	// 为 nil 则不鉴权(单测/未装配场景,向后兼容)。
	Auth *auth.Store
}

// routeDef 描述一条 /api 路由及其鉴权级别。admin=true → 仅管理员;false → 管理员或 app 自家(AppScope)。
type routeDef struct {
	method  string
	path    string
	handler gin.HandlerFunc
	admin   bool
}

// ownRoutes 全部资源路由 + 各自鉴权级别(矩阵见 docs/design.md §10)。
// 集中定义避免"加路由要改两处"。app 管理(新增/列出/删除)仅 admin;查看/修改 app 与
// app 名下资源均为 admin 任意 / app 仅自家。
func ownRoutes(d Deps) []routeDef {
	return []routeDef{
		{"POST", "/apps", d.createApp, true},
		{"GET", "/apps", d.listApps, true},
		{"GET", "/apps/:appId", d.getApp, false},
		{"PUT", "/apps/:appId", d.updateApp, false},
		{"DELETE", "/apps/:appId", d.deleteApp, true},

		{"POST", "/apps/:appId/jobs", d.createJob, false},
		{"GET", "/apps/:appId/jobs", d.listJobs, false},
		{"GET", "/apps/:appId/jobs/:id", d.getJob, false},
		{"PUT", "/apps/:appId/jobs/:id", d.updateJob, false},
		{"DELETE", "/apps/:appId/jobs/:id", d.deleteJob, false},
		{"POST", "/apps/:appId/jobs/:id/trigger", d.triggerJob, false},
		{"POST", "/apps/:appId/jobs/import-powerjob", d.importPowerJob, true}, // admin:SSRF 风险

		{"GET", "/apps/:appId/instances", d.listInstances, false},
		{"GET", "/apps/:appId/instances/:iid", d.getInstance, false},
		{"POST", "/apps/:appId/instances/:iid/stop", d.stopInstance, false},
		{"POST", "/apps/:appId/instances/:iid/retry", d.retryInstance, false},
		{"GET", "/apps/:appId/instances/:iid/logs", d.instanceLogs, false},

		{"GET", "/apps/:appId/workers", d.listWorkers, false},
	}
}

// Register 挂载 /api 资源路由到 group。调用方负责:公开的 /api/auth/login 用 LoginHandler
// 另行挂载(不走鉴权);/api/auth/me、/api/auth/logout 用 RegisterAuth 挂载。
//
// 鉴权矩阵在 d.Auth != nil 时生效:每条路由前置 [SessionAuth, (RequireAdmin|AppScope)],
// 保证身份解析与越权校验在 handler 之前完成。
func Register(r *gin.RouterGroup, d Deps) {
	var sa, adm, scope gin.HandlerFunc
	if d.Auth != nil {
		sa = auth.SessionAuth(d.Auth)
		adm = auth.RequireAdmin()
		scope = auth.AppScope("appId")
	}
	for _, rt := range ownRoutes(d) {
		var handlers []gin.HandlerFunc
		if d.Auth != nil {
			mw := scope
			if rt.admin {
				mw = adm
			}
			handlers = []gin.HandlerFunc{sa, mw, rt.handler}
		} else {
			handlers = []gin.HandlerFunc{rt.handler}
		}
		switch rt.method {
		case "GET":
			r.GET(rt.path, handlers...)
		case "POST":
			r.POST(rt.path, handlers...)
		case "PUT":
			r.PUT(rt.path, handlers...)
		case "DELETE":
			r.DELETE(rt.path, handlers...)
		}
	}
}

// ===== App =====

func (d Deps) createApp(c *gin.Context) {
	var req CreateAppReq
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "参数错误: "+err.Error())
		return
	}
	app, err := d.Apps.Create(req.AppName, req.Password, req.Status)
	if err != nil {
		fail(c, badStatus(err), err.Error())
		return
	}
	ok(c, AppToView(app))
}

func (d Deps) listApps(c *gin.Context) {
	page, size := parsePage(c)
	apps, total, err := d.Apps.List(c.Query("keyword"), page, size)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	views := make([]AppView, 0, len(apps))
	for i := range apps {
		views = append(views, AppToView(&apps[i]))
	}
	ok(c, gin.H{"list": views, "total": total})
}

func (d Deps) getApp(c *gin.Context) {
	app, err := d.Apps.Get(paramInt64(c, "appId"))
	if err != nil {
		fail(c, notFoundStatus(err), err.Error())
		return
	}
	ok(c, AppToView(app))
}

func (d Deps) updateApp(c *gin.Context) {
	var req UpdateAppReq
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "参数错误: "+err.Error())
		return
	}
	appID := paramInt64(c, "appId")
	// app 角色改密码须验旧密码(对齐管理员 changePassword,防 token 泄露后被重置长期持有);
	// admin 改任意 app 不校验(特权)。仅当本次要改密码(req.Password 非空)时校验。
	if sess, ok := auth.SessionFrom(c); ok && !sess.IsAdmin() && req.Password != nil && *req.Password != "" {
		old := ""
		if req.OldPassword != nil {
			old = *req.OldPassword
		}
		if err := d.Apps.VerifyOldPassword(appID, old); err != nil {
			fail(c, http.StatusBadRequest, "旧密码校验失败")
			return
		}
	}
	if err := d.Apps.Update(appID, req.AppName, req.Password, req.Status); err != nil {
		fail(c, badStatus(err), err.Error())
		return
	}
	ok(c, gin.H{"id": appID})
}

func (d Deps) deleteApp(c *gin.Context) {
	if err := d.Apps.Delete(paramInt64(c, "appId")); err != nil {
		fail(c, badStatus(err), err.Error())
		return
	}
	ok(c, gin.H{"id": paramInt64(c, "appId")})
}

// ===== Job =====

func (d Deps) createJob(c *gin.Context) {
	var req CreateJobReq
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "参数错误: "+err.Error())
		return
	}
	appID := paramInt64(c, "appId")
	job, err := CreateJobReqToJob(appID, req)
	if err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := d.Jobs.Create(job); err != nil {
		fail(c, badStatus(err), err.Error())
		return
	}
	ok(c, JobToView(job))
}

func (d Deps) listJobs(c *gin.Context) {
	page, size := parsePage(c)
	jobs, total, err := d.Jobs.List(paramInt64(c, "appId"), page, size)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	views := make([]JobView, 0, len(jobs))
	for i := range jobs {
		views = append(views, JobToView(&jobs[i]))
	}
	ok(c, gin.H{"list": views, "total": total})
}

func (d Deps) getJob(c *gin.Context) {
	job, err := d.Jobs.Get(paramInt64(c, "appId"), paramInt64(c, "id"))
	if err != nil {
		fail(c, notFoundStatus(err), err.Error())
		return
	}
	ok(c, JobToView(job))
}

func (d Deps) updateJob(c *gin.Context) {
	var req UpdateJobReq
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "参数错误: "+err.Error())
		return
	}
	if err := d.Jobs.Update(paramInt64(c, "appId"), paramInt64(c, "id"), UpdateJobReqToFields(req)); err != nil {
		fail(c, badStatus(err), err.Error())
		return
	}
	ok(c, gin.H{"id": paramInt64(c, "id")})
}

func (d Deps) deleteJob(c *gin.Context) {
	if err := d.Jobs.Delete(paramInt64(c, "appId"), paramInt64(c, "id")); err != nil {
		fail(c, badStatus(err), err.Error())
		return
	}
	ok(c, gin.H{"id": paramInt64(c, "id")})
}

func (d Deps) triggerJob(c *gin.Context) {
	appID := paramInt64(c, "appId")
	id := paramInt64(c, "id")
	priority, _ := strconv.Atoi(c.DefaultQuery("priority", "0"))
	if err := d.Jobs.Trigger(appID, id, priority, c.Query("instance_params"), "api"); err != nil {
		fail(c, notFoundStatus(err), err.Error())
		return
	}
	ok(c, gin.H{"id": id, "triggered": true, "priority": priority})
}

// importPowerJob POST /apps/:appId/jobs/import-powerjob(仅 admin):
// 作为 PowerJob OpenAPI 客户端拉取外部 server 的 job 定义,转换并 upsert 到当前 app。
// dry_run=true 仅预览不落库。SSRF 防护由装配层注入的 Transport(PowerJobClient.http)保证。
func (d Deps) importPowerJob(c *gin.Context) {
	var req ImportPowerJobReq
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "参数错误: "+err.Error())
		return
	}
	if d.PowerJobClient == nil {
		fail(c, http.StatusServiceUnavailable, "PowerJob 同步客户端未装配")
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()
	pjs, err := d.PowerJobClient.FetchJobs(ctx, req.ServerAddress, req.AppName, req.Password, req.Token)
	if err != nil {
		fail(c, http.StatusBadRequest, "拉取 PowerJob 任务失败: "+err.Error())
		return
	}
	ok(c, d.importJobs(paramInt64(c, "appId"), fingerprint(req.ServerAddress), pjs, req.DryRun))
}

// ===== Instance =====

func (d Deps) listInstances(c *gin.Context) {
	page, size := parsePage(c)
	f := repository.InstanceFilter{
		AppID: paramInt64(c, "appId"),
		JobID: paramInt64Query(c, "job_id"),
		Status: c.Query("status"),
		Page: page, Size: size,
	}
	list, total, err := d.Store.Instance.List(f)
	if err != nil {
		fail(c, http.StatusInternalServerError, err.Error())
		return
	}
	// 批量查 job 的 schedule_kind 填入视图(消除逐实例 N+1)
	jobSet := make(map[int64]struct{}, len(list))
	for i := range list {
		jobSet[list[i].JobID] = struct{}{}
	}
	ids := make([]int64, 0, len(jobSet))
	for id := range jobSet {
		ids = append(ids, id)
	}
	kinds := make(map[int64]string, len(ids))
	if len(ids) > 0 {
		jobs, err := d.Store.Job.ListByIDs(ids)
		if err != nil {
			fail(c, http.StatusInternalServerError, err.Error())
			return
		}
		for i := range jobs {
			kinds[jobs[i].ID] = jobs[i].ScheduleKind
		}
	}
	views := make([]InstanceView, 0, len(list))
	for i := range list {
		v := InstanceToView(&list[i])
		v.ScheduleKind = kinds[list[i].JobID]
		views = append(views, v)
	}
	ok(c, gin.H{"list": views, "total": total})
}

func (d Deps) getInstance(c *gin.Context) {
	// 经 GetInApp 校验实例归属 :appId,防 app 角色越权读其他 app 实例
	// (AppScope 只校验 :appId 路径参数,不校验 :iid 归属)。
	ins, err := d.Instances.GetInApp(paramInt64(c, "appId"), paramInt64(c, "iid"))
	if err != nil {
		fail(c, notFoundStatus(err), err.Error())
		return
	}
	ok(c, InstanceToView(ins))
}

func (d Deps) instanceLogs(c *gin.Context) {
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "500"))
	lines, total, err := d.Instances.LogsInApp(paramInt64(c, "appId"), paramInt64(c, "iid"), dservice.LogQuery{
		Offset: offset, Limit: limit,
	})
	if err != nil {
		fail(c, notFoundStatus(err), err.Error())
		return
	}
	ok(c, gin.H{"lines": lines, "total": total})
}

// stopInstance POST /apps/:appId/instances/:iid/stop:标记 stopped 并释放并发槽(停止排队/运行中实例)。
// 先 GetInApp 校验 :iid 归属当前 app(AppScope 只验 :appId 路径,不验 :iid),防 app 角色越权操作他 app 实例。
func (d Deps) stopInstance(c *gin.Context) {
	iid := paramInt64(c, "iid")
	if _, err := d.Instances.GetInApp(paramInt64(c, "appId"), iid); err != nil {
		fail(c, notFoundStatus(err), err.Error())
		return
	}
	if err := d.Instances.Stop(iid); err != nil {
		fail(c, badStatus(err), err.Error())
		return
	}
	ok(c, gin.H{"id": iid})
}

// retryInstance POST /apps/:appId/instances/:iid/retry:立即重排一个 failed/timeout 实例(交 RetryPump)。
// 归属校验同 cancelInstance;非可重试态/无余力返回 400(ErrInstanceNotRetryable)。
func (d Deps) retryInstance(c *gin.Context) {
	iid := paramInt64(c, "iid")
	if _, err := d.Instances.GetInApp(paramInt64(c, "appId"), iid); err != nil {
		fail(c, notFoundStatus(err), err.Error())
		return
	}
	if err := d.Instances.Retry(iid); err != nil {
		fail(c, badStatus(err), err.Error())
		return
	}
	ok(c, gin.H{"id": iid})
}

// ===== Worker =====

// listWorkers 列出 app 名下在线 worker(读 workerreg 内存注册表;不入库)。
func (d Deps) listWorkers(c *gin.Context) {
	appID := paramInt64(c, "appId")
	views := []WorkerView{}
	if d.Reg != nil {
		ws := d.Reg.Online(appID)
		for i := range ws {
			views = append(views, WorkerToView(&ws[i]))
		}
	}
	ok(c, gin.H{"list": views, "count": len(views)})
}

// ===== 小工具 =====

func ok(c *gin.Context, data any) {
	c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "ok", "data": data})
}

func fail(c *gin.Context, status int, msg string) {
	c.JSON(status, gin.H{"code": status, "msg": msg})
}

func paramInt64(c *gin.Context, key string) int64 {
	n, _ := strconv.ParseInt(c.Param(key), 10, 64)
	return n
}

func paramInt64Query(c *gin.Context, key string) int64 {
	n, _ := strconv.ParseInt(c.Query(key), 10, 64)
	return n
}

func parsePage(c *gin.Context) (int, int) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	size, _ := strconv.Atoi(c.DefaultQuery("size", "20"))
	if page < 1 {
		page = 1
	}
	if page > 100000 { // 防 page 过大导致巨大 OFFSET 慢查询(DoS);超大数据集应改游标分页
		page = 100000
	}
	if size < 1 || size > 500 {
		size = 20
	}
	return page, size
}

// badStatus 把 service 校验/状态错误映射到 400/409,其余 500。
func badStatus(err error) int {
	switch {
	case isSentinel(err, dservice.ErrAppValidate), isSentinel(err, dservice.ErrJobValidate),
		isSentinel(err, dservice.ErrInstanceValidate), isSentinel(err, dservice.ErrInstanceNotRetryable):
		return http.StatusBadRequest
	case isSentinel(err, dservice.ErrAppInUse):
		return http.StatusConflict
	}
	return http.StatusInternalServerError
}

func notFoundStatus(err error) int {
	if isSentinel(err, dservice.ErrAppNotFound) || isSentinel(err, dservice.ErrJobNotFound) ||
		isSentinel(err, dservice.ErrInstanceNotFound) {
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}

func isSentinel(err, target error) bool {
	return errors.Is(err, target)
}
