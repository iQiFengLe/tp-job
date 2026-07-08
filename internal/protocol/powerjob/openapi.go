// OpenAPI 兼容层(/openApi/*):对齐原版 PowerJob server 的 OpenAPIController,
// 让原对接 PowerJob 的业务客户端(自研 HTTP 调用)零改动接入 task-schedule。
//
// 覆盖 PowerJob OpenAPI 的 App/Job/Instance 区共 18 个端点(路径/DTO 对齐 powerjob-common)。
// Workflow/WorkflowInstance 区(13 个)因 task-schedule 无工作流模型,未实现。不支持官方 Java SDK。
//
// 鉴权对齐 PowerJob OpenAPI 默认信任 + 本项目"靠网络隔离"约定(业务客户端普遍不带 token 直连);
// 生产隔离由部署侧保证(见 deploy/nginx-isolation.conf.example,勿暴露公网)。
package powerjob

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"task-schedule/internal/domain"
	"task-schedule/internal/dservice"
	"task-schedule/internal/repository"
)

// OpenApiDeps OpenAPI 依赖。Store 供 fetchAllJob/queryJob/queryInstance 等灵活查询。
type OpenApiDeps struct {
	Jobs      *dservice.JobService
	Instances *dservice.InstanceService
	Apps      *dservice.AppService
	Store     *repository.Store
}

// RegisterOpenApi 挂载 /openApi/*(路径对齐 PowerJob OpenAPIConstant.WEB_PATH="/openApi")。
func RegisterOpenApi(r *gin.RouterGroup, d OpenApiDeps) {
	// App
	r.POST("/assert", d.assertApp)
	// Job
	r.POST("/saveJob", d.saveJob)
	r.POST("/copyJob", d.copyJob)
	r.POST("/exportJob", d.exportJob)
	r.POST("/fetchJob", d.fetchJob)
	r.POST("/fetchAllJob", d.fetchAllJob)
	r.POST("/queryJob", d.queryJob)
	r.POST("/deleteJob", d.deleteJob)
	r.POST("/disableJob", d.disableJob)
	r.POST("/enableJob", d.enableJob)
	r.POST("/runJob", d.runJob)
	r.POST("/runJob2", d.runJob2)
	// Instance
	r.POST("/stopInstance", d.stopInstance)
	r.POST("/cancelInstance", d.cancelInstance)
	r.POST("/retryInstance", d.retryInstance)
	r.POST("/fetchInstanceStatus", d.fetchInstanceStatus)
	r.POST("/fetchInstanceInfo", d.fetchInstanceInfo)
	r.POST("/queryInstance", d.queryInstance)
}

// PowerResultDTO 对齐 PowerJob PowerResultDTO:ResultDTO + code(runJob2 用)。
type PowerResultDTO struct {
	Success bool   `json:"success"`
	Data    any    `json:"data,omitempty"`
	Message string `json:"message,omitempty"`
	Code    string `json:"code,omitempty"`
}

func PowerResultOK(data any) PowerResultDTO { return PowerResultDTO{Success: true, Data: data} }
func PowerResultFail(msg string) PowerResultDTO {
	return PowerResultDTO{Success: false, Message: msg}
}

// ===== PowerJob DTO(字段名/类型对齐 powerjob-common response) =====

// JobInfoDTO 对齐 tech.powerjob.common.response.JobInfoDTO。task-schedule 无的概念(processor/logConfig 等)省略或零值。
type JobInfoDTO struct {
	ID                 int64  `json:"id"`
	JobName            string `json:"jobName"`
	JobDescription     string `json:"jobDescription,omitempty"`
	AppID              int64  `json:"appId"`
	JobParams          string `json:"jobParams,omitempty"`
	TimeExpressionType int    `json:"timeExpressionType,omitempty"` // 1 API/2 CRON/3 FIX_RATE/4 FIX_DELAY
	TimeExpression     string `json:"timeExpression,omitempty"`
	ExecuteType        int    `json:"executeType,omitempty"`  // task-schedule 固定 http→1 STANDALONE
	ProcessorType      int    `json:"processorType,omitempty"` // 占位 1(JAVA)
	ProcessorInfo      string `json:"processorInfo,omitempty"`
	MaxInstanceNum     int    `json:"maxInstanceNum,omitempty"`
	Concurrency        int    `json:"concurrency,omitempty"`
	InstanceTimeLimit  int64  `json:"instanceTimeLimit,omitempty"` // ms
	InstanceRetryNum   int    `json:"instanceRetryNum,omitempty"`
	Status             int    `json:"status,omitempty"` // 1 正常 / 2 停止
	NextTriggerTime    int64  `json:"nextTriggerTime,omitempty"` // ms
	StartTime          int64  `json:"startTime,omitempty"`       // ms 生效起始
	EndTime            int64  `json:"endTime,omitempty"`         // ms 生效截止
	Tag                string `json:"tag,omitempty"`
	GmtCreate          int64  `json:"gmtCreate,omitempty"`  // ms
	GmtModified        int64  `json:"gmtModified,omitempty"` // ms
}

// InstanceInfoDTO 对齐 tech.powerjob.common.response.InstanceInfoDTO。
type InstanceInfoDTO struct {
	JobID               int64  `json:"jobId"`
	AppID               int64  `json:"appId"`
	InstanceID          int64  `json:"instanceId"`
	JobParams           string `json:"jobParams,omitempty"`
	InstanceParams      string `json:"instanceParams,omitempty"`
	Status              int    `json:"status"` // PowerJob 数字码
	Result              string `json:"result,omitempty"`
	ExpectedTriggerTime int64  `json:"expectedTriggerTime,omitempty"` // ms
	ActualTriggerTime   int64  `json:"actualTriggerTime,omitempty"`   // ms
	FinishedTime        int64  `json:"finishedTime,omitempty"`        // ms
	TaskTrackerAddress  string `json:"taskTrackerAddress,omitempty"`
	RunningTimes        int64  `json:"runningTimes,omitempty"`
	GmtCreate           int64  `json:"gmtCreate,omitempty"`
	GmtModified         int64  `json:"gmtModified,omitempty"`
}

// SaveJobReq 对齐 SaveJobInfoRequest(指针字段区分未提供/零值)。task-schedule 仅消费能存储的子集。
type SaveJobReq struct {
	ID                 *int64  `json:"id,omitempty"`
	JobName            *string `json:"jobName,omitempty"`
	JobDescription     *string `json:"jobDescription,omitempty"`
	AppID              *int64  `json:"appId,omitempty"`
	JobParams          *string `json:"jobParams,omitempty"`
	TimeExpressionType *int    `json:"timeExpressionType,omitempty"`
	TimeExpression     *string `json:"timeExpression,omitempty"`
	StartTime          *int64  `json:"startTime,omitempty"` // ms 生效起始
	EndTime            *int64  `json:"endTime,omitempty"`   // ms 生效截止
	Concurrency        *int    `json:"concurrency,omitempty"`
	InstanceTimeLimit  *int64  `json:"instanceTimeLimit,omitempty"`
	InstanceRetryNum   *int    `json:"instanceRetryNum,omitempty"`
	Enable             *bool   `json:"enable,omitempty"`
	Tag                *string `json:"tag,omitempty"`
}

// RunJobReq 对齐 RunJobRequest(runJob2 body)。
type RunJobReq struct {
	JobID          *int64 `json:"jobId,omitempty"`
	InstanceParams string `json:"instanceParams,omitempty"`
	Delay          *int64 `json:"delay,omitempty"`
	AppID          *int64 `json:"appId,omitempty"`
}

// JobInfoQuery 对齐 tech.powerjob.common.request.query.JobInfoQuery(实现其常用子集)。
type JobInfoQuery struct {
	IDEq        *int64  `json:"idEq,omitempty"`
	JobNameEq   *string `json:"jobNameEq,omitempty"`
	JobNameLike *string `json:"jobNameLike,omitempty"`
	TagEq       *string `json:"tagEq,omitempty"`
}

// InstancePageQuery 对齐 InstancePageQuery(+PowerPageQuery 的 index/pageSize)。
type InstancePageQuery struct {
	Index        int    `json:"index,omitempty"`
	PageSize     int    `json:"pageSize,omitempty"`
	InstanceIDEq *int64 `json:"instanceIdEq,omitempty"`
	JobIDEq      *int64 `json:"jobIdEq,omitempty"`
	StatusIn     []int  `json:"statusIn,omitempty"`
}

// PageResult 对齐 tech.powerjob.common.response.PageResult。
type PageResult struct {
	Index      int   `json:"index"`
	PageSize   int   `json:"pageSize"`
	TotalPages int   `json:"totalPages"`
	TotalItems int64 `json:"totalItems"`
	Data       any   `json:"data"`
}

// ===== 翻译 =====

// scheduleKindToWire domain.ScheduleKind → PowerJob TimeExpressionType.v。
func scheduleKindToWire(k string) int {
	switch k {
	case "cron":
		return 2
	case "fix_rate":
		return 3
	case "fix_delay":
		return 4
	default: // api / run_at / delay → API(一次性)
		return 1
	}
}

func wireToScheduleKind(t int) string {
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

func ms(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.UnixMilli()
}

// msToTimePtr 毫秒时间戳 → *time.Time;nil/非正 → nil(无界)。
func msToTimePtr(v *int64) *time.Time {
	if v == nil || *v <= 0 {
		return nil
	}
	t := time.UnixMilli(*v)
	return &t
}

func jobToDTO(j *domain.Job) JobInfoDTO {
	status := 2
	if j.Enabled {
		status = 1
	}
	return JobInfoDTO{
		ID: j.ID, JobName: j.Name, JobDescription: j.Description, AppID: j.AppID, JobParams: j.JobParams,
		TimeExpressionType: scheduleKindToWire(j.ScheduleKind),
		TimeExpression:     j.ScheduleExpr,
		ExecuteType:        1, ProcessorType: 1,
		MaxInstanceNum:    j.MaxConcurrency,
		Concurrency:       j.MaxConcurrency,
		InstanceTimeLimit: int64(j.TimeoutSec) * 1000,
		InstanceRetryNum:  j.RetryCount,
		Status:            status,
		NextTriggerTime:   ms(j.NextRunTime),
		StartTime:         ms(j.StartTime),
		EndTime:           ms(j.EndTime),
		Tag:               j.Tag,
		GmtCreate:         j.CreatedAt.UnixMilli(),
		GmtModified:       j.UpdatedAt.UnixMilli(),
	}
}

func instanceToDTO(ins *domain.Instance, jobParams string) InstanceInfoDTO {
	return InstanceInfoDTO{
		JobID:               ins.JobID, AppID: ins.AppID, InstanceID: ins.ID,
		JobParams:           jobParams,
		InstanceParams:      ins.JobInstanceParams,
		Status:              DomainToWire(ins.Status),
		Result:              ins.Result,
		ExpectedTriggerTime: ms(&ins.TriggerTime),
		ActualTriggerTime:   ms(ins.StartTime),
		FinishedTime:        ms(ins.EndTime),
		TaskTrackerAddress:  ins.WorkerAddress,
		RunningTimes:        int64(ins.RetryIndex + 1),
		GmtCreate:           ins.CreatedAt.UnixMilli(),
		GmtModified:         ins.UpdatedAt.UnixMilli(),
	}
}

// ===== helpers =====

func formInt64(c *gin.Context, key string) int64 {
	v := c.PostForm(key)
	if v == "" {
		v = c.Query(key)
	}
	n, _ := strconv.ParseInt(v, 10, 64)
	return n
}

func formString(c *gin.Context, key string) string {
	v := c.PostForm(key)
	if v == "" {
		v = c.Query(key)
	}
	return v
}

// appIDFrom 取 appId:优先 JSON body 的 appId,回退 header X-POWERJOB-APP-ID,再回退 form appId。
func appIDFrom(c *gin.Context, jsonAppID *int64) int64 {
	if jsonAppID != nil && *jsonAppID > 0 {
		return *jsonAppID
	}
	if h := c.GetHeader("X-POWERJOB-APP-ID"); h != "" {
		if n, err := strconv.ParseInt(h, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return formInt64(c, "appId")
}

// jobBelongToApp 越权防护:job 必须属于 app(对齐 PowerJob checkJobIdValid)。
func (d OpenApiDeps) jobBelongToApp(jobID, appID int64) (*domain.Job, error) {
	if appID <= 0 || jobID <= 0 {
		return nil, errors.New("appId / jobId 非法")
	}
	return d.Jobs.Get(appID, jobID) // JobStore.Get 带 app_id 过滤
}

// instanceBelongToApp 越权防护(尽力而为):PowerJob OpenAPI 的 instance 操作客户端传统上只带
// instanceId(全局唯一)、不带 appId,故此处不强制 appId;仅当请求带了 appId(header/form)时校验
// 实例归属,不带则放行(对齐 PowerJob,生产隔离靠网络隔离)。返回实例供 fetch* 端点直接复用。
func (d OpenApiDeps) instanceBelongToApp(c *gin.Context, instanceID int64) (*domain.Instance, error) {
	ins, err := d.Instances.Get(instanceID)
	if err != nil {
		return nil, errors.New("实例不存在")
	}
	if appID := appIDFrom(c, nil); appID > 0 && ins.AppID != appID {
		return nil, errors.New("实例不属于该 app")
	}
	return ins, nil
}

// ===== App =====

// assertApp POST /openApi/assert(appName[,password]) → ResultDTO<Long>(appId)。对齐 PowerJob assertAppName。
func (d OpenApiDeps) assertApp(c *gin.Context) {
	appName := formString(c, "appName")
	if appName == "" {
		c.JSON(http.StatusOK, ResultFail("appName 不能为空"))
		return
	}
	app, err := d.Apps.GetByName(appName)
	if err != nil {
		c.JSON(http.StatusOK, ResultFail("app(" + appName + ")未注册"))
		return
	}
	c.JSON(http.StatusOK, ResultOK(app.ID))
}

// ===== Job =====

func (d OpenApiDeps) fetchJob(c *gin.Context) {
	job, err := d.jobBelongToApp(formInt64(c, "jobId"), formInt64(c, "appId"))
	if err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	c.JSON(http.StatusOK, ResultOK(jobToDTO(job)))
}

func (d OpenApiDeps) fetchAllJob(c *gin.Context) {
	appID := formInt64(c, "appId")
	if appID <= 0 {
		c.JSON(http.StatusOK, ResultFail("appId 非法"))
		return
	}
	jobs, _, err := d.Jobs.List(appID, 1, 100000)
	if err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	out := make([]JobInfoDTO, 0, len(jobs))
	for i := range jobs {
		out = append(out, jobToDTO(&jobs[i]))
	}
	c.JSON(http.StatusOK, ResultOK(out))
}

func (d OpenApiDeps) queryJob(c *gin.Context) {
	var q JobInfoQuery
	_ = c.ShouldBindJSON(&q)
	db := d.Store.DB.Model(&domain.Job{})
	if appID := appIDFrom(c, nil); appID > 0 { // 尽力而为:客户端带 appId 才过滤(对齐 queryInstance)
		db = db.Where("app_id = ?", appID)
	}
	if q.IDEq != nil {
		db = db.Where("id = ?", *q.IDEq)
	}
	if q.JobNameEq != nil {
		db = db.Where("name = ?", *q.JobNameEq)
	}
	if q.JobNameLike != nil && *q.JobNameLike != "" {
		db = db.Where("name LIKE ?", "%"+*q.JobNameLike+"%")
	}
	if q.TagEq != nil {
		db = db.Where("tag = ?", *q.TagEq)
	}
	var jobs []domain.Job
	if err := db.Order("id DESC").Limit(1000).Find(&jobs).Error; err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	out := make([]JobInfoDTO, 0, len(jobs))
	for i := range jobs {
		out = append(out, jobToDTO(&jobs[i]))
	}
	c.JSON(http.StatusOK, ResultOK(out))
}

func (d OpenApiDeps) saveJob(c *gin.Context) {
	var r SaveJobReq
	if err := c.ShouldBindJSON(&r); err != nil {
		c.JSON(http.StatusOK, ResultFail("参数解析失败: " + err.Error()))
		return
	}
	appID := appIDFrom(c, r.AppID)
	if appID <= 0 {
		c.JSON(http.StatusOK, ResultFail("appId 非法"))
		return
	}
	if r.ID != nil && *r.ID > 0 {
		// 更新前校验 job 归属 app:Jobs.Update 仅在 schedule 字段变化时经 Get 带 app_id 校验,
		// 改非 schedule 字段(name/params/tag/concurrency/retry 等)会直达 JobStore.Update(只按 id)——
		// 此处补齐,防 app A 客户端越权改 app B 的 job。
		if _, err := d.jobBelongToApp(*r.ID, appID); err != nil {
			c.JSON(http.StatusOK, ResultFail(err.Error()))
			return
		}
		// 更新:请求非 nil 字段转 fields(对齐 PowerJob saveJob 全量保存语义的子集)
		fields := map[string]any{}
		if r.JobName != nil {
			fields["name"] = *r.JobName
		}
		if r.JobParams != nil {
			fields["job_params"] = *r.JobParams
		}
		if r.JobDescription != nil {
			fields["description"] = *r.JobDescription
		}
		if r.TimeExpressionType != nil {
			fields["schedule_kind"] = wireToScheduleKind(*r.TimeExpressionType)
		}
		if r.TimeExpression != nil {
			fields["schedule_expr"] = *r.TimeExpression
		}
		if r.Concurrency != nil && *r.Concurrency > 0 {
			fields["max_concurrency"] = *r.Concurrency
		}
		if r.InstanceTimeLimit != nil {
			fields["timeout_sec"] = *r.InstanceTimeLimit / 1000
		}
		if r.InstanceRetryNum != nil {
			fields["retry_count"] = *r.InstanceRetryNum
		}
		if r.Enable != nil {
			fields["enabled"] = *r.Enable
		}
		if r.Tag != nil {
			fields["tag"] = *r.Tag
		}
		if r.StartTime != nil {
			fields["start_time"] = msToTimePtr(r.StartTime)
		}
		if r.EndTime != nil {
			fields["end_time"] = msToTimePtr(r.EndTime)
		}
		if err := d.Jobs.Update(appID, *r.ID, fields); err != nil {
			c.JSON(http.StatusOK, ResultFail(err.Error()))
			return
		}
		c.JSON(http.StatusOK, ResultOK(*r.ID))
		return
	}
	// 创建
	name := strOrDefault(r.JobName)
	if name == "" {
		c.JSON(http.StatusOK, ResultFail("jobName 不能为空"))
		return
	}
	// 表达式与类型必须配对:漏传 timeExpressionType 会落到 api,cron/fix_rate 表达式被静默忽略、
	// 创建出永不自动调度的 job,客户端无错误反馈。
	if r.TimeExpression != nil && *r.TimeExpression != "" && (r.TimeExpressionType == nil || *r.TimeExpressionType <= 0) {
		c.JSON(http.StatusOK, ResultFail("提供 timeExpression 时必须同时指定 timeExpressionType"))
		return
	}
	job := &domain.Job{
		AppID: appID, Name: name, ExecuteType: "http",
		JobParams:      strOrDefault(r.JobParams),
		Description:    strOrDefault(r.JobDescription),
		Tag:            strOrDefault(r.Tag),
		TimeoutSec:     int(int64Val(r.InstanceTimeLimit) / 1000),
		ScheduleKind:   wireToScheduleKind(intVal(r.TimeExpressionType)),
		ScheduleExpr:   strOrDefault(r.TimeExpression),
		StartTime:      msToTimePtr(r.StartTime),
		EndTime:        msToTimePtr(r.EndTime),
		MaxConcurrency: intOrDefault(r.Concurrency, 1),
		RetryCount:     intVal(r.InstanceRetryNum),
		Enabled:        boolOrDefault(r.Enable, true),
	}
	if err := d.Jobs.Create(job); err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	c.JSON(http.StatusOK, ResultOK(job.ID))
}

func (d OpenApiDeps) copyJob(c *gin.Context) {
	src, err := d.jobBelongToApp(formInt64(c, "jobId"), formInt64(c, "appId"))
	if err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	cp := *src
	cp.ID = 0
	cp.Name = src.Name + "-copy"
	cp.NextRunTime = nil
	if err := d.Jobs.Create(&cp); err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	c.JSON(http.StatusOK, ResultOK(cp.ID))
}

func (d OpenApiDeps) exportJob(c *gin.Context) {
	job, err := d.jobBelongToApp(formInt64(c, "jobId"), formInt64(c, "appId"))
	if err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	dto := jobToDTO(job)
	// 对齐 SaveJobInfoRequest 形状(枚举用 PowerJob 名称,便于回灌 saveJob)
	out := gin.H{
		"id":                 dto.ID,
		"jobName":            dto.JobName,
		"appId":              dto.AppID,
		"jobParams":          dto.JobParams,
		"timeExpressionType": dto.TimeExpressionType,
		"timeExpression":     dto.TimeExpression,
		"executeType":        "STANDALONE",
		"processorType":      "JAVA",
		"concurrency":        dto.Concurrency,
		"instanceTimeLimit":  dto.InstanceTimeLimit,
		"instanceRetryNum":   dto.InstanceRetryNum,
		"enable":             dto.Status == 1,
		"tag":                dto.Tag,
	}
	c.JSON(http.StatusOK, ResultOK(out))
}

func (d OpenApiDeps) deleteJob(c *gin.Context) {
	appID := formInt64(c, "appId")
	jobID := formInt64(c, "jobId")
	if _, err := d.jobBelongToApp(jobID, appID); err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	if err := d.Jobs.Delete(appID, jobID); err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	c.JSON(http.StatusOK, ResultOK(nil))
}

func (d OpenApiDeps) setJobEnabled(c *gin.Context, enabled bool) {
	appID := formInt64(c, "appId")
	jobID := formInt64(c, "jobId")
	if _, err := d.jobBelongToApp(jobID, appID); err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	if err := d.Jobs.Update(appID, jobID, map[string]any{"enabled": enabled}); err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	c.JSON(http.StatusOK, ResultOK(nil))
}

func (d OpenApiDeps) disableJob(c *gin.Context) { d.setJobEnabled(c, false) }
func (d OpenApiDeps) enableJob(c *gin.Context)  { d.setJobEnabled(c, true) }

// runJob POST /openApi/runJob(form:appId/jobId/instanceParams/delayMS|delay) → ResultDTO<Long>。
// 兼容客户端 delayMS 与 PowerJob 官方 delay(均毫秒)。
func (d OpenApiDeps) runJob(c *gin.Context) {
	if err := c.Request.ParseForm(); err != nil {
		c.JSON(http.StatusOK, ResultFail("参数解析失败: " + err.Error()))
		return
	}
	appID := formInt64(c, "appId")
	jobID := formInt64(c, "jobId")
	if appID <= 0 || jobID <= 0 {
		c.JSON(http.StatusOK, ResultFail("appId / jobId 非法"))
		return
	}
	delayMS := formInt64(c, "delayMS")
	if delayMS == 0 {
		delayMS = formInt64(c, "delay")
	}
	instanceID, err := d.Jobs.TriggerReturnInstance(appID, jobID, 0, c.PostForm("instanceParams"), delayMS)
	if err != nil {
		c.JSON(http.StatusOK, ResultFail("触发失败: " + err.Error()))
		return
	}
	c.JSON(http.StatusOK, ResultOK(instanceID))
}

// runJob2 POST /openApi/runJob2(JSON RunJobRequest) → PowerResultDTO<Long>。
func (d OpenApiDeps) runJob2(c *gin.Context) {
	var r RunJobReq
	if err := c.ShouldBindJSON(&r); err != nil {
		c.JSON(http.StatusOK, PowerResultFail("参数解析失败: " + err.Error()))
		return
	}
	appID := appIDFrom(c, r.AppID)
	jobID := int64(0)
	if r.JobID != nil {
		jobID = *r.JobID
	}
	if appID <= 0 || jobID <= 0 {
		c.JSON(http.StatusOK, PowerResultFail("appId / jobId 非法"))
		return
	}
	delayMS := int64(0)
	if r.Delay != nil {
		delayMS = *r.Delay
	}
	instanceID, err := d.Jobs.TriggerReturnInstance(appID, jobID, 0, r.InstanceParams, delayMS)
	if err != nil {
		c.JSON(http.StatusOK, PowerResultFail("触发失败: " + err.Error()))
		return
	}
	c.JSON(http.StatusOK, PowerResultOK(instanceID))
}

// ===== Instance =====

func (d OpenApiDeps) fetchInstanceStatus(c *gin.Context) {
	ins, err := d.instanceBelongToApp(c, formInt64(c, "instanceId"))
	if err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	c.JSON(http.StatusOK, ResultOK(DomainToWire(ins.Status)))
}

func (d OpenApiDeps) fetchInstanceInfo(c *gin.Context) {
	ins, err := d.instanceBelongToApp(c, formInt64(c, "instanceId"))
	if err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	jobParams := ""
	if job, err := d.Jobs.Get(ins.AppID, ins.JobID); err == nil {
		jobParams = job.JobParams
	}
	c.JSON(http.StatusOK, ResultOK(instanceToDTO(ins, jobParams)))
}

func (d OpenApiDeps) stopInstance(c *gin.Context) {
	ins, err := d.instanceBelongToApp(c, formInt64(c, "instanceId"))
	if err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	if err := d.Instances.Stop(ins.ID); err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	c.JSON(http.StatusOK, ResultOK(nil))
}

func (d OpenApiDeps) cancelInstance(c *gin.Context) {
	ins, err := d.instanceBelongToApp(c, formInt64(c, "instanceId"))
	if err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	if err := d.Instances.Cancel(ins.ID); err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	c.JSON(http.StatusOK, ResultOK(nil))
}

func (d OpenApiDeps) retryInstance(c *gin.Context) {
	ins, err := d.instanceBelongToApp(c, formInt64(c, "instanceId"))
	if err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	if err := d.Instances.Retry(ins.ID); err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	c.JSON(http.StatusOK, ResultOK(nil))
}

func (d OpenApiDeps) queryInstance(c *gin.Context) {
	var q InstancePageQuery
	_ = c.ShouldBindJSON(&q)
	if q.PageSize <= 0 {
		q.PageSize = 10
	}
	if q.Index < 0 {
		q.Index = 0
	}
	// 单实例精确查走 Get
	if q.InstanceIDEq != nil {
		ins, err := d.Instances.Get(*q.InstanceIDEq)
		if err != nil {
			c.JSON(http.StatusOK, ResultOK(PageResult{Index: q.Index, PageSize: q.PageSize, Data: []InstanceInfoDTO{}}))
			return
		}
		jobParams := ""
		if job, err := d.Jobs.Get(ins.AppID, ins.JobID); err == nil {
			jobParams = job.JobParams
		}
		c.JSON(http.StatusOK, ResultOK(PageResult{Index: q.Index, PageSize: q.PageSize, TotalItems: 1, TotalPages: 1, Data: []InstanceInfoDTO{instanceToDTO(ins, jobParams)}}))
		return
	}
	// 分页:PowerJob index 0-based → task-schedule page 1-based
	f := repository.InstanceFilter{Page: q.Index + 1, Size: q.PageSize}
	if q.JobIDEq != nil {
		f.JobID = *q.JobIDEq
	}
	if appID := appIDFrom(c, nil); appID > 0 {
		f.AppID = appID
	}
	if len(q.StatusIn) > 0 {
		// PowerJob statusIn 是列表(status IN […]),逐个翻译——原实现只取 [0] 会静默丢失多状态查询。
		statuses := make([]string, 0, len(q.StatusIn))
		for _, ws := range q.StatusIn {
			if ds, ok := WireToDomain(ws); ok {
				statuses = append(statuses, ds)
			}
		}
		f.StatusIn = statuses
	}
	list, total, err := d.Store.Instance.List(f)
	if err != nil {
		c.JSON(http.StatusOK, ResultFail(err.Error()))
		return
	}
	// 批量预加载 jobParams(消除 N+1),仿 own listInstances 的 ListByIDs 模式
	jobSet := make(map[int64]struct{}, len(list))
	for i := range list {
		jobSet[list[i].JobID] = struct{}{}
	}
	jobIDs := make([]int64, 0, len(jobSet))
	for id := range jobSet {
		jobIDs = append(jobIDs, id)
	}
	params := make(map[int64]string, len(jobIDs))
	if len(jobIDs) > 0 {
		if jobs, err := d.Store.Job.ListByIDs(jobIDs); err == nil {
			for i := range jobs {
				params[jobs[i].ID] = jobs[i].JobParams
			}
		}
	}
	out := make([]InstanceInfoDTO, 0, len(list))
	for i := range list {
		out = append(out, instanceToDTO(&list[i], params[list[i].JobID]))
	}
	totalPages := 0
	if total > 0 {
		totalPages = int((total + int64(q.PageSize) - 1) / int64(q.PageSize))
	}
	c.JSON(http.StatusOK, ResultOK(PageResult{Index: q.Index, PageSize: q.PageSize, TotalPages: totalPages, TotalItems: total, Data: out}))
}

// ===== 指针字段取值兜底 =====

func strOrDefault(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
func intVal(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}
func int64Val(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}
func intOrDefault(p *int, def int) int {
	if p == nil || *p <= 0 {
		return def
	}
	return *p
}
func boolOrDefault(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
}
