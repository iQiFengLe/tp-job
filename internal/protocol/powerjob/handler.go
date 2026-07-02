package powerjob

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"task-schedule/internal/domain"
	"task-schedule/internal/dservice"
	"task-schedule/internal/instancelog"
	"task-schedule/internal/repository"
	"task-schedule/internal/workerreg"
)

// Deps powerjob 协议依赖。
type Deps struct {
	Apps       *dservice.AppService
	Instances  *dservice.InstanceService
	Reg        *workerreg.Registry
	IL         *instancelog.Logger
	Store      *repository.Store
	ServerAddr string // /acquire 返回值(Worker 可达的 host:port)
}

// RegisterServer 挂载 /server/* 路由(无鉴权,靠网络隔离)。
func RegisterServer(r *gin.RouterGroup, d Deps) {
	r.GET("/assert", d.assertApp)
	r.GET("/acquire", d.acquire)
	r.POST("/workerHeartbeat", d.workerHeartbeat)
	r.POST("/reportInstanceStatus", d.reportInstanceStatus)
	r.POST("/reportLog", d.reportLog)
	r.POST("/queryJobCluster", d.queryJobCluster)
}

// assertApp GET /server/assert?appName= → ResultDTO{data: appId(Long)}
func (d Deps) assertApp(c *gin.Context) {
	appName := c.Query("appName")
	if appName == "" {
		c.JSON(http.StatusOK, ResultFail("app name 不能为空"))
		return
	}
	app, err := d.Apps.GetByName(appName)
	if err != nil {
		c.JSON(http.StatusOK, ResultFail("app(" + appName + ") is not registered"))
		return
	}
	c.JSON(http.StatusOK, ResultOK(app.ID))
}

// acquire GET /server/acquire → ResultDTO{data: server_address}
func (d Deps) acquire(c *gin.Context) {
	c.JSON(http.StatusOK, ResultOK(d.ServerAddr))
}

// workerHeartbeat POST /server/workerHeartbeat(tell:无响应体)。
// 兼容 PowerJob 单 tag 与通用 tags;protocol 固定 powerjob。
func (d Deps) workerHeartbeat(c *gin.Context) {
	var req HeartbeatReq
	_ = c.ShouldBindJSON(&req)
	if req.AppName == "" || req.WorkerAddress == "" {
		c.Status(http.StatusOK)
		return
	}
	app, err := d.Apps.GetByName(req.AppName)
	if err != nil {
		c.Status(http.StatusOK) // 容错:未知 app 静默,避免 worker 反压
		return
	}
	if !d.Reg.AllowedAddress(req.WorkerAddress) {
		c.Status(http.StatusOK) // 容错:非白名单静默不注册(对齐 PowerJob 不反压)
		return
	}
	tags := req.Tags
	if len(tags) == 0 && req.Tag != "" {
		tags = []string{req.Tag}
	}
	d.Reg.Heartbeat(workerreg.WorkerInfo{
		AppID:           app.ID,
		WorkerAddress:   req.WorkerAddress,
		Metrics:         req.SystemMetrics,
		Tags:            tags,
		AcceptNotTagJob: req.AcceptNotTagJob,
		Protocol:        workerreg.ProtocolPowerJob,
	})
	c.Status(http.StatusOK)
}

// reportInstanceStatus POST /server/reportInstanceStatus → AskResponse。
// 数字状态码翻译为领域 string;终态守护 + ReleaseInFlight 由 InstanceService 处理。
func (d Deps) reportInstanceStatus(c *gin.Context) {
	var req ReportInstanceStatusReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusOK, AskFailed("参数错误"))
		return
	}
	if !IsValidWireReport(req.InstanceStatus) {
		c.JSON(http.StatusOK, AskSucceedNil()) // 非法码静默忽略(对齐 PowerJob 行为)
		return
	}
	status, _ := WireToDomain(req.InstanceStatus)
	_ = d.Instances.ReportStatus(req.InstanceID, status, req.Result)
	c.JSON(http.StatusOK, AskSucceedNil())
}

// reportLog POST /server/reportLog:批量落库到实例日志文件。始终 200,避免 worker 反压。
func (d Deps) reportLog(c *gin.Context) {
	var req LogReportReq
	_ = c.ShouldBindJSON(&req)
	cache := make(map[int64]*domain.Instance)
	for _, lc := range req.InstanceLogContents {
		ins, ok := cache[lc.InstanceID]
		if !ok {
			got, err := d.Instances.Get(lc.InstanceID)
			ins = got
			if err != nil {
				ins = nil
			}
			cache[lc.InstanceID] = ins
		}
		if ins == nil {
			continue
		}
		d.IL.Append(ins.AppID, ins.ID, ins.RootInstanceID, instancelog.LogEntry{
			Time:    msToTime(lc.LogTime),
			Kind:    "WORKER",
			Level:   levelFromInt(lc.LogLevel),
			Message: lc.LogContent,
		})
	}
	c.Status(http.StatusOK)
}

// queryJobCluster POST /server/queryJobCluster → AskResponse.data = base64([]workerAddress)。
func (d Deps) queryJobCluster(c *gin.Context) {
	var req QueryClusterReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusOK, AskFailed("参数错误"))
		return
	}
	app, err := d.Store.App.Get(req.AppID)
	if err != nil {
		c.JSON(http.StatusOK, AskFailed("app not found"))
		return
	}
	online := d.Reg.Online(app.ID)
	addrs := make([]string, 0, len(online))
	for _, w := range online {
		addrs = append(addrs, w.WorkerAddress)
	}
	c.JSON(http.StatusOK, AskSucceed(addrs))
}

// ===== helpers =====

// levelFromInt PowerJob LogLevel(int)→可读:1=debug 2=info 3=warn 4=error,其余 info。
func levelFromInt(l int) string {
	switch l {
	case 1:
		return "debug"
	case 3:
		return "warn"
	case 4:
		return "error"
	default:
		return "info"
	}
}

func msToTime(ms int64) time.Time {
	if ms <= 0 {
		return time.Now()
	}
	return time.UnixMilli(ms)
}
