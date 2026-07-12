package powerjob

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"tp-job/internal/domain"
	"tp-job/internal/dservice"
	"tp-job/internal/instancelog"
	"tp-job/internal/repository"
	"tp-job/internal/workerreg"
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
		c.JSON(http.StatusOK, ResultFail("app("+appName+") is not registered"))
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
	if int64(req.InstanceID) <= 0 {
		c.JSON(http.StatusOK, AskSucceedNil()) // 脏 id(FlexInt64 解析失败),对齐 reportLog 跳过
		return
	}
	status, _ := WireToDomain(req.InstanceStatus)
	_ = d.Instances.ReportStatus(int64(req.InstanceID), status, req.Result)
	c.JSON(http.StatusOK, AskSucceedNil())
}

// reportLog POST /server/reportLog:批量落库到实例日志文件。始终 200,避免 worker 反压。
func (d Deps) reportLog(c *gin.Context) {
	var req LogReportReq
	_ = c.ShouldBindJSON(&req)
	cache := make(map[int64]*domain.Instance)
	for _, lc := range req.InstanceLogContents {
		iid := int64(lc.InstanceID)
		if iid <= 0 {
			// 主键自增从 1 起,id<=0 必为 FlexInt64 解析失败的脏数据(空/null/缺字段)。
			// 跳过且不写入 cache——否则 cache[0]=nil 会污染整个批次,让同批所有坏 id 日志被静默吞掉。
			continue
		}
		ins, ok := cache[iid]
		if !ok {
			got, err := d.Instances.Get(iid)
			ins = got
			if err != nil {
				ins = nil
			}
			cache[iid] = ins
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
	app, err := d.Store.App.Get(int64(req.AppID))
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
