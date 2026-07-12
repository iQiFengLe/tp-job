package worker

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"dida/internal/dservice"
	"dida/internal/instancelog"
	"dida/internal/repository"
	"dida/internal/workerreg"
)

// Deps worker 协议 handler 的依赖。
type Deps struct {
	Apps      *dservice.AppService
	Instances *dservice.InstanceService
	Reg       *workerreg.Registry
	IL        *instancelog.Logger
	Store     *repository.Store
}

// Register 把 /worker/* 路由挂到给定 group(无鉴权,靠网络隔离)。
func Register(r *gin.RouterGroup, d Deps) {
	r.POST("/heartbeat", d.heartbeat)
	r.POST("/instances/:iid/status", d.reportStatus)
	r.POST("/instances/:iid/logs", d.reportLog)
}

func (d Deps) heartbeat(c *gin.Context) {
	var req HeartbeatReq
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "参数错误: "+err.Error())
		return
	}
	if req.AppName == "" || req.WorkerAddress == "" {
		fail(c, http.StatusBadRequest, "appName / workerAddress 不能为空")
		return
	}
	app, err := d.Apps.GetByName(req.AppName)
	if err != nil {
		fail(c, http.StatusNotFound, "app("+req.AppName+")未注册")
		return
	}
	proto := req.Protocol
	if proto == "" {
		proto = workerreg.ProtocolHTTP
	}
	d.Reg.Heartbeat(workerreg.WorkerInfo{
		AppID:           app.ID,
		WorkerAddress:   req.WorkerAddress,
		Metrics:         req.SystemMetrics,
		Tags:            req.Tags,
		AcceptNotTagJob: req.AcceptNotTagJob,
		Protocol:        proto,
	})
	ok(c, gin.H{"ok": true})
}

func (d Deps) reportStatus(c *gin.Context) {
	iid := paramInt64(c, "iid")
	var req ReportStatusReq
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "参数错误: "+err.Error())
		return
	}
	// 归属校验:上报 worker 须与实例绑定的 worker_address 一致,防伪造实例 id 篡改他人实例状态
	// (iid 自增可枚举;/worker/* 无鉴权,根本防护靠网络隔离,此处为纵深防御)。
	ins, err := d.Instances.Get(iid)
	if err != nil {
		fail(c, http.StatusNotFound, err.Error())
		return
	}
	if ins.WorkerAddress == "" || req.WorkerAddress != ins.WorkerAddress {
		fail(c, http.StatusForbidden, "worker 与实例绑定不一致,拒绝状态上报")
		return
	}
	if err := d.Instances.ReportStatus(iid, req.Status, req.Result); err != nil {
		fail(c, http.StatusBadRequest, err.Error())
		return
	}
	ok(c, gin.H{"ok": true})
}

func (d Deps) reportLog(c *gin.Context) {
	iid := paramInt64(c, "iid")
	var req ReportLogReq
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "参数错误: "+err.Error())
		return
	}
	ins, err := d.Instances.Get(iid)
	if err != nil {
		fail(c, http.StatusNotFound, err.Error())
		return
	}
	level := req.Level
	if level == "" {
		level = "info"
	}
	d.IL.Append(ins.AppID, ins.ID, ins.RootInstanceID, instancelog.LogEntry{
		Time:    msToTime(req.Time),
		Kind:    "WORKER",
		Level:   level,
		Message: req.Message,
	})
	c.Status(http.StatusOK) // 对齐 PowerJob:日志上报始终 200,避免 worker 反压
}

// ===== 小工具(协议层内联,避免反向依赖旧 api 包) =====

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

func msToTime(ms int64) time.Time {
	if ms <= 0 {
		return time.Now()
	}
	return time.UnixMilli(ms)
}
