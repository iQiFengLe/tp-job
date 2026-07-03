package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"task-schedule/internal/auth"
	"task-schedule/internal/config"
	"task-schedule/internal/dispatch"
	"task-schedule/internal/dservice"
	"task-schedule/internal/instancelog"
	"task-schedule/internal/logger"
	"task-schedule/internal/protocol/own"
	"task-schedule/internal/protocol/powerjob"
	"task-schedule/internal/protocol/worker"
	"task-schedule/internal/repository"
	"task-schedule/internal/workerreg"
)

// embeddedWeb 内置前端管理台产物(web/dist)。编译期必须存在该目录(先 npm run build)。
// 运行时优先读磁盘 web/dist(便于热替换),缺失时回退内置版本,实现单二进制部署。
//
//go:embed all:web/dist
var embeddedWeb embed.FS

// resolveWebFS 返回前端静态资源文件系统:运行时 web/dist 目录优先(开发可热替换),
// 不存在时回退到编译期 embed 的内置产物(单二进制部署)。两者都不可用时返回 nil。
func resolveWebFS() fs.FS {
	if _, err := os.Stat(filepath.Join("web", "dist", "index.html")); err == nil {
		return os.DirFS(filepath.Join("web", "dist"))
	}
	sub, err := fs.Sub(embeddedWeb, "web/dist")
	if err != nil {
		return nil
	}
	return sub
}

func main() {
	cfgPath := flag.String("config", "config.yaml", "配置文件路径")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fail(err)
	}

	log, err := logger.Init(cfg.Log)
	if err != nil {
		fail(err)
	}

	// 鉴权安全校验:release 模式拒绝默认/空管理员密码启动(防管理接口裸奔上线)。
	if err := cfg.Auth.Validate(cfg.Server.Mode == "release"); err != nil {
		fail(err)
	}
	// /server/*、/worker/* 协议端点无鉴权(对齐 PowerJob Server 设计):可被任意调用——
	// 注册任意 worker 地址构成 SSRF、伪造实例状态。生产部署必须通过网络隔离保护,切勿直接暴露公网。
	log.Warn("/server/*、/worker/* 协议端点无鉴权(靠 appName + 网络隔离保护),生产部署切勿直接暴露公网")

	log.Info("配置加载完成",
		"driver", cfg.Database.Driver,
		"port", cfg.Server.Port,
		"worker_timeout_s", cfg.Worker.TimeoutSeconds,
		"admins", len(cfg.Auth.Admins))

	// 数据库 + 核心组件
	st, err := repository.New(cfg.Database)
	if err != nil {
		fail(err)
	}

	reg := workerreg.New(time.Duration(cfg.Worker.TimeoutSeconds)*time.Second, log)
	if len(cfg.Worker.AllowedCIDRs) > 0 {
		pol, err := workerreg.NewAddressPolicy(cfg.Worker.AllowedCIDRs)
		if err != nil {
			fail(err)
		}
		reg.SetPolicy(pol)
		log.Info("worker 地址白名单已启用", "cidrs", cfg.Worker.AllowedCIDRs)
	}
	il := instancelog.New(cfg.Log.Dir, time.Duration(cfg.Log.InstanceRetentionDays)*24*time.Hour)
	exec := dispatch.New(reg, 10*time.Second) // 派发 POST 超时(远小于实例执行超时)
	sch := dispatch.NewScheduler(st, exec, il, time.Duration(cfg.Scheduler.IntervalMs)*time.Millisecond, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动清理:把重启前未终结(waiting_receive/running)的实例做失败转移(UpdateResult(failed)
	// + scheduleRetry)。有重试余力的实例设 next_retry_time 由 RetryPump 接管重派——重启不丢,
	// 取代旧 MarkStaleActiveAsFailed 的 bulk 标记(旧版不衔接重试,在飞实例被静默放弃)。
	if err := sch.RecoverStaleActive(); err != nil {
		log.Error("清理僵尸实例失败", "err", err)
	}

	// 恢复重启前排队的实例(任意 trigger_type):优先队列是纯内存,重启即丢;queued 实例不被
	// reaper/RetryPump 捞,不恢复会永久滞留(违背 SubmitManual 落库即不丢的承诺)。
	if err := sch.RecoverQueued(); err != nil {
		log.Error("恢复 queued 实例失败", "err", err)
	}

	// 鉴权:会话 store + 登录服务(管理员配置 + app 表)
	authStore := auth.NewStore(time.Duration(cfg.Auth.Session.TTLSeconds) * time.Second)
	admins := make([]auth.AdminCredential, 0, len(cfg.Auth.Admins))
	for _, a := range cfg.Auth.Admins {
		admins = append(admins, auth.AdminCredential{Username: a.Username, PasswordHash: a.Password})
	}
	appSvc := dservice.NewAppService(st)
	loginSvc := auth.NewLoginService(admins, appSvc, authStore)

	// 业务服务
	jobSvc := dservice.NewJobService(st, sch)
	insSvc := dservice.NewInstanceService(st, sch, il)

	// 后台循环:定时调度 + 手动派发 + 失败转移 reaper + DB 重试 pump(sch.Start,纳入 sch.wg 跟踪)
	// + worker 清理 + 会话清理 + 实例日志清理(纳入 bg 跟踪)。优雅关闭时 cancel ctx 后统一等待,
	// 避免关闭期写库与 sqlDB.Close 竞态。
	sch.Start(ctx, reg)
	var bg sync.WaitGroup
	runBG := func(fn func()) {
		bg.Add(1)
		go func() { defer bg.Done(); fn() }()
	}
	runBG(func() { reg.Run(ctx) })
	runBG(func() { authStore.Run(ctx) })
	runBG(func() { il.Run(ctx) })

	// HTTP 服务
	webFS := resolveWebFS()
	handler := buildRouter(routerDeps{
		cfg: cfg, st: st,
		appSvc: appSvc, jobSvc: jobSvc, insSvc: insSvc,
		reg: reg, il: il, authStore: authStore, loginSvc: loginSvc,
		webFS: webFS,
	})
	httpSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second, // 慢首部(Slowloris)前置拦截
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		log.Info("HTTP 服务启动", "addr", httpSrv.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP 服务异常退出", "err", err)
			cancel()
		}
	}()

	// 优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Info("收到退出信号", "sig", sig.String())

	cancel() // 停止调度器 / reaper / 清理循环

	// 先停 HTTP(拒新请求、处理完在飞 handler),再等后台 goroutine 退出,最后关 DB——避免后台/reaper
	// 仍在写库时 Close DB 造成竞态。sch.Wait 等调度循环 + 派发子协程;bg 等注册表/会话/日志清理循环。
	// 各给 10s 超时,防某轮 runOnce 卡住拖死关闭进程(最坏丢极少量在飞写库,优于永久挂起)。
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		log.Error("HTTP 关闭失败", "err", err)
	}
	waitWithTimeout := func(done <-chan struct{}, name string) {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			log.Warn(name+" 10s 内未全部退出,继续关闭", "wait", name)
		}
	}
	schDone, bgDone := make(chan struct{}), make(chan struct{})
	go func() { sch.Wait(); close(schDone) }()
	go func() { bg.Wait(); close(bgDone) }()
	waitWithTimeout(schDone, "调度协程")
	waitWithTimeout(bgDone, "后台清理协程")
	if sqlDB, err := st.DB.DB(); err == nil {
		_ = sqlDB.Close()
	}
	log.Info("已安全退出")
}

// routerDeps buildRouter 的装配依赖。
type routerDeps struct {
	cfg       *config.Config
	st        *repository.Store
	appSvc    *dservice.AppService
	jobSvc    *dservice.JobService
	insSvc    *dservice.InstanceService
	reg       *workerreg.Registry
	il        *instancelog.Logger
	authStore *auth.Store
	loginSvc  *auth.LoginService
	webFS     fs.FS
}

// buildRouter 装配全部 HTTP 路由:
//
//	/health        探活
//	/api           管理端(/auth/login 公开;其余受 SessionAuth + 权限矩阵保护)
//	/worker        简化 http worker 协议(无鉴权,网络隔离)
//	/server        PowerJob 协议(无鉴权,网络隔离)
//	静态资源 + SPA 回退
func buildRouter(d routerDeps) *gin.Engine {
	gin.SetMode(d.cfg.Server.Mode)
	r := gin.New()
	// 不信任任何代理:gin 默认信任 0.0.0.0/0,会从客户端可伪造的 X-Forwarded-For 取 ClientIP,
	// 使登录限流(以 ClientIP 为 key)被换头绕过。nil 后 ClientIP 退回 TCP 源地址(RemoteAddr),
	// 不可伪造。若日后部署在受信反代后且需按真实客户端 IP 限流,改为反代网段列表。
	r.SetTrustedProxies(nil)
	r.Use(gin.Recovery(), bodyLimit(d.cfg.Server.MaxRequestBodyMB))

	r.GET("/health", func(c *gin.Context) { health(c, d.st) })

	// /api:登录公开;me/logout 与资源路由各自前置 SessionAuth(在 own 内部按矩阵挂)。
	api := r.Group("/api")
	api.POST("/auth/login", own.LoginRateLimit(d.cfg.Auth.Login.MaxAttemptsPerMin), own.LoginHandler(d.loginSvc))
	own.RegisterAuth(api, d.authStore)
	own.Register(api, own.Deps{
		Apps: d.appSvc, Jobs: d.jobSvc, Instances: d.insSvc,
		Store: d.st, Auth: d.authStore, Reg: d.reg,
	})

	// /worker:简化 http worker 协议(心跳/回报状态/回报日志),无鉴权。
	worker.Register(r.Group("/worker"), worker.Deps{
		Apps: d.appSvc, Instances: d.insSvc, Reg: d.reg, IL: d.il, Store: d.st,
	})

	// /server:PowerJob 协议(assert/acquire/heartbeat/reportStatus/reportLog/queryJob),无鉴权。
	powerjob.RegisterServer(r.Group("/server"), powerjob.Deps{
		Apps: d.appSvc, Instances: d.insSvc, Reg: d.reg, IL: d.il, Store: d.st,
		ServerAddr: d.cfg.PowerJob.ServerAddress,
	})

	// /openApi:PowerJob OpenAPI 兼容(App/Job/Instance 区,对齐 OpenAPIController),无鉴权
	// (对齐 PowerJob OpenAPI 默认信任 + 网络隔离)。让原对接 PowerJob 的业务客户端零改动接入。
	powerjob.RegisterOpenApi(r.Group("/openApi"), powerjob.OpenApiDeps{
		Jobs: d.jobSvc, Instances: d.insSvc, Apps: d.appSvc, Store: d.st,
	})

	mountWeb(r, d.webFS)
	return r
}

// health 探活 DB:Ping 失败返回 503 degraded。不向匿名访问者泄露内网信息(仅暴露 status)。
func health(c *gin.Context, st *repository.Store) {
	status, httpStatus := "ok", 200
	if sqlDB, err := st.DB.DB(); err != nil {
		status, httpStatus = "degraded", 503
	} else if err := sqlDB.Ping(); err != nil {
		status, httpStatus = "degraded", 503
	}
	c.JSON(httpStatus, gin.H{"code": 0, "msg": "ok", "data": gin.H{"status": status}})
}

// bodyLimit 限制请求体大小,防止超大 JSON 打爆内存。
func bodyLimit(maxMB int) gin.HandlerFunc {
	maxBytes := int64(maxMB) << 20
	return func(c *gin.Context) {
		if maxBytes > 0 && c.Request != nil && c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		}
		c.Next()
	}
}

// mountWeb 托管前端管理台:静态资源直出,未命中的非接口路径回退 index.html(SPA)。
func mountWeb(r *gin.Engine, webFS fs.FS) {
	if webFS == nil {
		return
	}
	fileServer := http.FileServer(http.FS(webFS))
	r.NoRoute(func(c *gin.Context) {
		path := c.Request.URL.Path
		if path == "/api" || path == "/server" || path == "/worker" || path == "/openApi" ||
			strings.HasPrefix(path, "/api/") || strings.HasPrefix(path, "/server/") || strings.HasPrefix(path, "/worker/") || strings.HasPrefix(path, "/openApi/") {
			c.JSON(http.StatusNotFound, gin.H{"code": http.StatusNotFound, "msg": "接口不存在"})
			return
		}
		// 命中静态文件(非目录)则直出
		if clean := strings.TrimPrefix(path, "/"); clean != "" {
			if f, err := webFS.Open(clean); err == nil {
				stat, _ := f.Stat()
				_ = f.Close()
				if stat != nil && !stat.IsDir() {
					fileServer.ServeHTTP(c.Writer, c.Request)
					return
				}
			}
		}
		// SPA 回退:其余路径返回 index.html
		c.Request.URL.Path = "/"
		fileServer.ServeHTTP(c.Writer, c.Request)
	})
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "启动失败:", err)
	os.Exit(1)
}
