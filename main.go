package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	_ "net/http/pprof" // 注册 /debug/pprof 到 DefaultServeMux(独立 pprof server 用,主服务卡死时仍可抓栈)
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"dida/internal/auth"
	"dida/internal/config"
	"dida/internal/dispatch"
	"dida/internal/dservice"
	"dida/internal/instancelog"
	"dida/internal/logger"
	"dida/internal/protocol/own"
	"dida/internal/protocol/powerjob"
	"dida/internal/protocol/worker"
	"dida/internal/repository"
	"dida/internal/workerreg"
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
	// 常用启动参数:仅显式指定时覆盖,优先级 flag > env > yaml > 内置默认。
	// server.mode / auth.login 等安全相关项刻意不开放 flag(对齐"env 不可覆盖"的防降级设计)。
	port := flag.Int("port", 0, "server.port 监听端口(默认 8080)")
	dbDriver := flag.String("db-driver", "", "database.driver: sqlite | mysql")
	sqlitePath := flag.String("sqlite-path", "", "database.sqlite.path")
	mysqlDSN := flag.String("mysql-dsn", "", "database.mysql.dsn(⚠ 会进命令行历史/进程列表,敏感数据建议用 env 或 yaml)")
	logLevel := flag.String("log-level", "", "log.level: debug | info | warn | error")
	logDir := flag.String("log-dir", "", "log.dir(主日志 + 实例日志根目录)")
	powerjobServer := flag.String("powerjob-server", "", "powerjob.server_address(/server/acquire 返回值,PowerJob worker 可达地址)")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fail(err)
	}
	// 显式指定的 flag 覆盖配置(优先级高于 env/yaml)。flag.Visit 只遍历显式设置的 flag,
	// 故未指定的 flag 不影响 yaml/env 已解析的值。
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "port":
			cfg.Server.Port = *port
		case "db-driver":
			cfg.Database.Driver = *dbDriver
		case "sqlite-path":
			cfg.Database.SQLite.Path = *sqlitePath
		case "mysql-dsn":
			cfg.Database.MySQL.DSN = *mysqlDSN
		case "log-level":
			cfg.Log.Level = *logLevel
		case "log-dir":
			cfg.Log.Dir = *logDir
		case "powerjob-server":
			cfg.PowerJob.ServerAddress = *powerjobServer
		}
	})

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
		"worker_timeout_s", cfg.Worker.TimeoutSeconds)

	// 数据库 + 核心组件
	st, err := repository.New(cfg.Database)
	if err != nil {
		fail(err)
	}

	reg := workerreg.New(time.Duration(cfg.Worker.TimeoutSeconds)*time.Second, log)
	il := instancelog.New(cfg.Log.Dir, time.Duration(cfg.Log.InstanceRetentionDays)*24*time.Hour)
	exec := dispatch.New(reg, 10*time.Second) // 派发 POST 超时(远小于实例执行超时)
	cbBuilder := dispatch.NewCallbackBuilder(cfg.Scheduler.Callback.Enabled)
	sch := dispatch.NewScheduler(st, exec, il, time.Duration(cfg.Scheduler.IntervalMs)*time.Millisecond, log, cbBuilder)
	// reaper 启动宽限:避免服务重启时 workerreg 尚空、worker 未及重新心跳,被 reaper 误判失联而批量
	// 失败转移"重启前在飞"的实例(导致重复执行)。默认 30s(cfg.Worker.WarmupSeconds)。
	sch.SetReaperWarmup(time.Duration(cfg.Worker.WarmupSeconds) * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动清理:把重启前未终结(waiting_receive/running)且已超 worker 心跳超时(grace)的实例做失败转移
	// (UpdateResult(failed) + scheduleRetry)。近期活跃实例交 reaper 按真实失联判定——避免重启即批量
	// 失败转移仍在正常执行的长任务(worker 迟到 success 被终态守护拒绝 → 重复执行)。
	if err := sch.RecoverStaleActive(time.Duration(cfg.Worker.TimeoutSeconds) * time.Second); err != nil {
		log.Error("清理僵尸实例失败", "err", err)
	}

	// 恢复重启前排队的实例(任意 trigger_type):优先队列是纯内存,重启即丢;queued 实例不被
	// reaper/RetryPump 捞,不恢复会永久滞留(违背 SubmitManual 落库即不丢的承诺)。
	if err := sch.RecoverQueued(); err != nil {
		log.Error("恢复 queued 实例失败", "err", err)
	}

	// 鉴权:会话 store + 登录服务(管理员 admin_user 表 + app 表)。
	// 管理员账户已迁出 config.yaml/env,首次启动由 SeedDefault 在 admin_user 表种 admin/admin123。
	authStore := auth.NewStore(time.Duration(cfg.Auth.Session.TTLSeconds) * time.Second)
	adminUserSvc := dservice.NewAdminUserService(st)
	if err := adminUserSvc.SeedDefault(); err != nil {
		fail(err)
	}
	appSvc := dservice.NewAppService(st)
	loginSvc := auth.NewLoginService(adminUserSvc, appSvc, authStore)

	// 业务服务
	jobSvc := dservice.NewJobService(st, sch)
	insSvc := dservice.NewInstanceService(st, sch, il, cbBuilder)

	// 后台循环:定时调度 + 手动派发 + 失败转移 reaper + DB 重试 pump(sch.Start,纳入 sch.wg 跟踪)
	// + worker 清理 + 会话清理 + 实例日志清理(纳入 bg 跟踪)。优雅关闭时 cancel ctx 后统一等待,
	// 避免关闭期写库与 sqlDB.Close 竞态。
	sch.Start(ctx, reg)

	// 实例状态变更回调 pump:扫描 pending 回调,POST 通知对端,至少一次。
	// callback URL 可信性靠部署侧网络隔离(同 worker);Transport 仅设连接超时,禁重定向防 302 诱导。
	var cbPump *dispatch.CallbackPump
	if cfg.Scheduler.Callback.Enabled {
		cbTransport := dispatch.NewDialTransport(time.Duration(cfg.Scheduler.Callback.TimeoutSec) * time.Second)
		cbPump = dispatch.NewCallbackPump(st, &http.Client{
			Timeout:   time.Duration(cfg.Scheduler.Callback.TimeoutSec) * time.Second,
			Transport: cbTransport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}, time.Duration(cfg.Scheduler.IntervalMs)*time.Millisecond, cfg.Scheduler.Callback, log)
		cbPump.Start(ctx)
	}
	// PowerJob 同步客户端:作为 OpenAPI 客户端拉取外部 PowerJob server 的任务定义。
	// ServerAddress 由 admin 显式填写(import-powerjob 仅 admin 可调 + validateAddr 限 scheme)。
	pjClient := powerjob.NewClient(&http.Client{
		Timeout:   30 * time.Second,
		Transport: dispatch.NewDialTransport(15 * time.Second),
	})
	var bg sync.WaitGroup
	// runBG 启动带 panic 自愈的后台循环:fn panic 时 recover+log+1s 退避后重启(fn 正常返回则退出)。
	// 防止 workerreg/auth/instancelog 任一循环因单点 panic 静默停止。
	runBG := func(name string, fn func()) {
		bg.Add(1)
		go func() {
			defer bg.Done()
			for {
				ok := func() (ok bool) {
					defer func() {
						if r := recover(); r != nil {
							log.Error("后台循环 panic,1s 后重启", "name", name, "panic", r)
							time.Sleep(time.Second)
						} else {
							ok = true
						}
					}()
					fn()
					return
				}()
				if ok {
					return
				}
			}
		}()
	}
	runBG("workerreg", func() { reg.Run(ctx) })
	runBG("auth", func() { authStore.Run(ctx) })
	runBG("instancelog", func() { il.Run(ctx) })

	// HTTP 服务
	webFS := resolveWebFS()
	handler := buildRouter(routerDeps{
		cfg: cfg, st: st,
		appSvc: appSvc, adminUserSvc: adminUserSvc, jobSvc: jobSvc, insSvc: insSvc,
		reg: reg, il: il, authStore: authStore, loginSvc: loginSvc,
		pjClient: pjClient,
		webFS:    webFS,
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

	// pprof 诊断端点(独立 listener + DefaultServeMux,不受主 gin server 阻塞影响):
	// 主服务卡死时仍可 curl <listen>/debug/pprof/goroutine?debug=2 抓全栈定位。
	// 由 cfg.Pprof 控制:默认关闭;启用时按 listen 绑定(127.0.0.1 仅本机 / 0.0.0.0 可远程)。
	// ⚠ pprof 无鉴权,0.0.0.0 仅在可信网络启用(可读 goroutine 栈/堆 profile)。
	if cfg.Pprof.Enabled {
		go func() {
			log.Info("pprof 诊断端点启动", "listen", cfg.Pprof.Listen)
			if err := http.ListenAndServe(cfg.Pprof.Listen, nil); err != nil {
				log.Error("pprof 服务退出", "err", err)
			}
		}()
	}

	// 优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Info("收到退出信号", "sig", sig.String())

	cancel() // 停止调度器 / reaper / 清理循环

	// 先停 HTTP(拒新请求、处理完在飞 handler),再等后台 goroutine 退出,最后关 DB——避免后台/reaper
	// 仍在写库时 Close DB 造成竞态。sch.Wait 等调度循环 + 派发子协程;bg 等注册表/会话/日志清理循环。
	// 关闭总预算 30s:须 > 派发 POST 超时(10s)+ callback POST 超时(默认 10s)+ DB 提交余量,
	// 确保 in-flight 派发/回调能在关闭前完成或超时回收,避免强杀在飞写库(原 10s 与 POST 超时同值,
	// 慢 DB/网络下卡边界)。httpSrv.Shutdown 与三类协程等待共用此预算——单一 deadline 而非各自独立
	// 超时串行累加(快的先过,慢的吃剩余,总时长封顶 30s)。
	const shutdownTimeout = 30 * time.Second
	shutCtx, shutCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutCancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		log.Error("HTTP 关闭失败", "err", err)
	}
	schDone, bgDone, cbDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() { sch.Wait(); close(schDone) }()
	go func() { bg.Wait(); close(bgDone) }()
	go func() {
		if cbPump != nil {
			cbPump.Wait()
		}
		close(cbDone)
	}()
	deadline := time.Now().Add(shutdownTimeout)
	for _, w := range []struct {
		name string
		done <-chan struct{}
	}{
		{"调度协程", schDone},
		{"后台清理协程", bgDone},
		{"回调 pump", cbDone},
	} {
		if remaining := time.Until(deadline); remaining > 0 {
			t := time.NewTimer(remaining)
			select {
			case <-w.done:
				t.Stop()
			case <-t.C:
				log.Warn("关闭预算内未退出,继续关闭", "wait", w.name, "budget", shutdownTimeout)
			}
		} else {
			log.Warn("关闭预算耗尽,部分协程仍运行,继续关闭", "wait", w.name, "budget", shutdownTimeout)
		}
	}
	if sqlDB, err := st.DB.DB(); err == nil {
		_ = sqlDB.Close()
	}
	log.Info("已安全退出")
}

// routerDeps buildRouter 的装配依赖。
type routerDeps struct {
	cfg          *config.Config
	st           *repository.Store
	appSvc       *dservice.AppService
	adminUserSvc *dservice.AdminUserService
	jobSvc       *dservice.JobService
	insSvc       *dservice.InstanceService
	reg          *workerreg.Registry
	il           *instancelog.Logger
	authStore    *auth.Store
	loginSvc     *auth.LoginService
	pjClient     *powerjob.Client // PowerJob 同步客户端(/apps/:appId/jobs/import-powerjob)
	webFS        fs.FS
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
	api.POST("/auth/auto-login", own.LoginRateLimit(d.cfg.Auth.Login.MaxAttemptsPerMin), own.AutoLoginHandler(d.loginSvc, d.cfg.Debug.AutoLogin))
	own.RegisterAuth(api, own.Deps{Auth: d.authStore, AdminUsers: d.adminUserSvc})
	own.Register(api, own.Deps{
		Apps: d.appSvc, Jobs: d.jobSvc, Instances: d.insSvc,
		Store: d.st, Auth: d.authStore, Reg: d.reg, AdminUsers: d.adminUserSvc,
		PowerJobClient: d.pjClient,
	})
	// /account/*:当前管理员自查/改用户名/改密码(仅管理员,挂 SessionAuth + RequireAdmin)。
	own.RegisterAccount(api, own.Deps{Auth: d.authStore, AdminUsers: d.adminUserSvc})

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

// health 探活 DB:Ping 失败返回 503 degraded。返回 status + driver(匿名可见,便于前端展示)。
func health(c *gin.Context, st *repository.Store) {
	status, httpStatus := "ok", 200
	if sqlDB, err := st.DB.DB(); err != nil {
		status, httpStatus = "degraded", 503
	} else if err := sqlDB.Ping(); err != nil {
		status, httpStatus = "degraded", 503
	}
	c.JSON(httpStatus, gin.H{"code": 0, "msg": "ok", "data": gin.H{
		"status": status,
		"driver": st.Driver,
	}})
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
