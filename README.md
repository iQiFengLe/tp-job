# tp-job

> 单二进制、零外部依赖(Go + 默认 SQLite)的轻量级任务调度服务:把任务**传送**(tp = teleport,游戏里"传送"的缩写)到 worker 执行——
> 统一 worker 派发、9 态状态机、DB 驱动重试、失败转移、实例日志、Web 管理台。适合做中小团队后台定时任务 /
> 业务编排的调度内核,也能对接已有的 PowerJob 自研 http worker;业务侧不嵌入 SDK、不绑 SaaS。

基于 Go + Gin + GORM 的**轻量任务调度服务**,采用**统一 worker 派发模型**:worker 无 token 心跳上报
(appName + systemMetrics + tags)→ 调度器按 tag 匹配 + score 择优选址 → 异步 POST 固定 body 到 worker
→ worker 回报状态推进终态 → reaper / RetryPump 兜底。默认 SQLite(纯 Go 驱动,无 CGO,静态二进制),
可一行切 MySQL。前端管理台 `//go:embed` 编译进单二进制。

- **统一执行模型**:服务端选 worker 异步派发,worker 回报。砍掉用户自定义 webhook URL/headers/body。
- **两种 worker 接入**:`/worker/*`(通用 http,固定 body)/`/server/*`(遵循 PowerJob 协议的自研 http worker)。不支持官方 Java processor,无任何 SDK。
- **9 态状态机** + **DB 驱动重试**(重启不丢)+ **reaper 失败转移**(worker 失联/超时)。
- **实例日志落本地文件**,按 root 实例聚合,呈现一次触发的完整时间线(调度埋点 + worker 上报)。
- **管理端账户登录**(管理员 admin_user 表 + 应用账户走 app 表)+ 权限矩阵(admin/app 双角色)。
- int 自增主键 + AppName 全局唯一;PowerJob 兼容(`/server/*` + runJob)与新模型统一收敛。

> 设计全貌见 [`docs/refactor-unified-model.md`](docs/refactor-unified-model.md)。

## 环境要求

| 组件 | 版本 | 说明 |
|---|---|---|
| Go | 1.26+ | 后端构建/运行(见 `go.mod`) |
| Node.js | 20+ | 仅构建前端管理台(产物 `//go:embed` 进二进制,运行时不需要) |
| OS | Linux / macOS / Windows | 纯 Go SQLite,无 CGO,跨平台静态二进制 |
| 数据库 | SQLite(默认,零配置) / MySQL 8+(可选) | SQLite 文件落盘即可;高负载切 `database.driver=mysql` |

> 生产只需 **一个二进制 + 一份配置**;Node 仅在构建前端时出现,不进运行环境。

## 快速开始

```bash
# 1. 构建前端(嵌入进二进制)
cd web && npm install && npm run build && cd ..

# 2. 运行(debug 模式默认种管理员 admin / admin123,开箱即用)
go run .

# 3. 另起一个终端,跑示例 http worker(先在管理台或用 API 建 app "demo")
go run ./examples/http-worker -server http://127.0.0.1:8080 -app demo -addr :9001

# 4. 打开 http://127.0.0.1:8080 → 用 admin / admin123 登录 → 建 app → 建 api 任务 → 触发
```

release 模式强制登录限流(`config.release.yaml` 显式配 `auth.login.max_attempts_per_min`),
不拦截默认口令——默认 `admin/admin123` 首登后请立即在「账户设置」改密(见下「鉴权」)。

## 目录结构

```
tp-job/
├── main.go                    # 入口:装配新栈 + 后台循环 + 优雅关闭 + 路由
├── config.yaml                # 配置
├── examples/http-worker/      # 最小 http worker 示例(/worker/* 协议)
├── web/                       # 前端管理台(React + antd),dist 嵌入二进制
├── deploy/nginx-isolation.conf.example  # /server/* /worker/* 网络隔离示例
└── internal/
    ├── domain/                # App/Job/Instance + 8 态状态机 + Executor/DispatchBody/SystemMetrics
    ├── repository/            # GORM 仓储(App/Job/Instance + OpenDatabase + 终态守护)
    ├── instancelog/           # 实例日志:文件 + per-file 锁 + 同 root 聚合 + 清理
    ├── workerreg/             # worker 心跳注册表(按 AppID)+ PickFull(tag+score)
    ├── schedtime/             # cron 解析 + 按 ScheduleKind 推算 next_run
    ├── dispatch/              # 统一调度器 + Executor + reaper + RetryPump + 并发槽/排队
    ├── dservice/              # 领域服务(App/Job/Instance 业务)
    ├── auth/                  # 会话 store + 登录服务 + SessionAuth/RequireAdmin/AppScope
    ├── config/ logger/        # 配置加载 / slog+lumberjack 日志
    └── protocol/
        ├── own/               # 管理端 REST /api/*(dto + translator + 登录 + 权限矩阵)
        ├── worker/            # 简化 worker 协议 /worker/*(心跳/回报,无鉴权)
        └── powerjob/          # PowerJob 协议 /server/*(assert/acquire/heartbeat/reportStatus/reportLog)
```

## 执行模型

```
worker 启动 → 心跳 {appName, workerAddress, systemMetrics, tags} (无 token)
                  │
任务到期/手动触发 → 调度器认领(AdvanceNextRun 乐观锁)→ 选 worker(tag 匹配 + score 择优)
                  │
服务端 POST {jobParams, jobInstanceParams, jobId, jobInstanceId} → worker.workerAddress
                  │ (worker ACK 2xx = 已接收)
实例: queued → dispatched → waiting_receive ──worker回报──→ running ──→ success/failed/timeout
                  │                                           │
                  │      worker 失联 ──reaper──→ failed        │
                  │      执行超时 ──reaper──→ timeout          │
                  └──── failed/timeout 且未达上限 ──→ next_retry_time ──→ RetryPump 重派
```

- **异步派发**:POST 仅交付任务(2xx=已接收),worker 异步执行后回调上报终态。
- **任务级并发槽**随实例生命周期(派发后绑定,终态/reaper 释放);`MaxConcurrency` 按在飞实例数计。
- **reaper**:扫 `waiting_receive`/`running`,worker 心跳超时 → `failed`;执行超 `TimeoutSec` → `timeout`;均触发重试。
- **RetryPump**:扫 `failed`/`timeout` 且 `next_retry_time` 到期,按 `retryIndex+1` 重派(DB 驱动,重启不丢)。
- **at-least-once**:worker 迟到回报可能与重派实例并存,业务需自行幂等。

### 状态机(9 态)

| 状态 | 含义 |
|---|---|
| `queued` | 排队(并发超限) |
| `waiting_receive` | 已派发,等 worker 接收 |
| `running` | 运行中(worker 上报) |
| `success` | 成功 |
| `failed` | 失败(worker 失联/派发失败/重启清理) |
| `timeout` | 执行超时 |
| `skipped` | 排队等待超时(⚠ 当前未实现,预留) |
| `canceled` / `stopped` | 取消 / 手动停止 |

终态不可回退(worker 迟到上报忽略;`MarkDispatched` / `UpdateResult` 均守护终态)。

### 调度类型

| `schedule_kind` | `schedule_expr` |
|---|---|
| `cron` | 标准 5 段 `"0 9 * * *"` |
| `fix_rate` / `fix_delay` | 毫秒数 `"5000"` |
| `delay` | 秒数 `"300"` |
| `run_at` | RFC3339 一次性 |
| `api` | —(仅 API/手动触发) |

### 实例状态变更回调

Job 配 `callback_url` 时,实例每次状态变化(派发 / 运行 / 终态 / 重试)由 `CallbackPump` POST 通知对端,
payload 为事件瞬间快照。投递语义为**至少一次**(at-least-once):

- 失败指数退避重试(`backoff_base_sec` → `backoff_max_sec`),达 `max_attempts` 仍不成功置 `dead`。
- **接收方必须幂等**:对端已返回 2xx 但本地记账(`MarkSent`)因 DB 瞬时故障失败时,pump 会退避后重投,
  同一事件可能被投递多次。用请求头 `X-TaskSchedule-Event-ID`(`cb-<callbackId>`)去重。
- 极端情况(DB 持续故障)下,一个实际已送达的事件可能重投至上限被标 `dead`——`dead` 表示"本地放弃投递",
  不等同于"对端从未收到",排查需结合对端日志。
- callback URL 可信性靠部署侧网络隔离(同 /server/*、/worker/*);禁重定向防 302 诱导。
- `retention_days` 控制已终态(`sent` / `dead`)回调记录的保留期,超期自动清理;`pending` 永不删(未投递保证)。

## 鉴权

**管理员账户**(admin_user 表):首次启动自动 seed `admin / admin123`,登录后立即在 Web 顶部「账户设置」改用户名/密码。**不走 config.yaml、不支持环境变量注入**(旧版 env 注入已移除——`TP_JOB_ADMIN_USERNAME/PASSWORD` 会被静默忽略,勿用)。release 模式由 `config.release.yaml` 的 `server.mode=release` + `auth.login.max_attempts_per_min` 控制(不再经 env 覆盖 mode)。

**应用账户**:app 表(AppName + bcrypt Password),worker 心跳不校验密码(靠 appName + 网络隔离)。

`POST /api/auth/login {ident, password}` → 先匹配管理员用户名,否则匹配 app 名 → 颁发 session token;
后续 `Authorization: Bearer <token>`。

| 操作 | 管理员 | 应用账户 |
|---|---|---|
| 新增 / 删除 app | ✓ | ✗ |
| 列出全部 app | ✓ | ✗(仅自己) |
| 修改 app / 切换 app | ✓(任意) | ✓(仅自己) |
| job / instance / 日志 CRUD | ✓(切到任意 app) | ✓(仅自己) |

## Worker 接入

两组端点均**无 token**(靠 appName 归属 + 网络隔离保护,对齐 PowerJob Server):

- `/worker/*` **简化 http 协议**:心跳 `POST /worker/heartbeat`;派发 `POST /run`
  body `{jobParams, jobInstanceParams, jobId, jobInstanceId}`;回报 `POST /worker/instances/:iid/status`
  `{workerAddress, status, result}`(`workerAddress` 须与实例绑定一致,防伪造 id 篡改)、
  `POST /worker/instances/:iid/logs` `{level, message, time}`。状态用领域 string。
- `/server/*` **PowerJob 协议**:遵循 PowerJob 协议的自研 http worker 接入(assert/acquire/workerHeartbeat/
  reportInstanceStatus/reportLog),派发用 `runJob`(`ServerScheduleJobReq` 子集),状态用官方数字码。
  不支持官方 Java processor(无 processorInfo 派发),不提供任何语言 SDK。

**选址**:`jobInstanceTag = instance.tag || job.tag`;候选 = `online(appName) ∧ matchTag`;`matchTag` =
`acceptNotTagJob || tag ∈ worker.tags || (tag 空 ∧ worker.tags 空)`;按 `systemMetrics.score` 降序取首。

> ⚠ `/server/*`、`/worker/*` 无鉴权:任何人可注册任意 worker 地址 → 到期 job 主动 POST 该地址(SSRF),
> 可伪造实例状态。**生产必须通过网络隔离保护**(见 `deploy/nginx-isolation.conf.example`),切勿直接暴露公网。

示例 worker:`examples/http-worker/` 演示 `/worker/*` 最小接入(心跳 + `/run` 回显 + 异步回报 success)。

## 配置

见 `config.yaml`(全部字段带注释)。关键环境变量(优先级高于文件):

| 环境变量 | 作用 |
|---|---|
| `TP_JOB_DB_DRIVER` | `sqlite` / `mysql` |
| `TP_JOB_MYSQL_DSN` | mysql DSN |
| `TP_JOB_POWERJOB_SERVER_ADDRESS` | `/server/acquire` 返回值(PowerJob worker 可达地址) |

> `server.mode` 不支持 env 覆盖(防降级绕过 release 限流强制),仅由 config.yaml 决定;部署需 release 用 `config.release.yaml`。
> 管理员账户走 admin_user 表(首次启动 seed admin/admin123),不支持 env 注入。

实例日志落 `{log.dir}/instances/{appID}/{instanceID}_{rootInstanceID}.log`;`log.instance_retention_days`
按 mtime 清理(0=不清理)。

## 部署

**单二进制**:`web/dist` 经 `//go:embed` 编译进二进制,`CGO_ENABLED=0` 静态产出。

```bash
cd web && npm install && npm run build && cd ..
CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags="-s -w" -o tp-job .
./tp-job -config config.yaml
```

**裸二进制启停(可选)**:仓库根的 `start.sh` / `stop.sh` 提供基于 pid 文件的启停(支持同机多实例,
各自维护 `tp-job.pid`)。`start.sh` 按 CPU 架构选择 `tp-job-linux-amd64` /
`tp-job-linux-arm64`,需先构建或下载对应架构二进制放仓库根(二进制本身不入库):

```bash
# amd64 示例(arm64 把 GOARCH 改成 arm64)。先打前端见上一节。
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -buildvcs=false -trimpath -ldflags="-s -w" -o tp-job-linux-amd64 .
./start.sh   # 后台启动,日志见 stdio.log,pid 见 tp-job.pid
./stop.sh    # 优雅停止(SIGTERM,15s 超时再 SIGKILL)
```

**Docker**:`docker compose up -d`(compose 经 `config.release.yaml` 设 release + 登录限流;镜像内
`//go:embed` 前端,健康检查打 `/health`,DB 不可达返回 503 供探针判定)。首启种 `admin/admin123`,
**登录后立即在「账户设置」改密**。

**网络隔离(生产必做)**:用 `deploy/nginx-isolation.conf.example` 前置反代,仅放行可信 worker 网段访问
`/server/*`、`/worker/*`;`/api/*` 自带登录鉴权可正常暴露。

## 主要 API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/auth/login` | 登录(admin/app),返 Bearer token |
| GET | `/api/auth/me` | 当前会话身份 |
| POST | `/api/auth/logout` | 注销 |
| POST/GET/PUT/DELETE | `/api/apps[/:appId]` | app 管理(仅 admin 新增/删除/列出全部) |
| POST/GET/PUT/DELETE | `/api/apps/:appId/jobs[/:id]` | job CRUD |
| POST | `/api/apps/:appId/jobs/:id/trigger` | 手动触发 |
| GET | `/api/apps/:appId/instances` | 实例列表(按 job_id/status 过滤) |
| GET | `/api/apps/:appId/instances/:iid/logs` | 实例日志(按行 offset/limit 分页) |
| GET | `/api/apps/:appId/workers` | 在线 worker 列表(读内存注册表) |
| POST | `/worker/heartbeat`、`/worker/instances/:iid/{status,logs}` | http worker 协议(无鉴权) |
| `*` | `/server/*` | PowerJob 协议(无鉴权) |

## 开发

```bash
go build -buildvcs=false ./... && go vet -buildvcs=false ./... && go test -buildvcs=false ./...
cd web && npm run build      # 前端门槛:tsc + vite
```

- 数据库无历史负担:drop 旧库重建即可(`app` / `job` / `job_instance` 三表由 AutoMigrate 创建)。
- 调度器/reaper/retry 单测见 `internal/dispatch`;权限矩阵集成测试见 `internal/protocol/own`。

## 声明

- 本项目(tp-job)为独立开发的任务调度服务,与 [PowerJob](https://github.com/PowerJob/PowerJob)
  官方项目及其团队**无任何隶属、代言或关联**。
- "PowerJob" 是其各自所有者的商标/项目名。本项目仅在 `/server/*` 与 `/openApi/*` 端点实现了与
  PowerJob 通信协议的部分兼容,目的是让已对接 PowerJob 的自研 http worker / 业务客户端尽量零改动接入;
  本项目**不包含 PowerJob 的任何源代码**,也不提供官方 Java processor 或任何语言 SDK。

## 许可证

[Apache License 2.0](./LICENSE)。贡献即视为按该协议授权。
