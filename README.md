# task-schedule

基于 Go + Gin + GORM 的**轻量任务调度服务**,采用**统一 worker 派发模型**:worker 无 token 心跳上报
(appName + systemMetrics + tags)→ 调度器按 tag 匹配 + score 择优选址 → 异步 POST 固定 body 到 worker
→ worker 回报状态推进终态 → reaper / RetryPump 兜底。默认 SQLite(纯 Go 驱动,无 CGO,静态二进制),
可一行切 MySQL。前端管理台 `//go:embed` 编译进单二进制。

- **统一执行模型**:服务端选 worker 异步派发,worker 回报。砍掉用户自定义 webhook URL/headers/body。
- **两种 worker 接入**:`/worker/*`(通用 http,固定 body)/`/server/*`(标准 PowerJob Java worker 不改源码)。
- **8 态状态机** + **DB 驱动重试**(重启不丢)+ **reaper 失败转移**(worker 失联/超时)。
- **实例日志落本地文件**,按 root 实例聚合,呈现一次触发的完整时间线(调度埋点 + worker 上报)。
- **管理端账户登录**(管理员配置注入 + 应用账户走 app 表)+ 权限矩阵(admin/app 双角色)。
- int 自增主键 + AppName 全局唯一;PowerJob 兼容(`/server/*` + runJob)与新模型统一收敛。

> 设计全貌见 [`docs/refactor-unified-model.md`](docs/refactor-unified-model.md)。

## 快速开始

```bash
# 1. 构建前端(嵌入进二进制)
cd web && npm install && npm run build && cd ..

# 2. 运行(debug 模式默认种占位管理员 admin / change-me-admin,开箱即用)
go run .

# 3. 另起一个终端,跑示例 http worker(先在管理台或用 API 建 app "demo")
go run ./examples/http-worker -server http://127.0.0.1:8080 -app demo -addr :9001

# 4. 打开 http://127.0.0.1:8080 → 用 admin / change-me-admin 登录 → 建 app → 建 manual 任务 → 触发
```

release 模式拒绝默认占位密码启动(见下「鉴权」)。

## 目录结构

```
task-schedule/
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
实例: queued → dispatched → waiting_receive ──worker回报──→ running ──→ success/failed
                  │                                           │
                  │      worker 失联/执行超时 ──reaper──→ failed │
                  └──── failed 且未达上限 ──→ next_retry_time ──→ RetryPump 重派
```

- **异步派发**:POST 仅交付任务(2xx=已接收),worker 异步执行后回调上报终态。
- **任务级并发槽**随实例生命周期(派发后绑定,终态/reaper 释放);`MaxConcurrency` 按在飞实例数计。
- **reaper**:扫 `waiting_receive`/`running`,worker 心跳超时或超 `TimeoutSec` → `failed` + 触发重试。
- **RetryPump**:扫 `failed` 且 `next_retry_time` 到期,按 `retryIndex+1` 重派(DB 驱动,重启不丢)。
- **at-least-once**:worker 迟到回报可能与重派实例并存,业务需自行幂等。

### 状态机(8 态)

| 状态 | 含义 |
|---|---|
| `queued` | 排队(并发超限) |
| `waiting_receive` | 已派发,等 worker 接收 |
| `running` | 运行中(worker 上报) |
| `success` / `failed` | 终态(失败含执行超时) |
| `skipped` | 排队等待超时 |
| `canceled` / `stopped` | 取消 / 手动停止 |

终态不可回退(worker 迟到上报忽略;`MarkDispatched` / `UpdateResult` 均守护终态)。

### 调度类型

| `schedule_kind` | `schedule_expr` |
|---|---|
| `cron` | 标准 5 段 `"0 9 * * *"` |
| `fix_rate` / `fix_delay` | 毫秒数 `"5000"` |
| `delay` | 秒数 `"300"` |
| `run_at` | RFC3339 一次性 |
| `manual` | —(仅手动触发) |

## 鉴权

**管理员账户**(配置注入,不入库):`config.yaml` 的 `auth.admins` 或环境变量
`TASK_SCHEDULE_ADMIN_USERNAME` / `TASK_SCHEDULE_ADMIN_PASSWORD`(明文,加载时 bcrypt)。
debug 模式 `admins` 为空则自动种占位 `admin / change-me-admin`;**release 模式拒绝默认占位启动**。

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
  `{status, result}`、`POST /worker/instances/:iid/logs` `{level, message, time}`。状态用领域 string。
- `/server/*` **PowerJob 协议**:标准 PowerJob Java worker 不改源码接入(assert/acquire/workerHeartbeat/
  reportInstanceStatus/reportLog),派发用官方 `runJob`(`ServerScheduleJobReq`),状态用官方数字码。

**选址**:`jobInstanceTag = instance.tag || job.tag`;候选 = `online(appName) ∧ matchTag`;`matchTag` =
`acceptNotTagJob || tag ∈ worker.tags || (tag 空 ∧ worker.tags 空)`;按 `systemMetrics.score` 降序取首。

> ⚠ `/server/*`、`/worker/*` 无鉴权:任何人可注册任意 worker 地址 → 到期 job 主动 POST 该地址(SSRF),
> 可伪造实例状态。**生产必须通过网络隔离保护**(见 `deploy/nginx-isolation.conf.example`),切勿直接暴露公网。
>
> 可选纵深防御:`worker.allowed_cidrs`(config.yaml)限制可注册的 worker 地址网段(CIDR/IP),
> 非白名单地址的注册被拒(`worker` 协议返 400,`server` 协议静默不注册)。默认空=不限制;启用后
> 建议填可信 worker 网段(如 `10.0.0.0/8`)。

示例 worker:`examples/http-worker/` 演示 `/worker/*` 最小接入(心跳 + `/run` 回显 + 异步回报 success)。

## 配置

见 `config.yaml`(全部字段带注释)。关键环境变量(优先级高于文件):

| 环境变量 | 作用 |
|---|---|
| `TASK_SCHEDULE_ADMIN_USERNAME` / `TASK_SCHEDULE_ADMIN_PASSWORD` | 管理员账户(明文密码加载时 bcrypt) |
| `TASK_SCHEDULE_DB_DRIVER` | `sqlite` / `mysql` |
| `TASK_SCHEDULE_MYSQL_DSN` | mysql DSN |
| `TASK_SCHEDULE_SERVER_MODE` | `debug` / `release` / `test` |
| `TASK_SCHEDULE_POWERJOB_SERVER_ADDRESS` | `/server/acquire` 返回值(PowerJob worker 可达地址) |

实例日志落 `{log.dir}/instances/{appID}/{instanceID}_{rootInstanceID}.log`;`log.instance_retention_days`
按 mtime 清理(0=不清理)。

## 部署

**单二进制**:`web/dist` 经 `//go:embed` 编译进二进制,`CGO_ENABLED=0` 静态产出。

```bash
cd web && npm install && npm run build && cd ..
CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags="-s -w" -o task-schedule .
./task-schedule -config config.yaml
```

**Docker**:`docker compose up -d`(compose 已注入 release 必填的 `TASK_SCHEDULE_ADMIN_PASSWORD`,
镜像内 `//go:embed` 前端,健康检查打 `/health`,DB 不可达返回 503 供探针判定)。

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
| GET | `/api/apps/:appId/instances/:iid/logs?group=1` | 实例日志(同 root 聚合) |
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
