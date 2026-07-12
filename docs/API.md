# tp-job API 参考

HTTP 接口文档。设计全貌见 [`design.md`](./design.md),部署/配置见根目录 README。

端点分四组:

| 组 | 前缀 | 鉴权 | 面向 |
|---|---|---|---|
| 管理端 | `/api/*` | 登录会话(Bearer) | 管理台 / 业务后台 |
| 简化 worker | `/worker/*` | 无(靠 appName + 网络隔离) | 通用 HTTP worker |
| PowerJob Server | `/server/*` | 无 | PowerJob Java worker |
| PowerJob OpenAPI | `/openApi/*` | 无 | PowerJob 业务客户端 |
| 探活 | `/health` | 无 | 监控 |

---

## 全局约定

### 响应格式

**自有协议**(`/health`、`/api/*`、`/worker/*` 带 JSON 体的端点):

```
成功: {"code": 0, "msg": "ok", "data": <data>}        HTTP 200
失败: {"code": <HTTP状态码>, "msg": "<描述>"}           HTTP = 该状态码(无 data)
```

**PowerJob 兼容**(`/server/*`、`/openApi/*`,始终 HTTP 200,对齐原版):

| Envelope | 用于 | data 形态 |
|---|---|---|
| `ResultDTO` `{success, data?, message?}` | `/server/assert`、`/server/acquire`、`/openApi/*`(除 runJob2) | 裸业务对象,失败时省略 |
| `AskResponse` `{success, data?, message?}` | `/server/reportInstanceStatus`、`/server/queryJobCluster` | **base64(业务对象 JSON)**;成功无数据时 `data=base64("null")` |
| `PowerResultDTO` `{success, data?, message?, code?}` | `/openApi/runJob2` | ResultDTO + 可选 `code` |

**始终 HTTP 200 + 空体**(对齐 PowerJob,避免 worker 反压):`/server/workerHeartbeat`、`/server/reportLog`、`/worker/instances/:iid/logs`。

### 鉴权(管理端)

- 登录:`POST /api/auth/login` → 拿 `token`(`base64url(rand 32B)`,约 43 字符)
- 请求头:`Authorization: Bearer <token>`(前缀大小写不敏感)
- 会话存内存,**进程重启即失效**;TTL 由 `auth.session.ttl_seconds` 控制(默认 86400s = 24h)
- 登录限流:每 IP 每分钟 `auth.login.max_attempts_per_min` 次,超限 → 429;release 模式启动强制要求 >0
- 角色:`admin`(admin_user 表,首启 seed `admin`/`admin123`,**首登必须改密**)、`app`(app 表,ident=AppName)

中间件:

| 中间件 | 失败响应 |
|---|---|
| `SessionAuth` | 401 `缺少认证信息` / `认证已失效,请重新登录` |
| `RequireAdmin` | 403 `需要管理员权限` |
| `AppScope("appId")` | admin 放行任意;app 仅当路径 `:appId` = 自身 AppID,否则 403 `无权访问该 app 资源` |

### 权限矩阵

| 资源 | 管理员 | 应用账户 |
|---|---|---|
| 新增 / 列出 / 删除 app | ✓ | ✗ |
| 查看 / 修改 app | ✓(任意) | ✓(仅自己) |
| job / instance / worker | ✓(切到任意 app) | ✓(仅自己 app) |
| import-powerjob | ✓ | ✗ |
| `/account/*`(自查/改密) | ✓ | ✗ |

### 通用约定

- **分页**:查询参数 `page`(默认 1,上限 100000)、`size`(默认 20,1~500);响应统一 `{list, total}`
- **请求体上限**:`server.max_request_body_mb`,默认 2MB(全路由)
- **不信任代理**:`SetTrustedProxies(nil)` → ClientIP 取 TCP 源地址,防登录限流被 `X-Forwarded-For` 绕过
- **终态守护**:终态(success/failed/timeout/skipped/canceled/stopped)不可回退,worker 乱序/重复上报被拒
- **越权防护**:`:iid` 归属在 handler 内二次校验(`GetInApp`),`AppScope` 只校验 `:appId` 路径
- **FlexInt64**:`/server/*` 的 `instanceId`/`jobId`/`appId` 兼容数字或字符串 JSON 写法(适配多语言 worker)

### PowerJob 数字状态码对照

`/server/reportInstanceStatus` 与 `/openApi/fetchInstanceStatus` 用 PowerJob 数字码,`/worker/*` 用领域字符串。对照:

| 数字 | 领域 string | 含义 | worker 可上报 |
|---|---|---|---|
| 1 | `queued` | 排队(并发超限) | ✓ |
| 2 | `waiting_receive` | 已派发等 worker 接收 | ✓ |
| 3 | `running` | 运行中 | ✓ |
| 4 | `failed` | 失败 | ✓ |
| 5 | `success` | 成功 | ✓ |
| 6 | `skipped` | 跳过 | ✗ 服务端专用 |
| 7 | `timeout` | 执行超时 | ✗ 服务端专用 |
| 9 | `canceled` | 取消 | ✓ |
| 10 | `stopped` | 手动停止 | ✓ |

worker 合法上报值集合:`{1,2,3,4,5,9,10}`;6/7 为非法码,静默忽略。

---

## 探活

### `GET /health`

无鉴权。DB Ping 探活。

- 响应 data:`{status: "ok"|"degraded", driver: "sqlite"|"mysql"}`
- 状态码:Ping 正常 200;Ping 失败 **503**(`code/msg` 不变,靠 `status:"degraded"` 标识)

---

## 管理端 `/api`

### 鉴权 `/api/auth`

#### `POST /api/auth/login`

无鉴权(前置登录限流)。

请求体 `LoginReq`:

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `ident` | string | 是 | 管理员用户名 或 app 名 |
| `password` | string | 是 | 明文密码 |

响应 data `LoginResp`:

| 字段 | 类型 | 说明 |
|---|---|---|
| `token` | string | Bearer token |
| `role` | string | `admin` / `app` |
| `username` | string | 管理员用户名 / app 名 |
| `app_id` | int64 | 仅 app 角色 |
| `app_name` | string | 仅 app 角色 |
| `expires_at` | string(RFC3339) | 会话过期时刻 |

匹配顺序:先 admin_user 表(命中不回退,防同名 app 凭据),再 app 表(bcrypt + 禁用判断)。错误统一 401 `用户名或密码错误`(防枚举);限流 429。

```bash
curl -XPOST http://127.0.0.1:8080/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"ident":"admin","password":"admin123"}'
```

#### `POST /api/auth/auto-login`

无鉴权(登录限流)。仅 `debug.auto_login=true` 时启用(release 必须 false)。无请求体,用默认管理员凭据走真实校验(默认密码被改后自然 401)。响应同 `LoginResp`;未启用 401 `自动登录未启用`。

#### `GET /api/auth/me`

SessionAuth(任意角色)。响应 data:`{role, username, app_id?, app_name?}`(admin 用户名实时查库)。

#### `POST /api/auth/logout`

SessionAuth,幂等。响应 data:`{logged_out: true}`。

---

### 账户 `/api/account`(仅管理员)

#### `GET /api/account/profile`

响应 data:`{id: int64, username: string}`。

#### `PUT /api/account/profile`

请求体 `{username: string}`(必填)。响应 data:`{id}`。用户名 <3 字符 400;重名 409。

#### `PUT /api/account/password`

请求体:

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `old_password` | string | 是 | 当前密码 |
| `new_password` | string | 是 | 新密码(服务端 bcrypt) |

响应 data:`{id}`。旧密码错 400;账户不存在 404。

---

### 应用 `/api/apps`

#### `POST /api/apps` (admin)

请求体 `CreateAppReq`:`{app_name, password, status?}`(status 默认 0)。响应 data `AppView`:`{id, app_name, status, created_at, updated_at}`。重名 409 `ErrAppInUse`。

#### `GET /api/apps` (admin)

查询:`keyword?`、`page`、`size`。响应 data:`{list: [AppView], total}`。

#### `GET /api/apps/:appId` (AppScope)

响应 data `AppView`。不存在 404。

#### `PUT /api/apps/:appId` (AppScope)

请求体(全指针,nil=不改):

| 字段 | 类型 | 说明 |
|---|---|---|
| `app_name` | string? | 新名 |
| `password` | string? | 新密码;**app 角色改密须带 `old_password` 验身**,admin 改任意不校验 |
| `old_password` | string? | app 角色改密时必填 |
| `status` | int8? | 状态 |

响应 data:`{id}`。重名 409;旧密码校验失败 400 `旧密码校验失败`。

#### `DELETE /api/apps/:appId` (admin)

响应 data:`{id}`。app 名下仍有 job 时 409 `ErrAppInUse`。

---

### 任务 `/api/apps/:appId/jobs`(除注明外均 AppScope)

#### `POST /api/apps/:appId/jobs`

请求体 `CreateJobReq`:

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `name` | string | — | 必填 |
| `description` | string | — | |
| `execute_type` | string | `http` | 当前唯一 |
| `job_params` | string | — | 透传 worker 的任务参数 |
| `tag` | string | — | worker 选址标签 |
| `timeout_sec` | int | 0 | 单实例执行超时(reaper 判 timeout) |
| `schedule_kind` | string | — | `cron`/`fix_rate`/`fix_delay`/`delay`/`run_at`/`api` |
| `schedule_expr` | string | — | 调度表达式 |
| `start_time` | int64? | — | 生效起始(毫秒戳) |
| `end_time` | int64? | — | 生效截止(毫秒戳) |
| `max_concurrency` | int | 1 | 最大并发实例数 |
| `max_wait_seconds` | int | 0 | 并发满时排队等待秒 |
| `retry_count` | int | 0 | 失败重试次数 |
| `retry_interval_sec` | int | 0 | 重试间隔 |
| `retry_jitter` | string | — | 抖动区间 `"min:max"` |
| `retry_max_backoff_sec` | int | 1800 | 退避上限 |
| `default_priority` | int | 0 | 实例优先级 |
| `callback_url` | string | — | 实例状态变更回调 URL |
| `enabled` | bool? | true | 是否启用 |

响应 data `JobView`(字段见下方 GET)。

#### `GET /api/apps/:appId/jobs`

查询:`page`、`size`。响应 data:`{list: [JobView], total}`。

#### `GET /api/apps/:appId/jobs/:id`

响应 data `JobView`:

| 字段 | 类型 | 说明 |
|---|---|---|
| `id`, `app_id` | int64 | |
| `name`, `description`, `execute_type`, `job_params`, `tag` | string | |
| `timeout_sec`, `max_concurrency`, `max_wait_seconds` | int | |
| `retry_count`, `retry_interval_sec`, `retry_max_backoff_sec`, `default_priority` | int | |
| `retry_jitter` | string | `"min:max"` |
| `schedule_kind`, `schedule_expr` | string | |
| `next_run_time` | string?(RFC3339) | 下次触发 |
| `start_time`, `end_time` | int64 | 毫秒戳,0=无界 |
| `callback_url` | string | |
| `enabled` | bool | |
| `from_id` | string | 来源 ID(自建=uuid;PowerJob=`pj:<serverKey>:<原jobID>`) |
| `from_type` | string | `SELF` / `powerjob` |
| `created_at`, `updated_at` | string(RFC3339) | |

不存在 404。

#### `PUT /api/apps/:appId/jobs/:id`

请求体 `UpdateJobReq`(全指针,nil=不改;`start_time`/`end_time` <=0 清空,>0 设值)。字段同 CreateJobReq 的指针版。响应 data:`{id}`。

#### `DELETE /api/apps/:appId/jobs/:id`

响应 data:`{id}`。

#### `POST /api/apps/:appId/jobs/:id/trigger`

查询参数:`priority`(默认 0)、`instance_params`(可选,透传给实例)。响应 data:`{id, triggered: true, priority}`。实例 `trigger_type=manual`,source `api`。

```bash
curl -XPOST 'http://127.0.0.1:8080/api/apps/1/jobs/1/trigger?priority=5&instance_params=hello' \
  -H 'Authorization: Bearer <token>'
```

#### `POST /api/apps/:appId/jobs/import-powerjob` (admin,SSRF 风险)

作为 PowerJob 客户端,从外部 server 拉 job 定义转换并 upsert。请求体:

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `server_address` | string | 是 | 如 `http://host:7700` |
| `app_name` | string | 是 | PowerJob app 名 |
| `password` | string | — | app 密码 |
| `token` | string | — | `POWERJOB-TOKEN`(4.3.3+) |
| `dry_run` | bool | — | true=仅预览不落库 |

响应 data:`{fetched, imported, updated, skipped, preview: [{name, schedule_kind, schedule_expr, enabled, conflict, error?}]}`。客户端未装配 503。

---

### 实例 `/api/apps/:appId/instances`(均 AppScope)

#### `GET /api/apps/:appId/instances`

查询:`job_id?`、`status?`(领域值,如 `running`/`failed`)、`page`、`size`。响应 data:`{list: [InstanceView], total}`。

`InstanceView` 字段:

| 字段 | 类型 | 说明 |
|---|---|---|
| `id`, `job_id`, `app_id` | int64 | |
| `status` | string | 领域状态码 |
| `trigger_type` | string | auto/manual/retry |
| `schedule_kind` | string | 列表批量从关联 job 填(单查不填) |
| `priority`, `retry_index` | int | |
| `root_instance_id` | int64 | 归属链首 |
| `tag`, `worker_address`, `job_instance_params`, `result` | string | |
| `trigger_time` | string(RFC3339) | |
| `start_time`, `end_time` | string?(RFC3339) | |
| `duration_ms` | int64 | |

#### `GET /api/apps/:appId/instances/:iid`

响应 data `InstanceView`(无 schedule_kind)。`:iid` 经 `GetInApp` 校验归属。不存在 404。

#### `POST /api/apps/:appId/instances/:iid/stop`

标记 `stopped` + 释放并发槽。响应 data:`{id}`。

#### `POST /api/apps/:appId/instances/:iid/retry`

立即重排 failed/timeout 实例(交 RetryPump)。响应 data:`{id}`。非可重试态 400 `ErrInstanceNotRetryable`。

#### `GET /api/apps/:appId/instances/:iid/logs`

查询:`offset`(默认 0)、`limit`(默认 500)。响应 data:`{lines: [string], total: int}`(按行分页,见 design.md §9)。实例不存在或无日志 404。

---

### Worker `/api/apps/:appId/workers` (AppScope)

#### `GET /api/apps/:appId/workers`

读内存注册表的在线 worker 快照。响应 data:`{list: [WorkerView], count}`。

`WorkerView`:`worker_address`、`protocol`(`http`/`powerjob`)、`tags`、`accept_not_tag_job`、`score`、`cpu_load`、`cpu_processors`、`jvm_max_memory`/`jvm_used_memory`/`jvm_memory_usage`(GB/GB/0~1)、`disk_total`/`disk_used`/`disk_usage`、`last_heartbeat`。

---

## 简化 worker 协议 `/worker/*`(无鉴权)

### `POST /worker/heartbeat`

请求体 `HeartbeatReq`:

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `appName` | string | 是 | 须已注册 |
| `workerAddress` | string | 是 | worker 标识 |
| `systemMetrics` | object | — | 见下 |
| `tags` | []string | — | 标签 |
| `acceptNotTagJob` | bool | — | 是否接收无 tag job |
| `protocol` | string | `http` | `http`/`powerjob` |

`systemMetrics`:`cpuLoad`、`cpuProcessors`、`diskTotal`、`diskUsage`、`diskUsed`、`jvmMaxMemory`、`jvmMemoryUsage`、`jvmUsedMemory`、`extra`、`score`。

响应 data:`{ok: true}`。app 未注册 404。

### `POST /worker/instances/:iid/status`

请求体 `ReportStatusReq`:`{workerAddress, status, result}`。

- `workerAddress` 须与实例绑定的 worker_address 一致,否则 403 `worker 与实例绑定不一致,拒绝状态上报`(防伪造 id 篡改)
- `status` 用领域字符串(`success`/`failed`/`running`/...)
- 实例不存在 404

响应 data:`{ok: true}`。

### `POST /worker/instances/:iid/logs`

请求体 `ReportLogReq`:`{level, message, time}`。`level` 空=`info`;`time` 毫秒戳,<=0 取服务端时间。**始终 200 空体**(实例不存在也静默)。

```bash
# worker 回报成功(假设实例 id=1,worker 自报地址须与派发时绑定的一致)
curl -XPOST http://127.0.0.1:8080/worker/instances/1/status \
  -H 'Content-Type: application/json' \
  -d '{"workerAddress":"127.0.0.1:9001","status":"success","result":"done"}'
```

---

## PowerJob Server 协议 `/server/*`(无鉴权)

让 PowerJob Java worker 不改源码接入。详情见 design.md §6。

| 端点 | 方法 | 请求 | 响应 envelope | data |
|---|---|---|---|---|
| `/server/assert` | GET | query `appName` | ResultDTO | appId(int64) |
| `/server/acquire` | GET | 无 | ResultDTO | server_address(string) |
| `/server/workerHeartbeat` | POST | HeartbeatReq | **200 空体** | — |
| `/server/reportInstanceStatus` | POST | ReportInstanceStatusReq | AskResponse(base64) | base64("null") |
| `/server/reportLog` | POST | LogReportReq | **200 空体** | — |
| `/server/queryJobCluster` | POST | QueryClusterReq | AskResponse(base64) | base64([]workerAddress) |

### `POST /server/workerHeartbeat`

请求体(对齐 PowerJob SystemMetrics):`{appName, workerAddress, systemMetrics, tag, tags, acceptNotTagJob}`。`tag`/`tags` 任一非空(单 tag 折成数组);protocol 固定 `powerjob`。未知 app 静默。

### `POST /server/reportInstanceStatus`

请求体:`{instanceId: FlexInt64, jobId: FlexInt64, instanceStatus: int, result: string}`。`instanceStatus` 须在 `{1,2,3,4,5,9,10}`(见状态码对照),非法码或 `instanceId<=0` 静默忽略。

### `POST /server/reportLog`

请求体:`{instanceLogContents: [{instanceId: FlexInt64, logLevel: int, logContent: string, logTime: int64(ms)}]}`。`logLevel`:1=debug 2=info 3=warn 4=error。始终 200;`instanceId<=0` 脏数据跳过。

### `POST /server/queryJobCluster`

请求体:`{appId: FlexInt64, jobId: FlexInt64}`。响应 data = base64(JSON `[]string`,在线 worker 地址)。失败 `success:false, message:"参数错误"/"app not found"`。

---

## PowerJob OpenAPI `/openApi/*`(无鉴权,18 端点)

让原对接 PowerJob 的业务客户端零改动接入。全部 POST、全部 HTTP 200,响应除 `/runJob2` 用 `PowerResultDTO` 外均 `ResultDTO`。

**appId 取值优先级**:JSON body `appId` → 请求头 `X-POWERJOB-APP-ID` → form/query `appId`。

### App 区

| 端点 | 请求 | 响应 data |
|---|---|---|
| `POST /openApi/assert` | form `appName`(+`password`?) | appId(int64) |

### Job 区

| 端点 | 对应 PowerJob | 请求 | 响应 data |
|---|---|---|---|
| `POST /openApi/saveJob` | SaveJobInfoRequest | JSON `SaveJobReq`(全指针) | jobId(int64) |
| `POST /openApi/copyJob` | — | form `appId`,`jobId` | 新 jobID |
| `POST /openApi/exportJob` | — | form `appId`,`jobId` | SaveJobInfoRequest 形状 |
| `POST /openApi/fetchJob` | fetchJob | form `appId`,`jobId` | JobInfoDTO |
| `POST /openApi/fetchAllJob` | — | form `appId` | []JobInfoDTO(≤10w) |
| `POST /openApi/queryJob` | JobInfoQuery | JSON `JobInfoQuery` | []JobInfoDTO(≤1000, id DESC) |
| `POST /openApi/deleteJob` | — | form `appId`,`jobId` | null |
| `POST /openApi/disableJob` | — | form `appId`,`jobId` | null |
| `POST /openApi/enableJob` | — | form `appId`,`jobId` | null |
| `POST /openApi/runJob` | RunJobRequest | form `appId`,`jobId`,`delayMS`?,`instanceParams`? | instanceID(int64) |
| `POST /openApi/runJob2` | RunJobRequest | JSON `RunJobReq` | instanceID(PowerResultDTO) |

**`SaveJobReq` 字段**(对齐 powerjob-common,全指针):`id`、`jobName`、`jobDescription`、`appId`、`jobParams`、`timeExpressionType`(数字码 1/2/3/4/5 或枚举名 `API`/`CRON`/`FIXED_RATE`/`FIXED_DELAY`/`WORKFLOW`)、`timeExpression`、`startTime`/`endTime`(ms)、`concurrency`、`instanceTimeLimit`(ms→秒存 timeout_sec)、`instanceRetryNum`、`enable`、`tag`。创建时给 `timeExpression` 必须同时给 `timeExpressionType`,否则 fail(防 cron 被静默忽略)。

**`JobInfoQuery` 字段**(JSON,全指针):`idEq`、`jobNameEq`、`jobNameLike`(LIKE %x%)、`tagEq`。

**`runJob`/`runJob2` 的优先级约定**:`instanceParams` 若为 JSON 且含 `priority` 字段(数字或字符串),解出注入实例;否则 0(兼容原协议)。source 分别为 `openapi-runJob` / `openapi-runJob2`,trigger_type=manual。

**`JobInfoDTO`** 关键字段(对齐 `tech.powerjob.common.response.JobInfoDTO`):`id`、`jobName`、`jobDescription`、`appId`、`jobParams`、`timeExpressionType`(1 API/2 CRON/3 FIX_RATE/4 FIX_DELAY)、`timeExpression`、`executeType`(固定 1=STANDALONE)、`processorType`(占位 1=JAVA)、`processorInfo`、`maxInstanceNum`、`concurrency`、`instanceTimeLimit`(ms)、`instanceRetryNum`、`status`(1 正常/2 停止)、`nextTriggerTime`(ms)、`startTime`/`endTime`(ms)、`tag`、`gmtCreate`/`gmtModified`(ms)。

### Instance 区

| 端点 | 对应 PowerJob | 请求 | 响应 data |
|---|---|---|---|
| `POST /openApi/stopInstance` | — | form `instanceId`(+`appId`?) | null |
| `POST /openApi/cancelInstance` | — | form `instanceId` | null |
| `POST /openApi/retryInstance` | — | form `instanceId` | null(非 failed/timeout fail) |
| `POST /openApi/fetchInstanceStatus` | — | form `instanceId` | PowerJob 数字状态码(int) |
| `POST /openApi/fetchInstanceInfo` | — | form `instanceId` | InstanceInfoDTO |
| `POST /openApi/queryInstance` | InstancePageQuery | JSON `InstancePageQuery` | PageResult |

`stop` 与 `cancel` 区别:分别落 `stopped` / `canceled` 两态。

**`InstancePageQuery`** 字段(JSON):`index`(0-based 页码)、`pageSize`(<=0 默认 10)、`instanceIdEq`(精确查,绕过分页)、`jobIdEq`、`appId`(经优先级规则)、`statusIn`(PowerJob 数字码列表,多状态 OR)。

**`PageResult`**:`{index, pageSize, totalPages, totalItems, data: [InstanceInfoDTO]}`。

**`InstanceInfoDTO`**(对齐 `tech.powerjob.common.response.InstanceInfoDTO`):`jobId`、`appId`、`instanceId`、`jobParams`、`instanceParams`、`status`(数字码)、`result`、`expectedTriggerTime`/`actualTriggerTime`/`finishedTime`(ms)、`taskTrackerAddress`、`runningTimes`(=retryIndex+1)、`gmtCreate`/`gmtModified`。

---

## 附录

### 常见错误状态码

| 码 | 场景 |
|---|---|
| 400 | 参数错误 / 业务校验失败(`ErrAppValidate`/`ErrJobValidate`/`ErrInstanceValidate`/`ErrInstanceNotRetryable`) |
| 401 | 未登录 / 会话失效 |
| 403 | 越权(非 admin 访问 admin 资源;app 跨 app;worker 地址不一致) |
| 404 | 资源不存在(app/job/instance/未注册 app) |
| 409 | 冲突(app 名重名 `ErrAppInUse`、管理员用户名重复) |
| 429 | 登录限流 |
| 503 | `/health` DB 不可达;import-powerjob 客户端未装配 |

### 源码索引

- 装配:`main.go`(`buildRouter`、`health`、`bodyLimit`)
- 管理端:`internal/protocol/own/`(`auth.go`、`account.go`、`handler.go`、`dto.go`、`import.go`)
- 简化 worker:`internal/protocol/worker/`(`handler.go`、`dto.go`)
- PowerJob:`internal/protocol/powerjob/`(`handler.go`、`openapi.go`、`wire.go`、`status.go`)
- 鉴权:`internal/auth/`(`session.go`、`login.go`、`middleware.go`)
- 状态码:`internal/domain/instance.go`;PowerJob 数字码:`internal/protocol/powerjob/status.go`
