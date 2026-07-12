# 重构方案:统一领域模型 + worker 派发

> 状态:**已实施**(阶段 0–5 全部落地,旧代码已清理)。本稿为该实现的设计依据。
> 无历史/数据负担,drop 重建。

---

## 1. 目标

把当前"自有 webhook 体系 + PowerJob Java worker 兼容层"两套平行实现,收敛为**一套统一 worker 派发模型**:

- **执行模式唯一**:服务端选一个在线 worker,异步 `POST {jobParams, jobInstanceParams, jobId, jobInstanceId}`,worker 回报状态。砍掉用户自定义 webhook URL/headers/body,也砍掉 PowerJob Java processor/runJob 协议。
- **一套领域模型**:`App`/`Job`/`Instance`,int 自增主键。
- **协议与领域分离**:HTTP 边界做翻译,内部不耦合任何外部协议的数据形状。
- **int 主键 + AppName 唯一**消除 uuid 低效;**webhook 重试改 DB 驱动**消除 `recoverRetries` 补丁;**实例日志落本地文件**按 root 分组。

---

## 2. 执行模型(核心)

```
worker 启动 → 心跳上报 {appName, workerAddress, load, time} (无 token,靠 appId/appName)
                    │
任务到期/手动触发 → 调度器选一个在线 worker(按 load 择优)
                    │
服务端 POST {jobParams, jobInstanceParams, jobId, jobInstanceId} → worker.workerAddress
                    │ (worker ACK=已接收)
实例: queued/dispatched → waiting_receive ──worker回报──→ running ──→ success/failed
                    │                                           │
                    │      worker 失联/超时 ──reaper──→ failed │
                    │                                           │
                    └──── failed 且未达上限 ──→ next_retry_time ──→ RetryPump 重派
```

- **异步派发**:POST 仅交付任务,worker 异步执行后回调上报 status。
- **worker 注册无 token**:靠 appId/appName 标识所属(部署侧靠网络隔离保护这组端点,对齐 PowerJob `/server/*` 的约束)。
- **reaper 兜底**:worker 心跳超时或实例执行超时 → 标记 failed 并触发重派。
- **DB 驱动重试**:failed 写 `next_retry_time`,RetryPump 扫描重派,重启不丢。

---

## 3. 统一领域模型

### 3.1 App

```go
type App struct {
    ID        int64     `gorm:"primaryKey;autoIncrement"`            // app_id:自增,不可用户指定
    AppName   string    `gorm:"type:varchar(128);uniqueIndex;not null"` // 唯一;兼作登录标识与显示名
    Password  string    `gorm:"type:varchar(128)"`                   // bcrypt;管理端应用登录用
    Status    int8      `gorm:"default:1"`                           // 1=启用 0=禁用
    CreatedAt time.Time `gorm:"autoCreateTime"`
    UpdatedAt time.Time `gorm:"autoUpdateTime"`                      // 最后更新时间
}
```

- `ID`(对外即 appId)自增,**不可用户指定**;`AppName` 全局唯一,用户创建时填写,兼作登录名与显示名。
- worker 心跳**不校验 Password**,仅按 AppName 归属(无 token,见 §7);管理端人类/程序登录才用 Password。

### 3.2 Job

```go
type Job struct {
    ID      int64  `gorm:"primaryKey;autoIncrement"`
    AppID   int64  `gorm:"index:idx_app_job;not null"`              // int 外键
    Name    string `gorm:"type:varchar(128);not null"`

    // —— 执行(当前唯一 http 派发;字段保留供未来扩展)——
    ExecuteType string `gorm:"type:varchar(16);not null;default:http"` // http
    JobParams   string `gorm:"type:text"`                             // 任务参数(字符串),随每次执行下发
    Tag         string `gorm:"type:varchar(128)"`                      // 任务标签;worker 匹配用(Instance.Tag 可覆盖)
    TimeoutSec  int                                                    // 实例执行超时(reaper 据此)

    // —— 调度 ——
    ScheduleKind  string  `gorm:"type:varchar(16)"`   // cron | fix_rate | fix_delay | delay | run_at | api
    ScheduleExpr  string  `gorm:"type:varchar(128)"`  // cron 串 / 毫秒数 / run_at 时间
    NextRunTime   *time.Time `gorm:"index"`

    // —— 并发 / 排队 / 重试 ——
    MaxConcurrency   int `gorm:"default:1"`
    MaxWaitSeconds   int `gorm:"default:0"`
    RetryCount       int `gorm:"default:0"`
    RetryIntervalSec int `gorm:"default:0"`
    DefaultPriority  int `gorm:"default:0"`
    Enabled          bool `gorm:"default:true"`

    CreatedAt time.Time `gorm:"autoCreateTime"`
    UpdatedAt time.Time `gorm:"autoUpdateTime"`  // 最后更新时间
}
```

- **不再有** `WebhookURL`/`HTTPMethod`/`Headers`/`Body`/`ProcessorType`/`ProcessorInfo`:执行地址来自 worker 上报,方法固定 POST,请求体固定结构(§7),无自定义头。
- `JobParams`:任务级参数字符串,派发时填入 body。

### 3.3 Instance

```go
type Instance struct {
    ID       int64  `gorm:"primaryKey;autoIncrement"`
    JobID    int64  `gorm:"index;not null"`
    AppID    int64  `gorm:"index;not null"`
    Status   string `gorm:"type:varchar(16);index;default:queued"`  // 见 §5
    TriggerType string `gorm:"type:varchar(16)"`                     // auto | manual | retry
    Priority    int
    RetryIndex  int
    RootInstanceID int64 `gorm:"index;default:0"`                    // 归属首个实例 id;0=自身即首次

    // —— 派发 / 参数 ——
    JobInstanceParams string  `gorm:"type:text"`     // 实例级参数(本次执行特有),随派发下发
    Tag               string  `gorm:"type:varchar(128)"` // 实例标签;派发匹配 worker 用(空则回退 Job.Tag)
    WorkerAddress     string  `gorm:"type:varchar(128)"` // 承接该实例的 worker 地址(派发时绑定)

    // —— 结果 ——
    HTTPStatus   int
    ResponseBody string     `gorm:"type:text"`       // worker ACK/响应
    Result       string     `gorm:"type:text"`       // 错误/结果文案
    NextRetryTime *time.Time `gorm:"index"`          // DB 驱动重试

    TriggerTime time.Time
    StartTime   *time.Time
    EndTime     *time.Time
    DurationMS  int64
    CreatedAt time.Time `gorm:"autoCreateTime"`
    UpdatedAt time.Time `gorm:"autoUpdateTime"`  // 最后更新时间
}
```

- `RootInstanceID`:重试仍新开实例,但同一逻辑触发的所有重试共享 root(永远指向首个实例 id,非上个);按 `ID` 自增排序即时间序,无需链表字段。赋值:`rootOrSelf(原) = 原.RootInstanceID==0 ? 原.ID : 原.RootInstanceID`。

---

## 4. 实例日志:本地文件(不建表)

**取消 `app_task_log` / `powerjob_instance_log` 两张表**。每实例一个文件,调度事件与 worker 上报日志混写,呈现一次执行的完整时间线。

**文件**:
- 路径:`{log.dir}/instances/{appID}/{instanceID}_{rootInstanceID}.log`
- 命名 `appid_实例id_归属实例id`;归属为首个实例 id,无重试(自身即首次)时为 `0`。
- 同 root 的文件按 `instanceID` 排序 = 一次触发的完整时间线(含重试)。

**内容(同文件按时间追加)**:

| 事件 | 埋点 | 示例 |
|---|---|---|
| 实例创建+快照 | 创建实例时 | `[ts] CREATE instance={...} job={name,jobParams,schedule…}` |
| 调度认领 | AdvanceNextRun 后 | `[ts] SCHEDULE next_run=...` |
| 选 worker + 派发 | Executor | `[ts] DISPATCH worker=1.2.3.4:9000 load=0.3 body={...}` |
| 状态变更 | 任一状态写入 | `[ts] STATUS waiting_receive→running` / `→success` |
| worker 上报日志 | reportLog | `[ts] [WARN] <worker 原始消息>`(保留其时间戳/级别) |
| 重试调度 | RetryPump | `[ts] RETRY scheduled_at=... retry_index=1` |
| 失败转移 | reaper | `[ts] REAP reason=worker 失联…` |

**组件**:`InstanceLogger`(替代 LogStore)
```go
type InstanceLogger interface {
    Append(appID, instanceID, rootInstanceID int64, e LogEntry)
    Read(appID, instanceID, rootInstanceID int64, q LogQuery) (lines []string, total int, err error)
}
```
- per-instance `sync.Mutex` 保证多 goroutine 写有序;按 mtime + `log.instance_retention_days` 清理。
- `GET .../instances/:iid/logs` 读单实例日志文件(按行 offset/limit 分页);不提供程序内聚合——同链路串联靠文件名格式 `{id}_{root}` 由 ssh/外部程序按名分析。

---

## 5. 状态机(9 态)

| 状态 | 含义 |
|---|---|
| `queued` | 排队(并发超限) |
| `waiting_receive` | 已派发,等 worker 接收/拉起 |
| `running` | 运行中(worker 上报) |
| `success` | 成功 |
| `failed` | 失败(worker 失联/派发失败/重启清理) |
| `timeout` | 执行超时(reaper 据 job.TimeoutSec) |
| `skipped` | 跳过(排队等待超时;⚠ 当前未实现,预留) |
| `canceled` | 取消 |
| `stopped` | 手动取消 |

- 不设 `pending`:实例创建即 `queued`/派发即 `waiting_receive`。执行超时独立为 `timeout`(区别于 `failed`);`failed`/`timeout` 均可重试,`skipped` 不可。
- 典型流转:`queued → waiting_receive → running → success`;reaper 把卡在 `waiting_receive`/`running` 的按成因标 `failed`(失联/未绑定)或 `timeout`(执行超 `TimeoutSec`)重派。终态不可回退(worker 迟到上报忽略)。

---

## 6. 调度类型

| `ScheduleKind` | `ScheduleExpr` | 现有来源 |
|---|---|---|
| `cron` | "0 9 * * *" | task_type=cron / PowerJob CRON |
| `fix_rate` | "5000"(ms) | PowerJob FIX_RATE |
| `fix_delay` | "5000"(ms) | PowerJob FIX_DELAY |
| `delay` | "300"(s) | task_type=delay |
| `run_at` | RFC3339 | task_type=normal(带 RunAt) |
| `api` | — | task_type=normal / PowerJob API |

`schedtime.ComputeNextRun` 按 `ScheduleKind` 分派,消除 `task_type` / `time_expression_type` 两套解析。

---

## 7. Worker 接入与派发协议

两套 worker 接入并存,统一注册表(按 appName),均**无 token**(靠 appName 归属 + `/server/*`、`/worker/*` 网络隔离):
- **PowerJob 协议** `/server/*`:标准 PowerJob Java worker 不改源码接入(assert/acquire/heartbeat/reportStatus/reportLog),派发用官方 `runJob`(`ServerScheduleJobReq`)。
- **简化 http 协议** `/worker/*`:通用 HTTP worker,派发用固定 body `{jobParams, jobInstanceParams, jobId, jobInstanceId}`。

### 7.1 心跳(两协议共用数据形状)

```
POST /worker/heartbeat          (简化协议)
POST /server/workerHeartbeat    (PowerJob 协议,字段对齐 PowerJob)
{
  "appName": "...",
  "workerAddress": "1.2.3.4:9000",
  "systemMetrics": {
      "cpuLoad": 2.4258, "cpuProcessors": 8, "diskTotal": 233.4691, "diskUsage": 0.0446,
      "diskUsed": 10.4179, "jvmMaxMemory": 3.5557, "jvmMemoryUsage": 0.1083,
      "jvmUsedMemory": 0.385, "extra": "", "score": 11
  },
  "tags": ["gpu", "highmem"],     // 可选;worker 能承接的标签
  "acceptNotTagJob": true,        // 可选;是否接受无 tag 任务
  "protocol": "http|powerjob"     // 由接入端点隐式区分(/worker/*→http, /server/*→powerjob)
}
```
- 注册表(内存,不入库):`appName → [{workerAddress, systemMetrics, tags, acceptNotTagJob, protocol, lastHeartbeat}]`;心跳超时剔除。

### 7.2 worker 选址(tag 匹配 + score 择优)

```
jobInstanceTag = Instance.Tag != "" ? Instance.Tag : Job.Tag
cantp-jobtes = [ w ∈ online(appName) | matchTag(jobInstanceTag, w) ]
matchTag(t, w) = w.acceptNotTagJob || t ∈ w.tags || (t == "" && len(w.tags) == 0)
pick = cantp-jobtes 按 systemMetrics.score 降序 取首   // 无候选 → 实例 failed(或排队重试)
```

### 7.3 派发(按 worker.protocol)

- `http`:`POST {workerAddress}/run` body `{jobParams, jobInstanceParams, jobId, jobInstanceId}`
- `powerjob`:`POST {workerAddress}/worker/runJob` body `ServerScheduleJobReq`(`protocol/powerjob` translator 构造,对齐官方多语言 HTTP 规范;非 akka 时代的 /taskTracker/runJob)

2xx = 已接收(实例 → `waiting_receive`);非 2xx/网络错 → `failed`。worker 异步执行后回报推进终态。

### 7.4 worker 回报(无 token)

```
POST /worker/instances/:iid/status   {status, result}                      (简化协议,领域 string)
POST /server/reportInstanceStatus    {instanceId, instanceStatus, ...}     (PowerJob 协议,官方数字码)
POST /worker/instances/:iid/logs     {level, message, time}
POST /server/reportLog               {instanceLogContents:[...]}
```
- 简化协议用领域 string 状态(§5);PowerJob 协议用官方数字码,`protocol/powerjob` adapter 做 int↔string 映射。终态不可回退守护。日志经 `InstanceLogger.Append` 落文件。

---

## 8. 调度器 + reaper + 重试

统一调度器(合并两套旧循环):
- `Run`:扫描 `Job.NextRunTime` 到期 → 认领(`AdvanceNextRun` 乐观锁)→ 选 worker → 派发(§7.2)。
- 任务级并发槽随实例生命周期持有(本次修复已对齐):派发后绑定实例,worker 回报终态/reaper 转移才释放。`MaxConcurrency` 按"在跑实例数"计数。
- 手动触发超限 → memQueue + DB 两层排队,`MaxWaitSeconds` 超时落 `skipped`。
- **reaper**:扫 `waiting_receive`/`running`,worker 心跳超时/未绑定 → `failed`;执行超 `TimeoutSec` → `timeout`;均触发重试。
- **RetryPump**:扫 `failed`/`timeout` 且 `next_retry_time` 到期,按 `rootOrSelf` 建重试实例重派。删 `recoverRetries`(DB 不丢)。

---

## 9. 管理端鉴权(账户 / 登录会话)

- **管理员账户**:admin_user 表(首次启动 seed admin/admin123,Web 可改用户名/密码;**已迁出 config/env**,
  `TP_JOB_ADMIN_PASSWORD` 等环境变量不再生效)。release 模式由 `config.release.yaml` 控制(不再经 env 覆盖 mode)。
- **应用账户**:`app` 表(AppName + Password)。
- `POST /api/auth/login {ident, password}` → session token;先匹配 admins(管理员)否则匹配 `app.AppName`(应用)。
- 后续 `Authorization: Bearer <token>`;`SessionAuth()` 解析 `{role, appID?}`。

| 操作 | 管理员 | 管理员切换 app | 应用登录 |
|---|---|---|---|
| 新增/删除 app | ✓ | ✗ | ✗ |
| 修改 app | ✓ | ✓ | ✓(仅自己) |
| 查看 app | ✓ 全部 | ✓ 当前 | ✓ 仅自己 |
| 切换 app | ✓ | ✓ | ✗ |
| 任务/实例/日志 CRUD | ✓(切到 app 后) | ✓ | ✓(仅自己) |

- **worker 心跳 `/worker/*` 不走登录 token**(§7.1),靠 appName + 网络隔离。
- 前端:登录页 + 顶部应用切换器(admin 可见)+ 按 role 显隐"新增/删除 app"。

---

## 10. 目录结构

```
internal/
  domain/      App/Job/Instance + 状态常量(无 Log 表)
  store/       统一仓储(int 外键)
  instancelog/ 实例日志:文件 + InstanceLogger + per-instance 锁 + 清理
  service/     领域服务
  scheduler/   统一调度器 + Executor + reaper + RetryPump + 并发槽/排队
  workerreg/   worker 心跳注册表(appName→address+load)
  protocol/
    own/       管理端 REST(/api/*):dto + translator
    worker/    简化 worker 协议(/worker/*):心跳/回报 dto + translator
    powerjob/  PowerJob 协议(/server/* + /api/poweradmin/*):wire + 状态映射 + translator
  api/         gin 路由 + handler(薄)
  config/ logger/ schedtime/
```

旧 `internal/powerjob` 包解散;`internal/worker`(旧展示用注册表)并入 `workerreg`。

---

## 11. 迁移

✅ 无历史/数据负担:直接 drop 旧表 + AutoMigrate 建新表,不写搬运脚本。旧表(`app`/`app_task`/`app_task_instance`/`app_task_log`/`powerjob_*`)随阶段推进删除。每阶段以"编译 + 全量测试 + `go vet` 干净"为门槛。

---

## 12. 已决 / 待定 / 风险

**已决**:
- ✅ 统一 worker 派发(异步);自有 webhook 双轨砍掉,PowerJob `/server/*` 协议兼容**保留**
- ✅ 无数据负担 → drop 重建
- ✅ int 自增 PK + AppName 唯一(不可指定)
- ✅ Job 去 webhook URL/method/headers/body,加 `JobParams` + `Tag`;Instance 加 `JobInstanceParams` + `Tag`
- ✅ 简化协议派发 body 固定 `{jobParams, jobInstanceParams, jobId, jobInstanceId}`(PowerJob 协议仍用 runJob)
- ✅ worker 心跳无 token,含 `systemMetrics`(PowerJob SystemMetrics)+ `tags[]` + `acceptNotTagJob`;选址 tag 匹配 + score 择优
- ✅ 8 态状态机;重试改 DB 驱动,删 `recoverRetries`
- ✅ 实例日志落文件 + `RootInstanceID` 分组
- ✅ 管理端账户登录 + 权限矩阵;前端管理台同步改造
- ✅ Job/Instance 显式 `UpdatedAt`

**待定**:
1. PowerJob worker 与简化 http worker 在管理台是否区分展示/分别管理(当前统一注册表,UI 可只列在线 worker + 其 protocol/tags/score)。

**主要风险**:
- 异步派发 + reaper + 槽语义是 correctness 密集区,本次新增的 `TestSchedulerHoldsSlotAfterPush` 等用例作回归基线。
- worker 协议是新定义的,需配套写一个示例 worker(echo 一下 body 即可)用于联调与文档。

---

## 13. 阶段任务清单

> 每阶段独立验证,门槛:编译 + 全量测试 + `go vet`。

### 阶段 0:领域骨架
- [ ] `internal/domain`:App/Job/Instance + 8 状态常量 + 辅助(Terminal/Valid)
- [ ] `scheduler.Executor` 接口 + `DispatchBody`
- [ ] 旧代码原样运行

### 阶段 1:统一 model 落库 + 日志/注册骨架
- [ ] domain model 经 store AutoMigrate(drop 旧表含 log 表)
- [ ] `Job`(`JobParams`/`Tag`/`ExecuteType=http`/无 webhook 字段)、`Instance`(`JobInstanceParams`/`Tag`/`WorkerAddress`/`RootInstanceID`)、`App`(int PK + AppName 唯一)
- [ ] `instancelog.InstanceLogger`(文件 `{appID}/{instanceID}_{rootInstanceID}.log` + per-instance 锁)
- [ ] `workerreg.Registry`(appName→{address, systemMetrics, tags, acceptNotTagJob, protocol, heartbeat})

### 阶段 2:调度器 + 异步派发
- [ ] 合并为单 scheduler,`WorkerDispatchExecutor`:tag 匹配 + score 选 worker → 按 worker.protocol 派发(http 用 `{jobParams,jobInstanceParams,jobId,jobInstanceId}`,powerjob 用 runJob) → 实例 waiting_receive
- [ ] 并发槽随实例生命周期(复用本次修复成果);memQueue/DB 两层排队
- [ ] reaper(心跳超时/`TimeoutSec`)、RetryPump(`next_retry_time`,`rootOrSelf`)
- [ ] 删 `recoverRetries`;调度事件埋点写实例日志

### 阶段 3:协议层
- [ ] `protocol/own`(/api/*):translator + dto
- [ ] `protocol/worker`:`/worker/heartbeat`(含 systemMetrics/tags)、`/worker/instances/:iid/status`、`/logs`(无 token)
- [ ] `protocol/powerjob`:`/server/*`(assert/acquire/heartbeat/reportStatus/reportLog)+ `/api/poweradmin/*`;int↔string 状态映射 + runJob body 翻译
- [ ] `/api/.../instances/:iid/logs` 改读 `InstanceLogger.Read`(单实例,offset/limit 分页)

### 阶段 4:管理端账户/登录
- [x] 管理员账户 admin_user 表(seed admin/admin123,Web 可改;已迁出 config/env)+ release 防呆(config.release.yaml)
- [ ] `POST /api/auth/login` + session token + `SessionAuth()`
- [ ] 权限矩阵(新增/删除 app 仅 admin;资源 CRUD 越权校验)+ 管理员切换 app
- [ ] 前端:登录页 + 应用切换器 + 按 role 显隐

### 阶段 5:清理
- [ ] 移除 X-Admin-Token / Basic Auth、`/server/*`、`internal/powerjob`、旧 model/表
- [ ] 重写 README(新鉴权 + worker 协议 + 状态机);附示例 worker

---

## 14. 小结

收敛为**一套领域模型 + 统一 worker 派发**:worker 无 token 心跳(appName+address+load)→ 调度器按负载选 worker → 异步 POST 固定 body → worker 回报 → reaper/RetryPump 兜底;实例日志落文件按 root 分组;管理端账户登录 + 权限矩阵。砍掉自有 webhook 双轨与 PowerJob Java 协议层,int 主键 + AppName 唯一,重试改 DB 驱动。后续扩展执行模式只需在 `ExecuteType` 加分支,不动领域层。

确认本稿后即可从**阶段 0** 开始动手。
