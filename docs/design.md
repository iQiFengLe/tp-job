# tp-job 设计文档

> 本文是 tp-job 的核心设计参考,聚焦领域模型、执行模型、状态机、Worker 协议、调度与重试、
> 回调、实例日志、鉴权的设计依据。**使用方式 / 部署 / API 列表见根目录 README**;
> 模型与字段的权威定义以 `internal/` 代码为准,此处展示设计骨架与关键决策。

---

## 1. 设计目标

把"自有 webhook 体系"与"PowerJob Java worker 兼容层"两套平行实现,收敛为**一套统一 worker 派发模型**:

- **执行模式唯一**:服务端选一个在线 worker,异步 `POST {jobParams, jobInstanceParams, jobId, jobInstanceId}`,worker 回报状态。砍掉用户自定义 webhook URL/headers/body,也砍掉 PowerJob Java processor/runJob 之外的协议负担。
- **一套领域模型**:`App` / `Job` / `Instance`,int 自增主键 + AppName 唯一。
- **协议与领域分离**:HTTP 边界做翻译,内部不耦合任何外部协议的数据形状。
- int 主键 + AppName 唯一消除 uuid 低效;重试改 **DB 驱动**(重启不丢);实例日志落本地文件按 root 分组。

---

## 2. 执行模型

```
worker 启动 → 心跳 {appName, workerAddress, systemMetrics, tags} (无 token,靠 appName 归属)
                    │
任务到期/手动触发 → 调度器认领(AdvanceNextRun 乐观锁)→ 选 worker(tag 匹配 + score 择优)
                    │
服务端 POST {jobParams, jobInstanceParams, jobId, jobInstanceId} → worker.workerAddress
                    │ (worker ACK 2xx = 已接收 → 实例 waiting_receive)
实例: queued ──派发──→ waiting_receive ──worker回报──→ running ──→ success / failed / timeout
                    │                                           │
                    │      worker 失联 ──reaper──→ failed        │
                    │      执行超时 ──reaper──→ timeout          │
                    └──── failed/timeout 且未达上限 ──→ next_retry_time ──→ RetryPump 重派
```

- **异步派发**:POST 仅交付任务(2xx = 已接收),worker 异步执行后回调上报状态推进终态。
- **worker 注册无 token**:靠 appId/appName 标识所属(部署侧靠网络隔离保护这组端点)。
- **任务级并发槽**随实例生命周期持有:派发后绑定实例,worker 回报终态或 reaper 转移才释放;`MaxConcurrency` 按"在飞实例数"计数。
- **reaper 兜底**:worker 心跳超时或实例执行超时 → 标记 `failed`/`timeout` 并触发重派。
- **DB 驱动重试**:`failed`/`timeout` 写 `next_retry_time`,RetryPump 扫描重派,重启不丢。
- **at-least-once**:worker 迟到回报可能与重派实例并存,业务侧需自行幂等。

---

## 3. 领域模型

### 3.1 App

```go
type App struct {
    ID        int64     `gorm:"primaryKey;autoIncrement"`                // app_id:自增,不可用户指定
    AppName   string    `gorm:"type:varchar(128);uniqueIndex;not null"`  // 全局唯一;兼作登录标识与显示名
    Password  string    `gorm:"type:varchar(128)"`                       // bcrypt;管理端应用登录用
    Status    int8      `gorm:"default:1"`                               // 1=启用 0=禁用
    CreatedAt time.Time `gorm:"autoCreateTime"`
    UpdatedAt time.Time `gorm:"autoUpdateTime"`
}
```

- `ID`(对外即 appId)自增,**不可用户指定**;`AppName` 全局唯一,用户创建时填写,兼作登录名与显示名。
- worker 心跳**不校验 Password**,仅按 AppName 归属(无 token,见 §6);管理端人类/程序登录才用 Password。

### 3.2 Job

```go
type Job struct {
    ID      int64  `gorm:"primaryKey;autoIncrement"`
    AppID   int64  `gorm:"index:idx_app_job;not null"`                   // int 外键
    Name    string `gorm:"type:varchar(128);not null"`

    ExecuteType string `gorm:"type:varchar(16);not null;default:http"`  // 当前唯一 http;字段保留供未来扩展
    JobParams   string `gorm:"type:text"`                                // 任务级参数,随每次执行下发
    Tag         string `gorm:"type:varchar(128)"`                        // 任务标签;worker 匹配用(Instance.Tag 可覆盖)
    TimeoutSec  int                                                     // 实例执行超时(reaper 据此)

    ScheduleKind string  `gorm:"type:varchar(16)"`   // cron | fix_rate | fix_delay | delay | run_at | api
    ScheduleExpr string  `gorm:"type:varchar(128)"`  // cron 串 / 毫秒数 / run_at 时间
    NextRunTime  *time.Time `gorm:"index"`

    MaxConcurrency   int `gorm:"default:1"`
    MaxWaitSeconds   int `gorm:"default:0"`
    RetryCount       int `gorm:"default:0"`
    RetryIntervalSec int `gorm:"default:0"`
    DefaultPriority  int `gorm:"default:0"`
    Enabled          bool `gorm:"default:true"`

    CreatedAt time.Time `gorm:"autoCreateTime"`
    UpdatedAt time.Time `gorm:"autoUpdateTime"`
}
```

- **不再有** `WebhookURL`/`HTTPMethod`/`Headers`/`Body`/`ProcessorType`/`ProcessorInfo`:执行地址来自 worker 上报,方法固定 POST,请求体固定结构(§6.3),无自定义头。
- Job 另可配 `callback_url` 等回调参数:实例状态变化时由 `CallbackPump` 通知对端(见 §8)。

### 3.3 Instance

```go
type Instance struct {
    ID       int64  `gorm:"primaryKey;autoIncrement"`
    JobID    int64  `gorm:"index;not null"`
    AppID    int64  `gorm:"index;not null"`
    Status   string `gorm:"type:varchar(16);index;default:queued"`  // 见 §4
    TriggerType string `gorm:"type:varchar(16)"`                    // auto | manual | retry
    Priority    int
    RetryIndex  int
    RootInstanceID int64 `gorm:"index;default:0"`                   // 归属首个实例 id;0=自身即首次

    JobInstanceParams string `gorm:"type:text"`                     // 实例级参数(本次执行特有),随派发下发
    Tag               string `gorm:"type:varchar(128)"`             // 实例标签;派发匹配 worker 用(空则回退 Job.Tag)
    WorkerAddress     string `gorm:"type:varchar(128)"`             // 承接该实例的 worker 地址(派发时绑定)

    HTTPStatus   int
    ResponseBody string     `gorm:"type:text"`                      // worker ACK/响应
    Result       string     `gorm:"type:text"`                      // 错误/结果文案
    NextRetryTime *time.Time `gorm:"index"`                         // DB 驱动重试

    TriggerTime time.Time
    StartTime   *time.Time
    EndTime     *time.Time
    DurationMS  int64
    CreatedAt time.Time `gorm:"autoCreateTime"`
    UpdatedAt time.Time `gorm:"autoUpdateTime"`
}
```

- `RootInstanceID`:重试仍新开实例,但同一逻辑触发的所有重试共享 root(永远指向首个实例 id,非上个);按 `ID` 自增排序即时间序,无需链表字段。
  赋值规则:`rootOrSelf(x) = x.RootInstanceID == 0 ? x.ID : x.RootInstanceID`。

---

## 4. 状态机(9 态)

| 状态 | 含义 |
|---|---|
| `queued` | 排队(并发超限,或 worker 回报解绑回归,见 §6.5) |
| `waiting_receive` | 已派发,等 worker 接收/拉起 |
| `running` | 运行中(worker 上报) |
| `success` | 成功 |
| `failed` | 失败(worker 失联/派发失败/重启清理) |
| `timeout` | 执行超时(reaper 据 `TimeoutSec`;与 `failed` 同样可重试) |
| `skipped` | 跳过(排队等待超时;⚠ 当前未实现,预留——无代码路径产出此态,且不可重试) |
| `canceled` | 取消 |
| `stopped` | 手动停止 |

- 不设 `pending`:实例创建即 `queued`,派发即 `waiting_receive`。执行超时独立为 `timeout`(区别于 `failed`)。
- 典型流转:`queued → waiting_receive → running → success`。
- **终态不可回退**:`success`/`failed`/`timeout`/`canceled`/`stopped` 等终态受 `WHERE status NOT IN (terminal)` 守护,worker 迟到上报一律忽略。终态集由 `domain.TerminalStatuses()` 提供(共 6 个)。

---

## 5. 调度类型

| `ScheduleKind` | `ScheduleExpr` | 说明 |
|---|---|---|
| `cron` | `"0 9 * * *"` | 标准 5 段 cron |
| `fix_rate` | `"5000"`(ms) | 固定频率 |
| `fix_delay` | `"5000"`(ms) | 固定延迟(上次完成后再计时) |
| `delay` | `"300"`(s) | 创建后延迟触发 |
| `run_at` | RFC3339 | 一次性定时 |
| `api` | — | 仅 API/手动触发,无自动调度 |

`schedtime.ComputeNextRun` 按 `ScheduleKind` 分派推算 `NextRunTime`,调度器据此认领到期任务。

---

## 6. Worker 接入与派发协议

两组端点并存,共用同一注册表(按 appName),均**无 token**(靠 appName 归属 + `/server/*`、`/worker/*` 网络隔离):

- **PowerJob 协议** `/server/*`:遵循 PowerJob 字段约定的**自研 http worker** 接入(心跳/状态/日志字段对齐 `SystemMetrics` 与官方数字状态码)。⚠ 官方 Java worker 的心跳/上报走 **AKKA**(`powerjob-remote`)而非 HTTP,不会调用 `workerHeartbeat`/`reportInstanceStatus`/`reportLog`/`queryJobCluster`——这组端点是 tp-job 为自研 worker 提供的 HTTP 兼容层;仅 `assert`/`acquire` 与原版 HTTP 一致(Worker 启动发现 Server)。派发用 `runJob`(`ServerScheduleJobReq`,server → worker)。
- **简化 http 协议** `/worker/*`:通用 HTTP worker,派发用固定 body `{jobParams, jobInstanceParams, jobId, jobInstanceId}`。

### 6.1 心跳(两协议共用数据形状)

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
- 多语言 worker(.NET/Python 等)上报的数值字段类型常常不一致(同一 worker 有时把 id 发成字符串、有时发成数字),协议层 DTO 经 `wire.FlexInt64` 兼容两种写法,避免 Go `encoding/json` 严格匹配导致置零。

### 6.2 选址(tag 匹配 + score 择优)

```
jobInstanceTag = Instance.Tag != "" ? Instance.Tag : Job.Tag
candidates     = [ w ∈ online(appName) | matchTag(jobInstanceTag, w) ]
matchTag(t, w) = w.acceptNotTagJob || t ∈ w.tags || (t == "" && len(w.tags) == 0)
pick           = candidates 按 systemMetrics.score 降序取首
```

**无候选的三态语义**(派发层 `dispatchToWorker`,与 §7 reaper 的 warmup 守卫对称):

- **app 无在线 worker**(registry 空:重启窗口 / worker 全挂):**不判 failed**——实例保持 `queued`,退避 ~1s 重入派发队列,worker 上线即命中;不消耗 `RetryCount`("无 worker"是"尚未轮到派",非"派发失败")。
- **有在线 worker 但 tag 全不匹配**(配置问题):`failed` 并衔接 RetryPump(反馈 `job.tag` / `worker.tags` 配错)。
- **选中**:即绑 `MarkDispatched → Send`(§6.3)。

> 即"无 worker 可派"与"有 worker 但都不合适"是两回事:前者退避保持 `queued`,后者才失败。

### 6.3 派发(按 worker.protocol)

- `http`:`POST {workerAddress}/run` body `{jobParams, jobInstanceParams, jobId, jobInstanceId}`
- `powerjob`:`POST {workerAddress}/worker/runJob` body `ServerScheduleJobReq`(`protocol/powerjob` translator 构造,对齐官方多语言 HTTP 规范)

2xx = 已接收(实例 → `waiting_receive`);非 2xx/网络错 → `failed`。worker 异步执行后回报推进终态。

### 6.4 回报(无 token)

```
POST /worker/instances/:iid/status   {status, result}                      (简化协议,领域 string)
POST /server/reportInstanceStatus    {instanceId, instanceStatus, ...}     (PowerJob 协议,官方数字码)
POST /worker/instances/:iid/logs     {level, message, time}
POST /server/reportLog               {instanceLogContents:[...]}
```

- 简化协议用领域 string 状态(§4);PowerJob 协议用官方数字码,`protocol/powerjob` adapter 做 int↔string 映射。
- 简化协议回报带 `workerAddress`,须与实例绑定一致,防伪造 id 篡改。
- **worker 收到任务后应及时回报 `running`**:作为"已接收并开始执行"的信号,支撑 reaper 的 `waiting_receive` 接收超时判定(§7)——区分"在正常执行"与"卡死未接收"。running 丢失会致接收超时误杀重派 → 重复执行,故须可靠上报(退避重试 + 幂等:终态守护 + `running→running` 无害)。
- 终态不可回退守护;日志经 `InstanceLogger.Append` 落文件(§9)。

### 6.5 worker 解绑与重派

worker 处理任务时若遇资源不足等,可回报"我无法处理,请重新调度":

- PowerJob 协议:`instanceStatus = 1`(`WAITING_DISPATCH`)
- 简化协议:`status = "queued"`

服务端 `UpdateResult` 收到此语义时:

1. 状态置 `queued`
2. **清空 `worker_address`**(解除绑定)
3. **清空 `start_time`**(重置派发时间)
4. 保留 `result`(记录原因,如"资源不足,请重新调度")

整条更新受终态守护(`WHERE status NOT IN terminal`),已终态实例不会被回退。

**重派路径**:

- **手动触发实例**:解绑后回到 `queued`,仍在优先队列中的项会在派发前重查 DB,发现 `worker_address` 已清空即重新走 `PickWorker → MarkDispatched → Send`,可能选到不同 worker。
- **定时触发实例**:当前**不会自动重派**。建议为定时任务配 `RetryCount > 0`,或由 reaper 扫描"`queued` 且 `worker_address` 为空且停留超时"的实例标 `failed` 触发重试。
- **迟到回报**:worker 已被 reaper 标 `failed` 并重试后,迟到的解绑回报会被终态守护拒绝(`failed` 是终态)。

---

## 7. 调度器 / reaper / 重试

统一调度器(合并了历史两套循环):

- `Run`:扫描 `Job.NextRunTime` 到期 → 认领(`AdvanceNextRun` 乐观锁)→ 选 worker → 派发(§6.2)。
- **任务级并发槽**随实例生命周期持有:派发后绑定,worker 回报终态或 reaper 转移才释放。
- 手动触发超限 → 内存队列 + DB 两层排队,`MaxWaitSeconds` 超时落 `skipped`。
- **优先级**:push 架构下优先级唯一作用域=派发顺序(手动队列按 `priority` desc, 同级 FIFO)。实例一旦 `waiting_receive`/`running` 已 POST 出去调整无意义,故仅 `queued` 实例可调(`POST /api/apps/:appId/instances/:iid/priority`):经指针堆 `heap.Fix` 即时重排内存队列 + 落 DB(重启 `RecoverQueued` 据此重排)。定时触发不参与优先级(走任务级串行 `tryAcquire`)。
- **派发层"无在线 worker"语义**(与 reaper warmup 对称,§6.2):app 无在线 worker(重启窗口 / 全挂)时实例保持 `queued` 退避重入队,**不判 failed、不消耗 `RetryCount`**;仅有在线但 tag 全不匹配才 failed。重启被 `RecoverQueued` 恢复的 queued 实例据此不被批量误杀。
- **reaper**:扫 `waiting_receive`/`running` 做失败转移,均触发重试:
  - **启动 warmup 守护**:`worker.warmup_seconds`(默认 30s)窗口内**跳过"worker 失联"判定**,给重启后 worker 重新心跳注册留时间,避免误杀重启前在飞的实例。
  - worker 心跳超时(warmup 后)或实例未绑定 worker → `failed`。
  - **`waiting_receive` 接收超时**:已派发但 worker 迟迟不进 `running`(繁忙/卡住/上报丢失),超 `worker.receive_timeout_seconds`(默认 60s;0=关闭,兼容不报 running 的旧 worker)即 `failed` 重派,不再等满 `TimeoutSec`。前提:worker 收到任务及时回报 `running`(§6.4)。
  - 执行超 `TimeoutSec` → `timeout`。
- **RetryPump**:扫 `failed`/`timeout` 且 `next_retry_time` 到期,按 `retryIndex+1` 建重试实例重派(`RootInstanceID` 指向首个实例)。DB 驱动,重启不丢。

---

## 8. 实例状态变更回调

Job 配 `callback_url` 时,实例每次状态变化(派发 / 运行 / 终态 / 重试)由 `CallbackPump` POST 通知对端,payload 为事件瞬间快照。投递语义为**至少一次**(at-least-once):

- 失败指数退避重试(`backoff_base_sec` → `backoff_max_sec`),达 `max_attempts` 仍不成功置 `dead`。
- **接收方必须幂等**:对端已返回 2xx 但本地记账(`MarkSent`)因 DB 瞬时故障失败时,pump 会退避后重投,同一事件可能被投递多次。用请求头 `X-TaskSchedule-Event-ID`(`cb-<callbackId>`)去重。
- 极端情况(DB 持续故障)下,一个实际已送达的事件可能重投至上限被标 `dead`——`dead` 表示"本地放弃投递",不等同于"对端从未收到",排查需结合对端日志。
- callback URL 可信性靠部署侧网络隔离(同 `/server/*`、`/worker/*`);禁重定向防 302 诱导。
- `retention_days` 控制已终态(`sent` / `dead`)回调记录的保留期,超期自动清理;`pending` 永不删(未投递保证)。

---

## 9. 实例日志:本地文件(不建表)

**不设日志表**。每实例一个文件,调度事件与 worker 上报日志混写,呈现一次执行的完整时间线。

**文件**:

- 路径:`{log.dir}/instances/{appID}/{instanceID}_{rootInstanceID}.log`
- 命名即 `{instanceID}_{rootInstanceID}`;归属为首个实例 id,无重试(自身即首次)时为 `0`。
- 同 root 的文件按 `instanceID` 排序 = 一次触发的完整时间线(含所有重试)。

**内容(同文件按时间追加)**:

| 事件 | 埋点时机 | 示例 |
|---|---|---|
| 实例创建 + 快照 | 创建实例时 | `[ts] CREATE instance={...} job={name,jobParams,schedule…}` |
| 调度认领 | `AdvanceNextRun` 后 | `[ts] SCHEDULE next_run=...` |
| 选 worker + 派发 | Executor | `[ts] DISPATCH worker=1.2.3.4:9000 load=0.3 body={...}` |
| 状态变更 | 任一状态写入 | `[ts] STATUS waiting_receive→running` / `→success` |
| worker 上报日志 | reportLog | `[ts] [WARN] <worker 原始消息>`(保留其时间戳/级别) |
| 重试调度 | RetryPump | `[ts] RETRY scheduled_at=... retry_index=1` |
| 失败转移 | reaper | `[ts] REAP reason=worker 失联…` |

**接口**:

```go
type InstanceLogger interface {
    Append(appID, instanceID, rootID int64, e LogEntry)
    Read(appID, instanceID, rootID int64, q LogQuery) (lines []string, total int, err error)
}
```

- per-instance `sync.Mutex` 保证多 goroutine 写有序;按 mtime + `log.instance_retention_days` 清理(0 = 不清理)。
- `GET .../instances/:iid/logs` 读单实例日志文件(按行 offset/limit 分页);**不在程序内聚合**——同链路串联靠文件名格式 `{id}_{root}`,由外部工具按名分析。

---

## 10. 管理端鉴权

- **管理员账户**:`admin_user` 表(首次启动 seed `admin/admin123`,Web 可改用户名/密码)。**不走 config/env**——`TP_JOB_ADMIN_PASSWORD` 等环境变量不生效。release 模式由 `config.release.yaml` 控制(不再经 env 覆盖 mode)。
- **应用账户**:`app` 表(AppName + Password)。worker 心跳不校验密码(靠 appName + 网络隔离)。
- `POST /api/auth/login {ident, password}` → session token;先匹配管理员用户名,否则匹配 `app.AppName`。
- 后续 `Authorization: Bearer <token>`;`SessionAuth()` 解析 `{role, appID?}`。

| 操作 | 管理员 | 管理员切换 app | 应用登录 |
|---|---|---|---|
| 新增/删除 app | ✓ | ✗ | ✗ |
| 修改 app | ✓ | ✓ | ✓(仅自己) |
| 查看 app | ✓ 全部 | ✓ 当前 | ✓ 仅自己 |
| 切换 app | ✓ | ✓ | ✗ |
| 任务/实例/日志 CRUD | ✓(切到 app 后) | ✓ | ✓(仅自己) |

---

## 11. 目录结构

```
internal/
  domain/       App/Job/Instance + 9 态状态常量 + Executor/DispatchBody/SystemMetrics
  repository/   GORM 仓储(App/Job/Instance + OpenDatabase + 终态守护)
  instancelog/  实例日志:文件 + per-instance 锁 + 按 root 聚合 + 按 mtime 清理
  workerreg/    worker 心跳注册表(按 AppID)+ PickFull(tag 匹配 + score 择优)
  schedtime/    cron 解析 + 按 ScheduleKind 推算 next_run
  dispatch/     统一调度器 + Executor + reaper + RetryPump + CallbackPump + 并发槽/排队
  dservice/     领域服务(App/Job/Instance 业务编排)
  auth/         会话 store + 登录服务 + SessionAuth/RequireAdmin/AppScope
  wire/         协议层共用 JSON 工具(FlexInt64,兼容多语言 worker 数值字段 int/字符串)
  protocol/
    own/        管理端 REST /api/*(dto + translator + 登录会话 + 权限矩阵)
    worker/     简化 worker 协议 /worker/*(心跳/回报,无鉴权)
    powerjob/   PowerJob 协议 /server/* + /openApi/*(assert/acquire/heartbeat/reportStatus/reportLog + runJob)
  config/ logger/   配置加载 / slog+lumberjack 日志
```
