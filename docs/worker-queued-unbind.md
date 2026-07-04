# Worker 回报 WAITING_DISPATCH 解绑机制

## 需求背景

Worker 处理任务时可能遇到资源不足等问题，需要告知调度器"我无法处理，请重新调度"。
PowerJob 协议中用 `instanceStatus=1`（WAITING_DISPATCH → queued）表达此语义。

## 实现方案

### 核心修改

**文件**：`internal/repository/instance.go:90-111`

```go
func (s InstanceStore) UpdateResult(id int64, status, result string) error {
    fields := map[string]any{"status": status}
    if result != "" {
        fields["result"] = result
    }
    if domain.StatusTerminal(status) {
        fields["end_time"] = time.Now()
    }
    // ★ worker 回报 queued 时解绑 worker + 清 start_time ★
    if status == domain.StatusQueued {
        fields["worker_address"] = nil
        fields["start_time"] = nil
    }
    return s.db.Model(&domain.Instance{}).
        Where("id = ? AND status NOT IN ?", id, domain.TerminalStatuses()).
        Updates(fields).Error
}
```

### 解绑逻辑

当 worker 回报 `instanceStatus=1`（PowerJob WAITING_DISPATCH）时：
1. 状态变为 `queued`
2. 清空 `worker_address`（解除绑定）
3. 清空 `start_time`（重置派发时间）
4. 保留 `result` 字段（记录原因："资源不足,请重新调度"等）

### 重新派发路径

解绑后的实例回到 `queued` 状态，将通过以下路径重新派发：

#### 手动触发实例
已在优先队列（`RunManualDispatcher`）中的实例，解绑后：
1. `runManualHeld` 派发前会查 DB（L335）
2. 发现 `status=queued` 但 `worker_address` 已清空
3. 重新执行 `dispatchToWorker`：`PickWorker` → `MarkDispatched` → `Send`
4. 重新选址可能选到不同 worker

**注意**：当前代码在 L335 检测到非终态会继续派发，`queued` 非终态，所以会继续。但内存中的 `manualItem.ins` 是入队快照，`WorkerAddress` 仍是旧值。实际派发会查 DB 获取最新状态，或者 `dispatchToWorker` 内部重新 `PickWorker`，不会复用快照中的旧 worker 地址。

#### 定时触发实例
定时触发创建的实例若回报 `queued`，当前不会自动重派（需人工介入或配置重试）。建议：
- 为定时任务配置 `RetryCount>0`，解绑后实例为 `queued` 非终态，可能需要额外逻辑将其标记为 `failed` 触发重试
- 或者 reaper 扫描时检测"queued 但已绑定过 worker（通过 updated_at 判断）"→ 标记 failed 触发重试

## 测试覆盖

**文件**：`internal/protocol/powerjob/handler_test.go:116-158`

```go
// worker 回报 1(WAITING_DISPATCH)表示无法处理:应解绑 worker + 清 start_time
ins3 := &domain.Instance{
    JobID: 1, AppID: 1, Status: domain.StatusWaitingReceive,
    WorkerAddress: "worker1:9000", StartTime: timePtr(time.Now()),
}
_ = st.Instance.Create(ins3)
do(t, "POST", "/server/reportInstanceStatus",
    ReportInstanceStatusReq{InstanceID: wire.FlexInt64(ins3.ID), 
        InstanceStatus: WireWaitingDispatch,
        Result: "资源不足,请重新调度"}, d)
got3, _ := st.Instance.Get(ins3.ID)
// 验证：status=queued, worker_address=nil, start_time=nil, result 保留
```

## 边界情况

### 1. 终态守护
回报 `queued` 受终态守护保护：
```go
Where("id = ? AND status NOT IN ?", id, domain.TerminalStatuses())
```
已终态（success/failed）的实例不会被回退到 `queued`。

### 2. 迟到回报
若 worker 已超时被 reaper 标记 `failed` 并重试，迟到的 `queued` 回报会被终态守护拒绝（failed 是终态）。

### 3. 并发安全
`UpdateResult` 通过 GORM 的 `Updates` 是原子操作，多个 worker 并发回报时只有一个能成功推进状态。

## 待优化项

### 1. 定时触发实例的重派
当前定时实例回报 `queued` 后不会自动重派。建议方案：
```go
// reaper 增强：扫描 queued 且 worker_address=null 但 updated_at 距今 >30s 的实例
// → 标记 failed + scheduleRetry（触发 RetryPump 重派）
```

### 2. 排队实例的快照问题
`manualItem` 持有的是入队快照，解绑后 `WorkerAddress` 仍是旧值。虽然实际派发会重新选址，但快照数据可能引起混淆。建议：
```go
// runManualHeld 派发前重新查 DB，不仅检测终态，还检测 worker 绑定是否被清空
if cur.Status == domain.StatusQueued && cur.WorkerAddress == "" {
    // 已被解绑，重新走完整派发流程（PickWorker 会选新 worker）
}
```

## 关联 PR 文件
- `internal/repository/instance.go`（核心逻辑）
- `internal/protocol/powerjob/handler_test.go`（测试用例）
- `internal/protocol/powerjob/status.go`（状态码映射，WireWaitingDispatch=1）
