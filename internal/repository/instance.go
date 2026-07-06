package repository

import (
	"time"

	"gorm.io/gorm"

	"task-schedule/internal/domain"
)

type InstanceStore struct{ db *gorm.DB }

// Create 创建实例(状态/参数等由调用方在 ins 指定)。
func (s InstanceStore) Create(ins *domain.Instance) error {
	return s.db.Create(ins).Error
}

// Get 按主键 id。
func (s InstanceStore) Get(id int64) (*domain.Instance, error) {
	var ins domain.Instance
	if err := s.db.Where("id = ?", id).First(&ins).Error; err != nil {
		return nil, err
	}
	return &ins, nil
}

// MarkDispatched 实例派发成功:置 waiting_receive + 绑定承接 worker + start_time。
//
// 终态守护:若实例已被并发置终态(stop/cancel 或 worker /run 回报终态),则不覆盖回 waiting_receive,
// 仅 status 受守护——worker_address/start_time 仍由第一条 UPDATE 无条件写入(审计"曾尝试派发")。
// 返回 RowsAffected:rows>0 表示 status 真改(未终态,worker 真承接);rows==0 表示已被并发置终态(守护未改),
// 调用方(dispatchToWorker)据此判断是否继续 Send。
//
// rows==0 的字段残留:此时 worker_address/start_time 已写入,但本次派发实际未送达(Send 不发生),
// 该终态实例会在 UI 上显示一个从未承接它的 worker 地址。属可接受的审计残留(不影响执行:终态不被 Send);
// 前端无法精确区分"真派发后置终态"与"未派发即置终态",故未在展示层特殊处理。
func (s InstanceStore) MarkDispatched(id int64, workerAddress string) (int64, error) {
	if err := s.db.Model(&domain.Instance{}).Where("id = ?", id).Updates(map[string]any{
		"worker_address": workerAddress,
		"start_time":     time.Now(),
	}).Error; err != nil {
		return 0, err
	}
	res := s.db.Model(&domain.Instance{}).
		Where("id = ? AND status NOT IN ?", id, domain.TerminalStatuses()).
		Update("status", domain.StatusWaitingReceive)
	return res.RowsAffected, res.Error
}

// InstanceFilter 实例列表过滤。
type InstanceFilter struct {
	AppID    int64
	JobID    int64
	Status   string
	StatusIn []string // 多状态 OR 查询(非空时与 Status 叠加)
	RootID   int64    // 按归属分组过滤(可选)
	Page     int
	Size     int
}

// List 按过滤条件分页查询(按 created_at DESC)。
func (s InstanceStore) List(f InstanceFilter) ([]domain.Instance, int64, error) {
	var list []domain.Instance
	var total int64
	q := s.db.Model(&domain.Instance{})
	if f.AppID > 0 {
		q = q.Where("app_id = ?", f.AppID)
	}
	if f.JobID > 0 {
		q = q.Where("job_id = ?", f.JobID)
	}
	if f.Status != "" {
		q = q.Where("status = ?", f.Status)
	}
	if len(f.StatusIn) > 0 {
		q = q.Where("status IN ?", f.StatusIn)
	}
	if f.RootID > 0 {
		q = q.Where("root_instance_id = ?", f.RootID)
	}
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	page, size := f.Page, f.Size
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 20
	}
	if err := q.Order("created_at DESC").Offset((page - 1) * size).Limit(size).Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

// resultFields 构造 UpdateResult 路径(终态守护写)的字段集:终态写 end_time;
// queued 解绑 worker(worker 回报 WAITING_DISPATCH=1 表示无法处理,清 worker_address + start_time
// 使其可重新选址派发;否则绑定仍指向无法处理的 worker,重派时可能再次选中)。
// UpdateResult 与 UpdateResultWithCallback 共用,避免终态/解绑规则双事实源。
func resultFields(status, result string) map[string]any {
	fields := map[string]any{"status": status}
	if result != "" {
		fields["result"] = result
	}
	if domain.StatusTerminal(status) {
		fields["end_time"] = time.Now()
	}
	if status == domain.StatusQueued {
		fields["worker_address"] = nil
		fields["start_time"] = nil
	}
	return fields
}

// forceStatusFields 构造 SetStatus 路径(管理员强制,无守护)的字段集:终态写 end_time,
// 非终态清 end_time(复活)。SetStatus 与 SetStatusWithCallback 共用。
func forceStatusFields(status, result string) map[string]any {
	fields := map[string]any{"status": status}
	if result != "" {
		fields["result"] = result
	}
	if domain.StatusTerminal(status) {
		fields["end_time"] = time.Now()
	} else {
		fields["end_time"] = nil // 复活:清空 end_time
	}
	return fields
}

// UpdateResult 写入状态/结果(终态不可回退守护:仅当前非终态时才更新,worker 乱序/迟到上报不覆盖既有终态)。
func (s InstanceStore) UpdateResult(id int64, status, result string) error {
	return s.db.Model(&domain.Instance{}).
		Where("id = ? AND status NOT IN ?", id, domain.TerminalStatuses()).
		Updates(resultFields(status, result)).Error
}

// SetStatus 强制写入状态(管理员纠错,不守护终态,可把终态实例复活或改终态)。
func (s InstanceStore) SetStatus(id int64, status, result string) error {
	return s.db.Model(&domain.Instance{}).Where("id = ?", id).Updates(forceStatusFields(status, result)).Error
}

// SetNextRetryTime 设定 DB 驱动重试到点时间。
func (s InstanceStore) SetNextRetryTime(id int64, t time.Time) error {
	return s.db.Model(&domain.Instance{}).Where("id = ?", id).Update("next_retry_time", t).Error
}

// ClearNextRetryTime 原子清重试标记(去重):仅当非空时清,返回是否抢到。
func (s InstanceStore) ClearNextRetryTime(id int64) (bool, error) {
	res := s.db.Model(&domain.Instance{}).Where("id = ? AND next_retry_time IS NOT NULL", id).
		Update("next_retry_time", nil)
	return res.RowsAffected > 0, res.Error
}

// ListRetryDue failed 且 next_retry_time 到期的实例,供 RetryPump 扫描。
func (s InstanceStore) ListRetryDue(now time.Time, limit int) ([]domain.Instance, error) {
	var list []domain.Instance
	if limit <= 0 {
		limit = 500
	}
	err := s.db.Where("status = ? AND next_retry_time IS NOT NULL AND next_retry_time <= ?",
		domain.StatusFailed, now).
		Order("next_retry_time ASC").Limit(limit).Find(&list).Error
	return list, err
}

// ListGeneralizedActive 已派发但未终结(waiting_receive/running),供 reaper / 启动恢复扫描。
// limit<=0 取默认上限,避免活跃实例极多时单轮全表扫打爆;调用方应批量预加载 job 消除 N+1。
// 按 start_time 升序:stallReason 的超时判定亦基于 start_time,卡得最久(最可能 stalled)的实例
// 优先落在 limit 窗口内——无 ORDER BY 时 DB 返回物理顺序,活跃数 > limit 时可能让不可回收的长任务
// 占满窗口,而真正 stalled 的实例(start_time 更小)反被截断漏扫。waiting/running 实例派发时即设
// start_time,非空,无需考虑 NULL 排序。
func (s InstanceStore) ListGeneralizedActive(limit int) ([]domain.Instance, error) {
	if limit <= 0 {
		limit = 500
	}
	var list []domain.Instance
	err := s.db.Where("status IN ?", []string{domain.StatusWaitingReceive, domain.StatusRunning}).
		Order("start_time ASC").Limit(limit).Find(&list).Error
	return list, err
}

// ListQueued 返回 status=queued 的实例(任意 trigger_type:manual/auto/retry),供调度器启动恢复。
// 手动优先队列是纯内存,重启即丢;queued 实例不被 reaper(只看 waiting_receive/running)/
// RetryPump(只看 failed)捞,需在启动时重新入队,否则永久滞留。auto/retry 触发路径在
// Create(queued) 与 Dispatch 之间崩溃也会残留 queued,故恢复不再限定 trigger_type。
func (s InstanceStore) ListQueued() ([]domain.Instance, error) {
	var list []domain.Instance
	err := s.db.Where("status = ?", domain.StatusQueued).Find(&list).Error
	return list, err
}

// ListUnboundQueued 返回 queued 且 worker_address=null 且距上次更新超过阈值的实例。
// 用于 reaper 扫描"worker 无法处理已解绑"的实例:worker 回报 WAITING_DISPATCH(1→queued)
// 表示无法处理(资源不足/依赖缺失),UpdateResult 会解绑 worker_address。该方法捞出这些实例,
// 由 reaper 转 failed 触发重试(有 RetryCount 的会重派,无的定格 failed)。
//
// 仅回收 trigger_type 为 auto/retry 的实例:这两类不会"排队等槽"(auto 定时触发 tryAcquire
// 失败即跳过本次;retry 槽满走 SetNextRetryTime),故无 worker_address 的 queued 必为派发后被解绑。
// manual 实例由 SubmitManualDelayed 落库后进内存优先队列等 MaxConcurrency 槽,等槽期间 updated_at
// 不刷新且 worker_address 为空——若纳入回收会被误杀(并发打满时 30s 后转 failed,RetryCount=0 则丢失、
// >0 则重复执行)。manual 派发后被解绑的极少见边界(可手动 stop 处理),不在此自动回收。
//
// worker_address 为空串或 NULL 都算解绑(GORM Updates 空串不写 NULL);
// updated_at < staleTime 避免误杀刚解绑的实例。按 updated_at ASC:卡得最久的优先处理。limit 500 防单轮全表扫打爆。
func (s InstanceStore) ListUnboundQueued(staleThreshold time.Duration) ([]domain.Instance, error) {
	var list []domain.Instance
	staleTime := time.Now().Add(-staleThreshold)
	err := s.db.Where(
		"status = ? AND trigger_type IN ? AND (worker_address IS NULL OR worker_address = '') AND updated_at < ?",
		domain.StatusQueued, []string{"auto", "retry"}, staleTime,
	).Order("updated_at ASC").Limit(500).Find(&list).Error
	return list, err
}

// ===== 状态变更 + 回调(同事务) =====
//
// *WithCallback 系列在写实例状态的同事务内顺带插入一条 callback 记录(cb 非 nil 时)。
// 返回 RowsAffected:status 实际变化(匹配 WHERE)才 >0,调用方据此决定是否计入"真事件"。
// cb.InstanceID 在事务内(ins 创建后 / 已知 id)回填。原方法(Create/MarkDispatched/...)保留无回调路径。

// CreateWithCallback 创建实例;build 非 nil 时在 Create 后(ins.ID 已填)调用,同事务插入回调。
// 用 build 闭包而非预构造 cb:保证 payload 快照里的 instance.id 是 Create 后的真实 ID(而非 0)。
func (s InstanceStore) CreateWithCallback(ins *domain.Instance, build func() *domain.Callback) error {
	if build == nil {
		return s.db.Create(ins).Error
	}
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(ins).Error; err != nil {
			return err
		}
		cb := build() // ins.ID 已由 Create 填充
		if cb == nil {
			return nil
		}
		cb.InstanceID = ins.ID
		return tx.Create(cb).Error
	})
}

// MarkDispatchedWithCallback 置 waiting_receive;status 真变化(rows>0)且 cb 非 nil 才插回调。
// rows==0(终态守护)时不插回调;worker_address/start_time 残留语义同 MarkDispatched。
func (s InstanceStore) MarkDispatchedWithCallback(id int64, workerAddress string, cb *domain.Callback) (int64, error) {
	if cb == nil {
		return s.MarkDispatched(id, workerAddress) // 无回调走原路径,免事务开销(返回真实 rows)
	}
	var rows int64
	err := s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&domain.Instance{}).Where("id = ?", id).Updates(map[string]any{
			"worker_address": workerAddress,
			"start_time":     time.Now(),
		}).Error; err != nil {
			return err
		}
		res := tx.Model(&domain.Instance{}).
			Where("id = ? AND status NOT IN ?", id, domain.TerminalStatuses()).
			Update("status", domain.StatusWaitingReceive)
		if res.Error != nil {
			return res.Error
		}
		rows = res.RowsAffected
		if rows == 0 {
			return nil // 已终态守护未改
		}
		cb.InstanceID = id
		return tx.Create(cb).Error
	})
	return rows, err
}

// UpdateResultWithCallback 写状态/结果(终态守护);status 真变化(rows>0)且 cb 非 nil 才插回调。
// cb==nil 仍走事务(不插回调)以返回真实 RowsAffected——reaper/recover 据此判断实例是否已被
// 并发终结,不能早返回到不返回 rows 的 UpdateResult 快路径。
func (s InstanceStore) UpdateResultWithCallback(id int64, status, result string, cb *domain.Callback) (int64, error) {
	var rows int64
	err := s.db.Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&domain.Instance{}).
			Where("id = ? AND status NOT IN ?", id, domain.TerminalStatuses()).
			Updates(resultFields(status, result))
		if res.Error != nil {
			return res.Error
		}
		rows = res.RowsAffected
		if rows == 0 || cb == nil {
			return nil
		}
		cb.InstanceID = id
		return tx.Create(cb).Error
	})
	return rows, err
}

// FailDispatchWithCallback 派发失败善后专用:置 failed + 清 worker_address/start_time + end_time,
// status 真变化(rows>0)且 cb 非 nil 才插回调。
//
// 清 worker 绑定是因为派发失败(选后即绑下 Send 失败时 worker_address 已先于 POST commit),
// 该绑定无意义且会误导(展示一个 failed 实例仍指向某 worker);与 worker 回报 failed(保留
// worker_address 供审计"哪个 worker 执行失败")路径区分,故不复用 UpdateResultWithCallback。
// 终态守护同 UpdateResultWithCallback:并发 stop/cancel 已置终态时 rows=0,不覆盖。
func (s InstanceStore) FailDispatchWithCallback(id int64, reason string, cb *domain.Callback) (int64, error) {
	var rows int64
	err := s.db.Transaction(func(tx *gorm.DB) error {
		fields := map[string]any{
			"status":         domain.StatusFailed,
			"worker_address": nil,
			"start_time":     nil,
			"end_time":       time.Now(),
		}
		if reason != "" {
			fields["result"] = reason
		}
		res := tx.Model(&domain.Instance{}).
			Where("id = ? AND status NOT IN ?", id, domain.TerminalStatuses()).
			Updates(fields)
		if res.Error != nil {
			return res.Error
		}
		rows = res.RowsAffected
		if rows == 0 {
			return nil
		}
		if cb != nil {
			cb.InstanceID = id
			return tx.Create(cb).Error
		}
		return nil
	})
	return rows, err
}

// SetStatusWithCallback 强制写状态(无守护);rows>0 且 cb 非 nil 才插回调。
func (s InstanceStore) SetStatusWithCallback(id int64, status, result string, cb *domain.Callback) (int64, error) {
	if cb == nil {
		return 0, s.SetStatus(id, status, result) // 无回调走原路径
	}
	var rows int64
	err := s.db.Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&domain.Instance{}).Where("id = ?", id).Updates(forceStatusFields(status, result))
		if res.Error != nil {
			return res.Error
		}
		rows = res.RowsAffected
		if rows == 0 {
			return nil
		}
		cb.InstanceID = id
		return tx.Create(cb).Error
	})
	return rows, err
}
