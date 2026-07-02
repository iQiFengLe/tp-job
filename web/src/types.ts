// 前端类型,与后端 internal/protocol/own 的 dto.go / auth.go JSON 形状对齐。

export interface ApiBody<T> {
  code: number;
  msg: string;
  data: T;
}

// 列表响应:新 /api 只回 {list, total};分页 page/size 由前端本地维护(请求时作为 query 发出)。
export interface PageResult<T> {
  list: T[];
  total: number;
}

// ===== 鉴权 =====

export type Role = 'admin' | 'app';

export interface LoginReq {
  ident: string;
  password: string;
}

export interface LoginResp {
  token: string;
  role: Role;
  username: string;
  app_id?: number;
  app_name?: string;
  expires_at: string;
}

export interface MeResp {
  role: Role;
  username: string;
  app_id?: number;
  app_name?: string;
}

// ===== App =====

export interface AppView {
  id: number;
  app_name: string;
  status: number; // 1=启用 0=禁用
  created_at: string;
  updated_at: string;
}

export interface AppCreateValues {
  app_name: string;
  password: string;
  status?: number;
}

export interface AppUpdateValues {
  app_name?: string;
  password?: string;
  status?: number;
}

// ===== Job =====

export type ScheduleKind = 'cron' | 'fix_rate' | 'fix_delay' | 'delay' | 'run_at' | 'manual';

export interface JobView {
  id: number;
  app_id: number;
  name: string;
  execute_type?: string;
  job_params?: string;
  tag?: string;
  timeout_sec?: number;
  schedule_kind?: ScheduleKind;
  schedule_expr?: string;
  next_run_time?: string;
  max_concurrency?: number;
  max_wait_seconds?: number;
  retry_count?: number;
  retry_interval_sec?: number;
  default_priority?: number;
  enabled: boolean;
  created_at: string;
  updated_at: string;
}

export interface JobCreateValues {
  name: string;
  schedule_kind: ScheduleKind;
  schedule_expr?: string;
  job_params?: string;
  tag?: string;
  timeout_sec?: number;
  max_concurrency?: number;
  max_wait_seconds?: number;
  retry_count?: number;
  retry_interval_sec?: number;
  default_priority?: number;
  enabled?: boolean;
}

export type JobUpdateValues = Partial<JobCreateValues>;

// ===== Instance =====

// 8 态状态机(见 docs/refactor-unified-model.md §5)
export type InstanceStatus =
  | 'queued'
  | 'waiting_receive'
  | 'running'
  | 'success'
  | 'failed'
  | 'skipped'
  | 'canceled'
  | 'stopped';

export interface InstanceView {
  id: number;
  job_id: number;
  app_id: number;
  status: InstanceStatus | string;
  trigger_type?: string;
  priority?: number;
  retry_index?: number;
  root_instance_id?: number;
  tag?: string;
  worker_address?: string;
  job_instance_params?: string;
  result?: string;
  trigger_time: string;
  start_time?: string;
  end_time?: string;
  duration_ms?: number;
}

// ===== 日志 =====

// 实例日志:按 root 聚合的原始行(worker 上报 + 调度埋点),前端原样渲染。
export interface LogResult {
  lines: string[];
  total: number;
}

// ===== Worker(在线节点,读 workerreg 内存注册表,不入库)=====

export interface WorkerView {
  worker_address: string;
  protocol: string; // http | powerjob
  tags?: string[];
  accept_not_tag_job?: boolean;
  score?: number; // 选址分(高=空闲)
  cpu_load?: number;
  cpu_processors?: number;
  last_heartbeat: string;
}
