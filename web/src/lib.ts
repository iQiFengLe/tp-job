import dayjs from 'dayjs';

export const PAGE_SIZE = 20;

// 9 态状态机颜色(见 docs/refactor-unified-model.md §5)
export const statusColor: Record<string, string> = {
  queued: 'default',
  waiting_receive: 'processing',
  running: 'processing',
  success: 'success',
  failed: 'error',
  timeout: 'orange',
  skipped: 'default',
  canceled: 'default',
  stopped: 'warning',
};

export const statusLabel: Record<string, string> = {
  queued: '排队中',
  waiting_receive: '等待接收',
  running: '运行中',
  success: '成功',
  failed: '失败',
  timeout: '已超时',
  skipped: '已跳过',
  canceled: '已取消',
  stopped: '已停止',
};

export const statusOptions = ['queued', 'waiting_receive', 'running', 'success', 'failed', 'timeout', 'skipped', 'canceled', 'stopped'].map(
  (value) => ({ label: statusLabel[value] || value, value }),
);

// 实例触发来源(auto/manual/retry)中文映射。
export const triggerTypeLabel: Record<string, string> = {
  auto: '自动',
  manual: '手动',
  retry: '重试',
};

export function formatTime(value?: string | number) {
  return value ? dayjs(value).format('YYYY-MM-DD HH:mm:ss') : '-';
}

// 耗时友好化:0/缺省→"-";<1s 显示 ms;>=60s 显示 m+s;其余 1 位小数秒。
export function formatDuration(ms?: number) {
  if (!ms) return '-';
  if (ms < 1000) return `${ms}ms`;
  const sec = ms / 1000;
  if (sec < 60) return `${sec.toFixed(1)}s`;
  const m = Math.floor(sec / 60);
  const s = Math.round(sec % 60);
  return `${m}m${s}s`;
}

// 紧凑化:剔除 undefined/null/'' 字段,适配部分更新。
export function compactObject<T extends Record<string, unknown>>(values: T) {
  return Object.fromEntries(
    Object.entries(values).filter(([, value]) => value !== undefined && value !== null && value !== ''),
  ) as Partial<T>;
}
