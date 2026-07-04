import dayjs from 'dayjs';
import { formatTime } from './lib';
import type { JobView, ScheduleKind } from './types';

export const scheduleKindOptions: { label: string; value: ScheduleKind }[] = [
  { label: 'Cron', value: 'cron' },
  { label: '固定频率', value: 'fix_rate' },
  { label: '固定延迟', value: 'fix_delay' },
  { label: '延迟执行', value: 'delay' },
  { label: '指定时刻', value: 'run_at' },
  { label: 'API 触发', value: 'api' },
];

// 自动调度类型(有固定节奏、受生效窗口约束):cron/固定频率/固定延迟/延迟执行。
// api(纯手动触发)、run_at(指定时刻一次性)不算自动调度——无周期,生效窗口对它们无意义。
export const AUTO_KINDS: ScheduleKind[] = ['cron', 'fix_rate', 'fix_delay', 'delay'];
export const isAutoKind = (k: ScheduleKind | undefined): boolean => !!k && (AUTO_KINDS as string[]).includes(k);

export function scheduleExprFromForm(kind: ScheduleKind | undefined, expr: unknown, runAt: unknown): string | undefined {
  switch (kind) {
    case 'cron':
      return (expr as string) || undefined;
    case 'fix_rate':
    case 'fix_delay':
    case 'delay':
      return expr === undefined || expr === null || expr === '' ? undefined : String(expr);
    case 'run_at':
      return runAt ? (runAt as dayjs.Dayjs).toISOString() : undefined;
    case 'api':
      return undefined;
    default:
      return undefined;
  }
}

export function formatScheduleExpr(job: JobView): string {
  switch (job.schedule_kind) {
    case 'fix_rate':
    case 'fix_delay':
      return job.schedule_expr ? `${job.schedule_expr}ms` : '-';
    case 'delay':
      return job.schedule_expr ? `${job.schedule_expr}s` : '-';
    case 'run_at':
      return formatTime(job.schedule_expr);
    case 'cron':
      return job.schedule_expr || '-';
    default:
      return '-';
  }
}
