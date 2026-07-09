import { Progress } from 'antd';
import type { WorkerView } from '../types';

// 资源占用健康色:<70% 绿、70~90% 橘、>=90% 红。Worker 负载一眼可见。
export function healthColor(pct: number): string {
  if (pct >= 90) return '#ff4d4f';
  if (pct >= 70) return '#faad14';
  return '#52c41a';
}

function ResRow(props: { name: string; pct: number | null; text: string }) {
  const has = props.pct != null;
  return (
    <div className="res-row">
      <span className="res-name">{props.name}</span>
      <Progress
        percent={has ? Math.min(100, Math.round(props.pct as number)) : 0}
        size="small"
        showInfo={false}
        strokeColor={has ? healthColor(props.pct as number) : '#d9d9d9'}
        trailColor="rgba(128, 128, 128, 0.15)"
      />
      <span className="res-val">{props.text}</span>
    </div>
  );
}

// Worker 资源条单元格:CPU/内存/磁盘 三条紧凑进度条竖排,替代旧版纯文字 "2.3/8核"。
// 无上报数据时 pct=null,进度条空、文本显示 "-"。
export default function ResourceBar(props: { worker: WorkerView }) {
  const w = props.worker;
  const cpuPct = w.cpu_processors ? ((w.cpu_load ?? 0) / w.cpu_processors) * 100 : null;
  const memPct = w.jvm_memory_usage != null ? w.jvm_memory_usage * 100 : null;
  const diskPct = w.disk_usage != null ? w.disk_usage * 100 : null;

  return (
    <div className="res-cell">
      <ResRow
        name="CPU"
        pct={cpuPct}
        text={w.cpu_processors ? `${(w.cpu_load ?? 0).toFixed(1)} / ${w.cpu_processors}核` : '-'}
      />
      <ResRow
        name="内存"
        pct={memPct}
        text={w.jvm_max_memory ? `${(w.jvm_used_memory ?? 0).toFixed(1)} / ${w.jvm_max_memory.toFixed(1)}G` : '-'}
      />
      <ResRow
        name="磁盘"
        pct={diskPct}
        text={w.disk_total ? `${(w.disk_used ?? 0).toFixed(0)} / ${w.disk_total.toFixed(0)}G` : '-'}
      />
    </div>
  );
}
