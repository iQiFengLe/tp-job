import { Card } from 'antd';
import { statusLabel } from '../lib';

// 9 态状态机→固定展示色(取 antd 常用语义色,深浅主题下都可读)。
// 与 lib.ts 的 statusColor(preset 名)并存:那张表给 antd Tag 用,这张给自定义横条用 hex。
export const STATUS_HEX: Record<string, string> = {
  running: '#1677ff',
  waiting_receive: '#4096ff',
  queued: '#bfbfbf',
  success: '#52c41a',
  failed: '#ff4d4f',
  timeout: '#faad14',
  stopped: '#d48806',
  skipped: '#d9d9d9',
  canceled: '#d9d9d9',
};

// 展示顺序:把"值得关注"的状态排在前面(运行中 > 失败 > 超时 > 停止 > 成功 > 其余)。
const ORDER = ['running', 'waiting_receive', 'failed', 'timeout', 'stopped', 'success', 'queued', 'skipped', 'canceled'];

// 实例状态分布条:左侧堆叠横条(各状态按比例分段),下方紧凑图例(圆点+标签+计数+占比)。
// count=0 的状态不显示。用于实例页顶部"一眼掌握全局"。
export default function StatusDistribution(props: {
  counts: Record<string, number>;
  note?: string;
}) {
  const entries = ORDER.filter((s) => (props.counts[s] || 0) > 0).map((s) => ({
    status: s,
    count: props.counts[s],
    hex: STATUS_HEX[s] || '#bfbfbf',
  }));
  const sum = entries.reduce((a, e) => a + e.count, 0) || 1;

  return (
    <Card className="dist" variant="outlined">
      <div className="dist-head">
        <span className="label">实例状态分布</span>
        <span className="total">{props.note || `共 ${sum} 条`}</span>
      </div>
      <div className="dist-bar">
        {entries.map((e) => (
          <div
            key={e.status}
            className="dist-seg"
            style={{ width: `${(e.count / sum) * 100}%`, background: e.hex }}
            title={`${statusLabel[e.status] || e.status}: ${e.count}`}
          />
        ))}
      </div>
      <div className="dist-legend">
        {entries.map((e) => (
          <div className="item" key={e.status}>
            <span className="dot" style={{ background: e.hex }} />
            <span>{statusLabel[e.status] || e.status}</span>
            <span className="cnt">{e.count}</span>
            <span className="pct">{Math.round((e.count / sum) * 100)}%</span>
          </div>
        ))}
      </div>
    </Card>
  );
}
