import { Card } from 'antd';
import type { ReactNode } from 'react';

// KPI 统计卡:左侧数值 + 标签 + 副标题,右侧彩色图标块。
// tint 为图标主色,icon 容器背景由 color-mix 派生 tint 12%——跨主题(深浅)都成立,
// 不依赖 antd 背景色,避免 glass/dark 下背景撞色。
export default function StatCard(props: {
  label: ReactNode;
  value: ReactNode;
  unit?: ReactNode;
  hint?: ReactNode;
  icon?: ReactNode;
  tint?: string;
  loading?: boolean;
}) {
  const tint = props.tint || '#1677ff';
  return (
    <Card className="stat-card" variant="outlined" loading={props.loading}>
      <div className="stat-inner">
        <div className="stat-meta">
          <span className="stat-label">{props.label}</span>
          <span className="stat-value">
            {props.value}
            {props.unit && <span className="unit">{props.unit}</span>}
          </span>
          {props.hint && <span className="stat-hint">{props.hint}</span>}
        </div>
        {props.icon && (
          <span
            className="stat-icon"
            style={{
              color: tint,
              background: `color-mix(in srgb, ${tint} 12%, transparent)`,
            }}
          >
            {props.icon}
          </span>
        )}
      </div>
    </Card>
  );
}
