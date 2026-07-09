import { Card } from 'antd';
import type { ReactNode } from 'react';

// 区块卡片:统一包裹表格/内容,顶部放标题(+副标题)与操作区。
// 用 antd Card 做"白卡"容器(现代极简风的核心),替代旧版裸 Table 平铺。
export default function SectionCard(props: {
  title?: ReactNode;
  sub?: ReactNode;
  extra?: ReactNode;
  children: ReactNode;
  className?: string;
  bodyClassName?: string;
}) {
  return (
    <Card className={`section-card ${props.className || ''}`} variant="outlined">
      {(props.title || props.extra) && (
        <div className="section-card-head">
          <div className="title-block">
            {typeof props.title === 'string' ? <h4>{props.title}</h4> : props.title}
            {props.sub && <div className="sub">{props.sub}</div>}
          </div>
          {props.extra && <div className="section-card-actions">{props.extra}</div>}
        </div>
      )}
      <div className={props.bodyClassName}>{props.children}</div>
    </Card>
  );
}
