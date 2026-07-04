import { Drawer, Empty } from 'antd';

export default function LogsDrawer(props: { title: string; open: boolean; lines: string[]; onClose: () => void }) {
  return (
    <Drawer title={props.title} open={props.open} onClose={props.onClose} size={720}>
      {props.lines.length ? (
        <pre className="log-pre">{props.lines.join('\n')}</pre>
      ) : (
        <Empty description="暂无日志" />
      )}
    </Drawer>
  );
}
