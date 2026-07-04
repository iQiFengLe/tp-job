import { Empty } from 'antd';

function adminHint() {
  return '请先选择一个应用';
}

export default function AppGate(props: { appId?: number }) {
  if (props.appId) return null;
  return (
    <div className="gate">
      <Empty description={adminHint()} />
    </div>
  );
}
