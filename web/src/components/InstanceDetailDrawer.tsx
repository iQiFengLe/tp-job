import { FileTextOutlined } from '@ant-design/icons';
import { Button, Descriptions, Drawer, Tag, Typography } from 'antd';
import { formatTime, statusColor, statusLabel, triggerTypeLabel } from '../lib';
import { scheduleKindOptions } from '../schedule';
import type { InstanceView } from '../types';

const { Text } = Typography;

// 耗时友好化:<1s 显示 ms,>=60s 显示 m+s,其余 1 位小数秒。
function formatDuration(ms?: number) {
  if (!ms) return '-';
  if (ms < 1000) return `${ms}ms`;
  const sec = ms / 1000;
  if (sec < 60) return `${sec.toFixed(1)}s`;
  const m = Math.floor(sec / 60);
  const s = Math.round(sec % 60);
  return `${m}m${s}s`;
}

export default function InstanceDetailDrawer(props: {
  instance?: InstanceView;
  onClose: () => void;
  onShowLogs?: (instance: InstanceView) => void;
}) {
  const ins = props.instance;
  const kindLabel = ins ? scheduleKindOptions.find((o) => o.value === ins.schedule_kind)?.label || ins.schedule_kind : '';
  // root_instance_id 省略了 0:有值且非自身时表明是重试子实例,链回根实例。
  const hasRoot = ins && ins.root_instance_id && ins.root_instance_id !== ins.id;
  return (
    <Drawer
      open={Boolean(ins)}
      onClose={props.onClose}
      size={600}
      title="实例详情"
      extra={
        ins && props.onShowLogs ? (
          <Button icon={<FileTextOutlined />} onClick={() => props.onShowLogs?.(ins)}>
            查看日志
          </Button>
        ) : undefined
      }
    >
      {ins && (
        <Descriptions column={1} bordered size="medium">
          <Descriptions.Item label="实例 ID">{ins.id}</Descriptions.Item>
          <Descriptions.Item label="状态">
            <Tag color={statusColor[ins.status] || 'default'} style={{ fontWeight: 500 }}>
              {statusLabel[ins.status] || ins.status}
            </Tag>
          </Descriptions.Item>
          <Descriptions.Item label="Job ID">{ins.job_id}</Descriptions.Item>
          <Descriptions.Item label="触发">
            {triggerTypeLabel[ins.trigger_type || ''] || ins.trigger_type || '-'}
            {ins.retry_index ? <Text type="secondary"> · 第 {ins.retry_index} 次重试</Text> : ''}
          </Descriptions.Item>
          <Descriptions.Item label="调度类型">{kindLabel || '-'}</Descriptions.Item>
          <Descriptions.Item label="优先级">{ins.priority || 0}</Descriptions.Item>
          <Descriptions.Item label="Root 实例">
            {hasRoot ? ins.root_instance_id : <Text type="secondary">本实例即根</Text>}
          </Descriptions.Item>
          <Descriptions.Item label="标签">{ins.tag || '-'}</Descriptions.Item>
          <Descriptions.Item label="Worker">{ins.worker_address || '-'}</Descriptions.Item>
          <Descriptions.Item label="触发时间">{formatTime(ins.trigger_time)}</Descriptions.Item>
          <Descriptions.Item label="开始时间">{formatTime(ins.start_time)}</Descriptions.Item>
          <Descriptions.Item label="结束时间">{formatTime(ins.end_time)}</Descriptions.Item>
          <Descriptions.Item label="耗时">{formatDuration(ins.duration_ms)}</Descriptions.Item>
          <Descriptions.Item label="实例参数">
            {ins.job_instance_params ? <Text code style={{ whiteSpace: 'pre-wrap' }}>{ins.job_instance_params}</Text> : '-'}
          </Descriptions.Item>
          <Descriptions.Item label="执行结果">
            {ins.result ? <Text style={{ whiteSpace: 'pre-wrap' }}>{ins.result}</Text> : '-'}
          </Descriptions.Item>
        </Descriptions>
      )}
    </Drawer>
  );
}
