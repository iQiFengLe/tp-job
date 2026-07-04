import { Descriptions, Drawer, Tag, Typography } from 'antd';
import { formatTime } from '../lib';
import { formatScheduleExpr, scheduleKindOptions } from '../schedule';
import type { JobView } from '../types';

const { Text } = Typography;

export default function JobDetailDrawer(props: { job?: JobView; onClose: () => void }) {
  const job = props.job;
  const kindLabel = job ? scheduleKindOptions.find((opt) => opt.value === job.schedule_kind)?.label || job.schedule_kind : '';
  return (
    <Drawer open={Boolean(job)} onClose={props.onClose} size={600} title="任务详情">
      {job && (
        <Descriptions column={1} bordered size="medium">
          <Descriptions.Item label="ID">{job.id}</Descriptions.Item>
          <Descriptions.Item label="名称">{job.name}</Descriptions.Item>
          <Descriptions.Item label="调度类型">
            <Tag style={{ fontWeight: 500 }}>{kindLabel}</Tag>
          </Descriptions.Item>
          <Descriptions.Item label="调度表达式">{formatScheduleExpr(job)}</Descriptions.Item>
          <Descriptions.Item label="标签">{job.tag || '-'}</Descriptions.Item>
          <Descriptions.Item label="回调 URL">{job.callback_url || '-'}</Descriptions.Item>
          <Descriptions.Item label="任务参数">
            <Text code style={{ whiteSpace: 'pre-wrap' }}>{job.job_params || '-'}</Text>
          </Descriptions.Item>
          <Descriptions.Item label="超时秒数">{job.timeout_sec || '-'}</Descriptions.Item>
          <Descriptions.Item label="最大并发">{job.max_concurrency || '-'}</Descriptions.Item>
          <Descriptions.Item label="排队等待秒">{job.max_wait_seconds || '-'}</Descriptions.Item>
          <Descriptions.Item label="重试次数">{job.retry_count || 0}</Descriptions.Item>
          <Descriptions.Item label="重试间隔秒">{job.retry_interval_sec || 0}</Descriptions.Item>
          <Descriptions.Item label="默认优先级">{job.default_priority || 0}</Descriptions.Item>
          <Descriptions.Item label="状态">
            <Tag color={job.enabled ? 'success' : 'default'} style={{ fontWeight: 500 }}>
              {job.enabled ? '启用' : '停用'}
            </Tag>
          </Descriptions.Item>
          <Descriptions.Item label="生效窗口">
            {(job.start_time || job.end_time)
              ? `${formatTime(job.start_time) || '无起始'} ~ ${formatTime(job.end_time) || '无截止'}`
              : '-'}
          </Descriptions.Item>
          <Descriptions.Item label="下次执行">{formatTime(job.next_run_time)}</Descriptions.Item>
          <Descriptions.Item label="创建时间">{formatTime(job.created_at)}</Descriptions.Item>
          <Descriptions.Item label="更新时间">{formatTime(job.updated_at)}</Descriptions.Item>
        </Descriptions>
      )}
    </Drawer>
  );
}
