import { ReloadOutlined } from '@ant-design/icons';
import { Button, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useEffect, useState } from 'react';
import { api } from '../api';
import { formatTime } from '../lib';
import type { WorkerView } from '../types';
import AppGate from './AppGate';

const { Text, Title } = Typography;

export default function WorkersView(props: { appId?: number; onError: (error: unknown) => void }) {
  const [workers, setWorkers] = useState<WorkerView[]>([]);
  const [loading, setLoading] = useState(false);

  const load = async () => {
    if (!props.appId) return;
    setLoading(true);
    try {
      const data = await api.workers.list(props.appId);
      setWorkers(data.list || []);
    } catch (error) {
      props.onError(error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (props.appId) void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [props.appId]);

  const columns: ColumnsType<WorkerView> = [
    { title: '地址', dataIndex: 'worker_address' },
    {
      title: '协议',
      dataIndex: 'protocol',
      width: 110,
      render: (v: string) => (
        <Tag color={v === 'powerjob' ? 'purple' : 'blue'} style={{ fontWeight: 500 }}>
          {v || '-'}
        </Tag>
      ),
    },
    {
      title: '标签',
      dataIndex: 'tags',
      render: (tags?: string[]) => (
        tags && tags.length ? tags.map((t) => <Tag key={t} style={{ fontWeight: 500 }}>{t}</Tag>) : '-'
      ),
    },
    { title: 'Score', dataIndex: 'score', width: 80, render: (v?: number) => v ?? '-' },
    {
      title: 'CPU',
      width: 130,
      render: (_, r) => (r.cpu_processors ? `${(r.cpu_load ?? 0).toFixed(2)} / ${r.cpu_processors}核` : '-'),
    },
    {
      title: '内存',
      width: 170,
      render: (_, r) =>
        r.jvm_max_memory && r.jvm_max_memory > 0
          ? `${(r.jvm_used_memory ?? 0).toFixed(0)} / ${r.jvm_max_memory.toFixed(0)} MB (${((r.jvm_memory_usage ?? 0) * 100).toFixed(0)}%)`
          : '-',
    },
    {
      title: '磁盘',
      width: 170,
      render: (_, r) =>
        r.disk_total && r.disk_total > 0
          ? `${(r.disk_used ?? 0).toFixed(1)} / ${r.disk_total.toFixed(1)} GB (${((r.disk_usage ?? 0) * 100).toFixed(0)}%)`
          : '-',
    },
    { title: '最后心跳', dataIndex: 'last_heartbeat', render: formatTime, width: 180 },
  ];

  return (
    <section className="view">
      <AppGate appId={props.appId} />
      <div className="view-head">
        <div>
          <Title level={3}>Worker</Title>
          <Text type="secondary">当前应用在线节点(内存注册表,不入库)。选址按 tag 匹配 + score 择优。</Text>
        </div>
        <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>
          刷新
        </Button>
      </div>
      <Table rowKey="worker_address" columns={columns} dataSource={workers} loading={loading} pagination={false} scroll={{ x: 1200 }} />
    </section>
  );
}
