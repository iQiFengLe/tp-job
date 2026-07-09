import { CloudServerOutlined, DashboardOutlined, DatabaseOutlined, HddOutlined, ReloadOutlined } from '@ant-design/icons';
import { Button, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useEffect, useState } from 'react';
import { api } from '../api';
import { formatTime } from '../lib';
import type { WorkerView } from '../types';
import AppGate from './AppGate';
import ResourceBar, { healthColor } from './ResourceBar';
import SectionCard from './SectionCard';
import StatCard from './StatCard';

const { Text, Title } = Typography;

function avg(nums: number[]): number | null {
  if (!nums.length) return null;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

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

  // 概览指标:只对上报了对应字段的节点求平均
  const cpuPcts = workers.filter((w) => w.cpu_processors).map((w) => ((w.cpu_load ?? 0) / (w.cpu_processors as number)) * 100);
  const memPcts = workers.filter((w) => w.jvm_memory_usage != null).map((w) => (w.jvm_memory_usage as number) * 100);
  const diskPcts = workers.filter((w) => w.disk_usage != null).map((w) => (w.disk_usage as number) * 100);
  const avgCpu = avg(cpuPcts);
  const avgMem = avg(memPcts);
  const avgDisk = avg(diskPcts);
  const fmt = (v: number | null) => (v == null ? '-' : Math.round(v).toString());
  const maxHint = (arr: number[], v: number | null) => (v == null ? '暂无上报' : `峰值 ${Math.round(Math.max(...arr))}%`);

  const columns: ColumnsType<WorkerView> = [
    { title: '地址', dataIndex: 'worker_address' },
    {
      title: '协议',
      dataIndex: 'protocol',
      width: 110,
      render: (v: string) => <Tag color={v === 'powerjob' ? 'purple' : 'blue'}>{v || '-'}</Tag>,
    },
    {
      title: '标签',
      dataIndex: 'tags',
      render: (tags?: string[]) => (tags && tags.length ? tags.map((t) => <Tag key={t}>{t}</Tag>) : '-'),
    },
    { title: 'Score', dataIndex: 'score', width: 80, render: (v?: number) => v ?? '-' },
    { title: '资源占用', width: 210, render: (_, r) => <ResourceBar worker={r} /> },
    { title: '最后心跳', dataIndex: 'last_heartbeat', render: formatTime, width: 180 },
  ];

  return (
    <section className="view">
      <AppGate appId={props.appId} />
      <div className="view-head">
        <div>
          <Title level={3}>Worker 节点</Title>
          <Text type="secondary">当前应用在线节点(内存注册表,不入库)。选址按 tag 匹配 + score 择优。</Text>
        </div>
      </div>
      <div className="stat-row">
        <StatCard label="在线 Worker" value={workers.length} hint="内存注册表节点数" icon={<CloudServerOutlined />} tint="#1677ff" loading={loading} />
        <StatCard
          label="平均 CPU"
          value={fmt(avgCpu)}
          unit="%"
          hint={maxHint(cpuPcts, avgCpu)}
          icon={<DashboardOutlined />}
          tint={avgCpu == null ? '#1677ff' : healthColor(avgCpu)}
          loading={loading}
        />
        <StatCard
          label="平均内存"
          value={fmt(avgMem)}
          unit="%"
          hint={maxHint(memPcts, avgMem)}
          icon={<DatabaseOutlined />}
          tint={avgMem == null ? '#722ed1' : healthColor(avgMem)}
          loading={loading}
        />
        <StatCard
          label="平均磁盘"
          value={fmt(avgDisk)}
          unit="%"
          hint={maxHint(diskPcts, avgDisk)}
          icon={<HddOutlined />}
          tint={avgDisk == null ? '#13c2c2' : healthColor(avgDisk)}
          loading={loading}
        />
      </div>
      <SectionCard
        title="在线节点"
        sub="CPU/内存/磁盘 进度条颜色随占用变化(绿 <70% / 橘 <90% / 红 ≥90%)"
        extra={
          <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>
            刷新
          </Button>
        }
      >
        <Table rowKey="worker_address" columns={columns} dataSource={workers} loading={loading} pagination={false} scroll={{ x: 1000 }} />
      </SectionCard>
    </section>
  );
}
