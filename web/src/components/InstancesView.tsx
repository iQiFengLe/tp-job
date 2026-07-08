import { FileTextOutlined, InfoCircleOutlined, ReloadOutlined } from '@ant-design/icons';
import { Button, InputNumber, Select, Space, Table, Tag, Tooltip, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useEffect, useState } from 'react';
import { api } from '../api';
import { PAGE_SIZE, formatTime, statusColor, statusLabel, statusOptions, triggerTypeLabel } from '../lib';
import { scheduleKindOptions } from '../schedule';
import type { InstanceView } from '../types';
import AppGate from './AppGate';
import InstanceDetailDrawer from './InstanceDetailDrawer';
import LogsDrawer from './LogsDrawer';

const { Text, Title } = Typography;

export default function InstancesView(props: { appId?: number; onError: (error: unknown) => void }) {
  const [list, setList] = useState<InstanceView[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [size, setSize] = useState(PAGE_SIZE);
  const [loading, setLoading] = useState(false);
  const [filters, setFilters] = useState<{ job_id?: number; status?: string }>({});
  const [logLines, setLogLines] = useState<string[]>([]);
  const [logTitle, setLogTitle] = useState('');
  const [logOpen, setLogOpen] = useState(false);
  const [detailInstance, setDetailInstance] = useState<InstanceView>();

  const load = async (p = page, s = size) => {
    if (!props.appId) return;
    setLoading(true);
    try {
      const data = await api.instances.list(props.appId, { ...filters, page: p, size: s });
      setList(data.list || []);
      setTotal(data.total);
    } catch (error) {
      props.onError(error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    setPage(1);
    if (props.appId) void load(1, size);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [props.appId, filters.job_id, filters.status]);

  const openLogs = async (instance: InstanceView) => {
    if (!props.appId) return;
    try {
      const data = await api.instances.logs(props.appId, instance.id, { limit: 500 });
      setLogLines(data.lines || []);
      setLogTitle(`实例 ${instance.id} 日志(共 ${data.total} 行)`);
      setLogOpen(true);
    } catch (error) {
      props.onError(error);
    }
  };

  const columns: ColumnsType<InstanceView> = [
    { title: '实例 ID', dataIndex: 'id', width: 100 },
    { title: 'Job ID', dataIndex: 'job_id', width: 100 },
    { title: '调度类型', dataIndex: 'schedule_kind', width: 110, render: (v: string) => scheduleKindOptions.find((o) => o.value === v)?.label || v || '-' },
    { title: '触发', dataIndex: 'trigger_type', width: 100, render: (v: string) => triggerTypeLabel[v] || v || '-' },
    { title: '重试', dataIndex: 'retry_index', width: 80, render: (v) => v || 0 },
    {
      title: '状态',
      dataIndex: 'status',
      width: 140,
      render: (value: string) => (
        <Tag color={statusColor[value] || 'default'} style={{ fontWeight: 500 }}>
          {statusLabel[value] || value}
        </Tag>
      ),
    },
    { title: 'Worker', dataIndex: 'worker_address', width: 180, render: (v) => v || '-' },
    { title: '耗时', dataIndex: 'duration_ms', width: 100, render: (v) => (v ? `${v}ms` : '-') },
    { title: '触发时间', dataIndex: 'trigger_time', render: formatTime, width: 180 },
    {
      title: '操作',
      width: 110,
      render: (_, record) => (
        <Space size="small">
          <Tooltip title="详情">
            <Button size="small" icon={<InfoCircleOutlined />} onClick={() => setDetailInstance(record)} />
          </Tooltip>
          <Tooltip title="查看日志">
            <Button size="small" icon={<FileTextOutlined />} onClick={() => openLogs(record)} />
          </Tooltip>
        </Space>
      ),
    },
  ];

  return (
    <section className="view">
      <AppGate appId={props.appId} />
      <div className="view-head">
        <div>
          <Title level={3}>任务实例</Title>
          <Text type="secondary">查看执行记录(8 态)与实例日志(同 root 聚合的完整时间线)。</Text>
        </div>
      </div>
      <div className="toolbar">
        <InputNumber
          placeholder="Job ID"
          min={1}
          value={filters.job_id}
          onChange={(value) => setFilters((prev) => ({ ...prev, job_id: typeof value === 'number' ? value : undefined }))}
        />
        <Select
          allowClear
          placeholder="状态"
          value={filters.status}
          onChange={(value) => setFilters((prev) => ({ ...prev, status: value }))}
          options={statusOptions}
          className="toolbar-select"
        />
        <Button icon={<ReloadOutlined />} onClick={() => load()} loading={loading}>
          刷新
        </Button>
      </div>
      <Table
        rowKey="id"
        columns={columns}
        className="table-container"
        dataSource={list}
        loading={loading}
        scroll={{ x: 1400 }}
        pagination={{
          current: page,
          pageSize: size,
          total,
          showSizeChanger: true,
          onChange: (p, s) => {
            setPage(p);
            setSize(s);
            load(p, s);
          },
        }}
      />
      <LogsDrawer title={logTitle} open={logOpen} lines={logLines} onClose={() => setLogOpen(false)} />
      <InstanceDetailDrawer instance={detailInstance} onClose={() => setDetailInstance(undefined)} onShowLogs={openLogs} />
    </section>
  );
}
