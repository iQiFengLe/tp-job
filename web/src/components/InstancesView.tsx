import { CheckCircleOutlined, FileTextOutlined, InfoCircleOutlined, RedoOutlined, ReloadOutlined, StopOutlined, SyncOutlined, UnorderedListOutlined, WarningOutlined } from '@ant-design/icons';
import { App as AntApp, Button, InputNumber, Popconfirm, Select, Space, Table, Tooltip, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useEffect, useState } from 'react';
import { api } from '../api';
import { PAGE_SIZE, formatDuration, formatTime, statusLabel, statusOptions, triggerTypeLabel } from '../lib';
import { scheduleKindOptions } from '../schedule';
import type { InstanceView } from '../types';
import AppGate from './AppGate';
import InstanceDetailDrawer from './InstanceDetailDrawer';
import LogsDrawer from './LogsDrawer';
import SectionCard from './SectionCard';
import StatCard from './StatCard';
import StatusDistribution, { STATUS_HEX } from './StatusDistribution';

const { Text, Title } = Typography;

// 终态实例不可停止(已无在飞槽位);仅 failed/timeout 可重试。与后端 InstanceService.Stop/Retry 语义对齐。
const TERMINAL_STATUSES = ['success', 'failed', 'timeout', 'skipped', 'canceled', 'stopped'];
const isStoppable = (s: string) => !TERMINAL_STATUSES.includes(s);
const isRetryable = (s: string) => s === 'failed' || s === 'timeout';

// 状态圆点 + 文字(替代旧版 Tag,更素净的现代极简风)
function StatusBadge({ status }: { status: string }) {
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
      <i
        style={{
          width: 8,
          height: 8,
          borderRadius: '50%',
          background: STATUS_HEX[status] || '#bfbfbf',
          display: 'inline-block',
          flex: 'none',
        }}
      />
      {statusLabel[status] || status}
    </span>
  );
}

export default function InstancesView(props: { appId?: number; onError: (error: unknown) => void }) {
  const [list, setList] = useState<InstanceView[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [size, setSize] = useState(PAGE_SIZE);
  const [loading, setLoading] = useState(false);
  const [filters, setFilters] = useState<{ job_id?: number; status?: string }>({});
  // 状态分布用"最近一批"实例前端聚合,零后端改动(语义=近期概览,对监控仪表盘足够)
  const [stats, setStats] = useState<InstanceView[]>([]);
  const [logLines, setLogLines] = useState<string[]>([]);
  const [logTitle, setLogTitle] = useState('');
  const [logOpen, setLogOpen] = useState(false);
  const [detailInstance, setDetailInstance] = useState<InstanceView>();
  const { message } = AntApp.useApp();

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

  // 取最近一批实例用于状态分布/成功率,独立于列表分页与筛选(始终代表应用整体)。
  const loadStats = async () => {
    if (!props.appId) return;
    try {
      const data = await api.instances.list(props.appId, { page: 1, size: 100 });
      setStats(data.list || []);
    } catch {
      // 分布条非关键,失败静默(列表请求已会报错)
    }
  };

  useEffect(() => {
    setPage(1);
    if (props.appId) void load(1, size);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [props.appId, filters.job_id, filters.status]);

  useEffect(() => {
    if (props.appId) void loadStats();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [props.appId]);

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

  const stopInstance = async (instance: InstanceView) => {
    if (!props.appId) return;
    try {
      await api.instances.stop(props.appId, instance.id);
      message.success(`实例 ${instance.id} 已停止`);
      load();
      loadStats();
    } catch (error) {
      props.onError(error);
    }
  };

  const retryInstance = async (instance: InstanceView) => {
    if (!props.appId) return;
    try {
      await api.instances.retry(props.appId, instance.id);
      message.success(`实例 ${instance.id} 已重新加入重试队列`);
      load();
      loadStats();
    } catch (error) {
      props.onError(error);
    }
  };

  const refresh = () => {
    load();
    loadStats();
  };

  // 状态分布聚合 + 成功率(终态中 success 占比,分母=success+failed+timeout)
  const counts = stats.reduce<Record<string, number>>((acc, ins) => {
    acc[ins.status] = (acc[ins.status] || 0) + 1;
    return acc;
  }, {});
  const success = counts.success || 0;
  const failed = counts.failed || 0;
  const timeout = counts.timeout || 0;
  const running = (counts.running || 0) + (counts.waiting_receive || 0) + (counts.queued || 0);
  const finished = success + failed + timeout;
  const successRate = finished ? Math.round((success / finished) * 100) : null;
  const rateTint = successRate == null ? '#1677ff' : successRate >= 95 ? '#52c41a' : successRate >= 80 ? '#faad14' : '#ff4d4f';

  const columns: ColumnsType<InstanceView> = [
    { title: '实例 ID', dataIndex: 'id', width: 100 },
    { title: 'Job ID', dataIndex: 'job_id', width: 100 },
    { title: '调度类型', dataIndex: 'schedule_kind', width: 110, render: (v: string) => scheduleKindOptions.find((o) => o.value === v)?.label || v || '-' },
    { title: '触发', dataIndex: 'trigger_type', width: 100, render: (v: string) => triggerTypeLabel[v] || v || '-' },
    { title: '优先级', dataIndex: 'priority', width: 80, render: (v?: number) => (v ? <span style={{ fontWeight: 600 }}>{v}</span> : '-') },
    { title: '重试', dataIndex: 'retry_index', width: 80, render: (v) => v || 0 },
    { title: '状态', dataIndex: 'status', width: 130, render: (v: string) => <StatusBadge status={v} /> },
    { title: 'Worker', dataIndex: 'worker_address', width: 180, render: (v) => v || '-' },
    { title: '耗时', dataIndex: 'duration_ms', width: 100, render: formatDuration },
    { title: '触发时间', dataIndex: 'trigger_time', render: formatTime, width: 180 },
    {
      title: '操作',
      width: 180,
      fixed: 'right',
      render: (_, record) => (
        <Space size="small">
          <Tooltip title="详情">
            <Button size="small" icon={<InfoCircleOutlined />} onClick={() => setDetailInstance(record)} />
          </Tooltip>
          <Tooltip title="查看日志">
            <Button size="small" icon={<FileTextOutlined />} onClick={() => openLogs(record)} />
          </Tooltip>
          {isStoppable(record.status) && (
            <Popconfirm title={`停止实例 ${record.id}?`} onConfirm={() => stopInstance(record)}>
              <Button size="small" danger icon={<StopOutlined />}>
                停止
              </Button>
            </Popconfirm>
          )}
          {isRetryable(record.status) && (
            <Popconfirm title={`重试实例 ${record.id}?`} onConfirm={() => retryInstance(record)}>
              <Button size="small" type="primary" ghost icon={<RedoOutlined />}>
                重试
              </Button>
            </Popconfirm>
          )}
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
          <Text type="secondary">查看执行记录(9 态)与实例日志(同 root 聚合的完整时间线)。</Text>
        </div>
      </div>
      <div className="stat-row">
        <StatCard
          label="成功率"
          value={successRate == null ? '-' : successRate}
          unit={successRate == null ? undefined : '%'}
          hint={successRate == null ? '暂无终态样本' : `成功 ${success} / 终态 ${finished}`}
          icon={<CheckCircleOutlined />}
          tint={rateTint}
          loading={loading}
        />
        <StatCard
          label="运行中"
          value={running}
          hint={`排队 ${counts.queued || 0} · 等待 ${counts.waiting_receive || 0}`}
          icon={<SyncOutlined />}
          tint="#1677ff"
          loading={loading}
        />
        <StatCard
          label="失败 / 超时"
          value={failed + timeout}
          hint={`失败 ${failed} · 超时 ${timeout}`}
          icon={<WarningOutlined />}
          tint="#ff4d4f"
          loading={loading}
        />
        <StatCard label="近期实例" value={stats.length} hint={`最近 ${stats.length} 条取样`} icon={<UnorderedListOutlined />} tint="#722ed1" loading={loading} />
      </div>
      <StatusDistribution counts={counts} note={`近 ${stats.length} 条实例的状态分布`} />
      <SectionCard
        title="实例列表"
        sub={`共 ${total} 条`}
        extra={
          <Button icon={<ReloadOutlined />} onClick={refresh} loading={loading}>
            刷新
          </Button>
        }
      >
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
        </div>
        <Table
          rowKey="id"
          columns={columns}
          className="table-container"
          dataSource={list}
          loading={loading}
          scroll={{ x: 1650 }}
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
      </SectionCard>
      <LogsDrawer title={logTitle} open={logOpen} lines={logLines} onClose={() => setLogOpen(false)} />
      <InstanceDetailDrawer instance={detailInstance} onClose={() => setDetailInstance(undefined)} onShowLogs={openLogs} />
    </section>
  );
}
