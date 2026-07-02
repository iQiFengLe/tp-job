import {
  ApiOutlined,
  AppstoreOutlined,
  BugOutlined,
  ClockCircleOutlined,
  CloudServerOutlined,
  DeleteOutlined,
  EditOutlined,
  FileTextOutlined,
  LoginOutlined,
  LogoutOutlined,
  PlayCircleOutlined,
  PlusOutlined,
  ReloadOutlined,
} from '@ant-design/icons';
import {
  App as AntApp,
  Badge,
  Button,
  Card,
  DatePicker,
  Descriptions,
  Drawer,
  Empty,
  Form,
  Input,
  InputNumber,
  Layout,
  Menu,
  Modal,
  Popconfirm,
  Select,
  Space,
  Spin,
  Switch,
  Table,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import type { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import dayjs from 'dayjs';
import { useEffect, useMemo, useState } from 'react';
import { ApiError, api, clearToken, setToken, setUnauthorizedHandler } from './api';
import type {
  AppUpdateValues,
  AppView,
  InstanceView,
  JobCreateValues,
  JobUpdateValues,
  JobView,
  LoginReq,
  MeResp,
  Role,
  ScheduleKind,
  WorkerView,
} from './types';

const { Header, Sider, Content } = Layout;
const { Text, Title } = Typography;

const TOKEN_KEY = 'task-schedule-token';
const PAGE_SIZE = 20;

// 8 态状态机颜色(见 docs/refactor-unified-model.md §5)
const statusColor: Record<string, string> = {
  queued: 'default',
  waiting_receive: 'processing',
  running: 'processing',
  success: 'success',
  failed: 'error',
  skipped: 'default',
  canceled: 'default',
  stopped: 'warning',
};
const statusOptions = ['queued', 'waiting_receive', 'running', 'success', 'failed', 'skipped', 'canceled', 'stopped'].map(
  (value) => ({ label: value, value }),
);

const scheduleKindOptions: { label: string; value: ScheduleKind }[] = [
  { label: 'Cron', value: 'cron' },
  { label: 'Fix Rate', value: 'fix_rate' },
  { label: 'Fix Delay', value: 'fix_delay' },
  { label: 'Delay', value: 'delay' },
  { label: 'Run At', value: 'run_at' },
  { label: 'Manual', value: 'manual' },
];

type ViewKey = 'apps' | 'jobs' | 'instances' | 'workers';

function loadToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}
function saveToken(token: string) {
  localStorage.setItem(TOKEN_KEY, token);
}
function removeToken() {
  localStorage.removeItem(TOKEN_KEY);
}

function formatTime(value?: string) {
  return value ? dayjs(value).format('YYYY-MM-DD HH:mm:ss') : '-';
}

// 紧凑化:剔除 undefined/null/'' 字段,适配部分更新。
function compactObject<T extends Record<string, unknown>>(values: T) {
  return Object.fromEntries(
    Object.entries(values).filter(([, value]) => value !== undefined && value !== null && value !== ''),
  ) as Partial<T>;
}

function useErrorHandler() {
  const { message } = AntApp.useApp();
  return (error: unknown) => {
    const text = error instanceof ApiError || error instanceof Error ? error.message : '操作失败';
    message.error(text);
  };
}

export default function App() {
  const [me, setMe] = useState<MeResp | null>(null);
  const [booting, setBooting] = useState(true);

  // 401 → 清身份回登录页
  useEffect(() => {
    setUnauthorizedHandler(() => {
      removeToken();
      setToken(null);
      setMe(null);
    });
    return () => setUnauthorizedHandler(null);
  }, []);

  // 启动:有 token 则 me() 校验;无则直接登录页
  useEffect(() => {
    const tok = loadToken();
    if (!tok) {
      setBooting(false);
      return;
    }
    setToken(tok);
    api.auth
      .me()
      .then((m) => setMe(m))
      .catch(() => {
        removeToken();
        setToken(null);
      })
      .finally(() => setBooting(false));
  }, []);

  const onLoggedIn = (token: string, m: MeResp) => {
    saveToken(token);
    setToken(token);
    setMe(m);
  };
  const onLoggedOut = async () => {
    try {
      await api.auth.logout();
    } catch {
      // 忽略:本地 token 无论如何清掉
    }
    removeToken();
    clearToken();
    setMe(null);
  };

  if (booting) {
    return (
      <div className="boot">
        <Spin size="large" />
      </div>
    );
  }
  if (!me) {
    return <LoginView onLoggedIn={onLoggedIn} />;
  }
  return <Console me={me} onLoggedOut={onLoggedOut} />;
}

// ===== 登录页 =====

function LoginView(props: { onLoggedIn: (token: string, me: MeResp) => void }) {
  const handleError = useErrorHandler();
  const [form] = Form.useForm<LoginReq>();
  const [loading, setLoading] = useState(false);

  const submit = async (values: LoginReq) => {
    setLoading(true);
    try {
      const resp = await api.auth.login(values);
      props.onLoggedIn(resp.token, {
        role: resp.role as Role,
        username: resp.username,
        app_id: resp.app_id,
        app_name: resp.app_name,
      });
    } catch (error) {
      handleError(error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="login-page">
      <Card className="login-card">
        <div className="login-brand">
          <ApiOutlined />
          <div>
            <strong>Task Schedule</strong>
            <span>调度管理台</span>
          </div>
        </div>
        <Form form={form} layout="vertical" onFinish={submit} initialValues={{ ident: '', password: '' }}>
          <Form.Item name="ident" label="账户" rules={[{ required: true, message: '请输入管理员用户名或应用名' }]}>
            <Input placeholder="管理员用户名 / app 名" autoFocus />
          </Form.Item>
          <Form.Item name="password" label="密码" rules={[{ required: true }]}>
            <Input.Password />
          </Form.Item>
          <Button type="primary" htmlType="submit" block loading={loading} icon={<LoginOutlined />}>
            登录
          </Button>
        </Form>
      </Card>
    </div>
  );
}

// ===== 控制台 =====

function Console(props: { me: MeResp; onLoggedOut: () => void }) {
  const handleError = useErrorHandler();
  const { me } = props;
  const isAdmin = me.role === 'admin';
  const [view, setView] = useState<ViewKey>(isAdmin ? 'apps' : 'jobs');
  const [apps, setApps] = useState<AppView[]>([]);
  const [selectedAppId, setSelectedAppId] = useState<number | undefined>(me.app_id);
  const [appsLoading, setAppsLoading] = useState(false);
  const [health, setHealth] = useState<{ status: string; driver: string }>();

  const loadApps = async () => {
    if (!isAdmin) return;
    setAppsLoading(true);
    try {
      const data = await api.apps.list({ page: 1, size: 200 });
      setApps(data.list || []);
      if (selectedAppId === undefined && data.list?.[0]) {
        setSelectedAppId(data.list[0].id);
      }
    } catch (error) {
      handleError(error);
    } finally {
      setAppsLoading(false);
    }
  };

  useEffect(() => {
    api.health().then(setHealth).catch(() => undefined);
  }, []);
  useEffect(() => {
    if (isAdmin) void loadApps();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const currentAppId = isAdmin ? selectedAppId : me.app_id;
  const currentApp = apps.find((item) => item.id === selectedAppId);

  const menuItems = [
    ...(isAdmin ? [{ key: 'apps', icon: <AppstoreOutlined />, label: '应用' }] : []),
    { key: 'jobs', icon: <ClockCircleOutlined />, label: '任务' },
    { key: 'instances', icon: <BugOutlined />, label: '实例' },
    { key: 'workers', icon: <CloudServerOutlined />, label: 'Worker' },
  ];

  return (
    <Layout className="shell">
      <Sider width={248} className="side">
        <div className="brand">
          <ApiOutlined />
          <div>
            <strong>Task Schedule</strong>
            <span>调度管理台</span>
          </div>
        </div>
        <Menu theme="dark" mode="inline" selectedKeys={[view]} onClick={({ key }) => setView(key as ViewKey)} items={menuItems} />
      </Sider>
      <Layout>
        <Header className="topbar">
          <Space size={12} wrap>
            {isAdmin ? (
              <Select
                className="app-switch"
                placeholder="选择应用"
                value={selectedAppId}
                onChange={setSelectedAppId}
                options={apps.map((item) => ({ label: `${item.app_name} (${item.id})`, value: item.id }))}
                loading={appsLoading}
              />
            ) : (
              <Tag icon={<ApiOutlined />} color="blue">
                {me.app_name || `App ${me.app_id}`}
              </Tag>
            )}
            {isAdmin && currentApp && (
              <Tag color={currentApp.status === 1 ? 'green' : 'default'}>
                {currentApp.status === 1 ? '启用' : '禁用'}
              </Tag>
            )}
            {health && <Badge status={health.status === 'ok' ? 'success' : 'error'} text={`DB ${health.driver}`} />}
          </Space>
          <Space>
            <Tag color={isAdmin ? 'gold' : 'geekblue'}>{isAdmin ? '管理员' : '应用'}</Tag>
            <Button icon={<LogoutOutlined />} onClick={props.onLoggedOut}>
              登出
            </Button>
          </Space>
        </Header>
        <Content className="content">
          {view === 'apps' && isAdmin && <AppsView apps={apps} loading={appsLoading} onReload={loadApps} onError={handleError} />}
          {view === 'jobs' && (
            <JobsView appId={currentAppId} onError={handleError} />
          )}
          {view === 'instances' && <InstancesView appId={currentAppId} onError={handleError} />}
          {view === 'workers' && <WorkersView appId={currentAppId} onError={handleError} />}
        </Content>
      </Layout>
    </Layout>
  );
}

// ===== 应用管理(admin) =====

function AppsView(props: {
  apps: AppView[];
  loading: boolean;
  onReload: () => void;
  onError: (error: unknown) => void;
}) {
  const { message } = AntApp.useApp();
  const [form] = Form.useForm();
  const [modalOpen, setModalOpen] = useState(false);
  const [editingApp, setEditingApp] = useState<AppView>();

  const openCreate = () => {
    setEditingApp(undefined);
    form.resetFields();
    form.setFieldsValue({ status: 1 });
    setModalOpen(true);
  };

  const openEdit = (app: AppView) => {
    setEditingApp(app);
    form.setFieldsValue({ app_name: app.app_name, status: app.status });
    setModalOpen(true);
  };

  const submit = async () => {
    const values = await form.validateFields();
    try {
      if (editingApp) {
        await api.apps.update(editingApp.id, compactObject(values));
        message.success('应用已更新');
      } else {
        await api.apps.create(values as { app_name: string; password: string; status?: number });
        message.success('应用已创建');
      }
      setModalOpen(false);
      props.onReload();
    } catch (error) {
      props.onError(error);
    }
  };

  const remove = async (id: number) => {
    try {
      await api.apps.remove(id);
      message.success('应用已删除');
      props.onReload();
    } catch (error) {
      props.onError(error);
    }
  };

  const columns: ColumnsType<AppView> = [
    {
      title: '应用',
      dataIndex: 'app_name',
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <Text strong>{record.app_name}</Text>
          <Text type="secondary">ID {record.id}</Text>
        </Space>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      width: 100,
      render: (value: number) => <Tag color={value === 1 ? 'green' : 'default'}>{value === 1 ? '启用' : '禁用'}</Tag>,
    },
    { title: '创建时间', dataIndex: 'created_at', render: formatTime },
    { title: '更新时间', dataIndex: 'updated_at', render: formatTime },
    {
      title: '操作',
      width: 160,
      render: (_, record) => (
        <Space>
          <Button size="small" icon={<EditOutlined />} onClick={() => openEdit(record)} />
          <Popconfirm title="删除应用" description="仅无任务的应用可删除。" onConfirm={() => remove(record.id)}>
            <Button size="small" danger>
              删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <section className="view">
      <div className="view-head">
        <div>
          <Title level={3}>应用管理</Title>
          <Text type="secondary">维护接入应用(管理员)。App ID 自增,AppName 全局唯一兼作登录名。</Text>
        </div>
        <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
          新建应用
        </Button>
      </div>
      <div className="toolbar">
        <Button icon={<ReloadOutlined />} onClick={props.onReload} loading={props.loading}>
          刷新
        </Button>
      </div>
      <Table rowKey="id" columns={columns} dataSource={props.apps} loading={props.loading} pagination={false} />

      <Modal title={editingApp ? '编辑应用' : '新建应用'} open={modalOpen} onOk={submit} onCancel={() => setModalOpen(false)} destroyOnClose>
        <Form form={form} layout="vertical">
          <Form.Item name="app_name" label="应用名称" rules={[{ required: true, message: '请输入应用名称' }]}>
            <Input />
          </Form.Item>
          <Form.Item name="password" label={editingApp ? '新密码' : '密码'} rules={editingApp ? [] : [{ required: true, message: '请输入密码' }]}>
            <Input.Password placeholder={editingApp ? '留空不修改' : undefined} />
          </Form.Item>
          <Form.Item name="status" label="状态">
            <Select
              options={[
                { label: '启用', value: 1 },
                { label: '禁用', value: 0 },
              ]}
            />
          </Form.Item>
        </Form>
      </Modal>
    </section>
  );
}

// ===== 任务(Job)=====

function JobsView(props: { appId?: number; onError: (error: unknown) => void }) {
  const { message } = AntApp.useApp();
  const [form] = Form.useForm();
  const [jobs, setJobs] = useState<JobView[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [size, setSize] = useState(PAGE_SIZE);
  const [loading, setLoading] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [editingJob, setEditingJob] = useState<JobView>();
  const [detailJob, setDetailJob] = useState<JobView>();

  const load = async (p = page, s = size) => {
    if (!props.appId) return;
    setLoading(true);
    try {
      const data = await api.jobs.list(props.appId, { page: p, size: s });
      setJobs(data.list || []);
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
  }, [props.appId]);

  const openCreate = () => {
    setEditingJob(undefined);
    form.resetFields();
    form.setFieldsValue({
      schedule_kind: 'manual',
      timeout_sec: 30,
      retry_count: 0,
      retry_interval_sec: 0,
      max_concurrency: 1,
      max_wait_seconds: 0,
      default_priority: 0,
      enabled: true,
    });
    setModalOpen(true);
  };

  const openEdit = (job: JobView) => {
    setEditingJob(job);
    form.setFieldsValue({
      ...job,
      run_at: job.schedule_kind === 'run_at' && job.schedule_expr ? dayjs(job.schedule_expr) : undefined,
    });
    setModalOpen(true);
  };

  const submit = async () => {
    if (!props.appId) return;
    const raw = await form.validateFields();
    // schedule_expr 按 kind 整理:run_at→ISO;fix_rate/fix_delay/delay→String(number);cron→原值;manual→忽略
    const expr = scheduleExprFromForm(raw.schedule_kind, raw.schedule_expr, raw.run_at);
    const values = compactObject({
      name: raw.name,
      schedule_kind: raw.schedule_kind,
      schedule_expr: expr,
      job_params: raw.job_params,
      tag: raw.tag,
      timeout_sec: raw.timeout_sec,
      max_concurrency: raw.max_concurrency,
      max_wait_seconds: raw.max_wait_seconds,
      retry_count: raw.retry_count,
      retry_interval_sec: raw.retry_interval_sec,
      default_priority: raw.default_priority,
      enabled: raw.enabled,
    });
    try {
      if (editingJob) {
        await api.jobs.update(props.appId, editingJob.id, values as JobUpdateValues);
        message.success('任务已更新');
      } else {
        await api.jobs.create(props.appId, values as JobCreateValues);
        message.success('任务已创建');
      }
      setModalOpen(false);
      load();
    } catch (error) {
      props.onError(error);
    }
  };

  const remove = async (id: number) => {
    if (!props.appId) return;
    try {
      await api.jobs.remove(props.appId, id);
      message.success('任务已删除');
      load();
    } catch (error) {
      props.onError(error);
    }
  };

  const trigger = async (id: number) => {
    if (!props.appId) return;
    try {
      await api.jobs.trigger(props.appId, id, {});
      message.success('已触发任务');
    } catch (error) {
      props.onError(error);
    }
  };

  const columns: ColumnsType<JobView> = [
    {
      title: '任务',
      dataIndex: 'name',
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <Button type="link" className="link-button" onClick={() => setDetailJob(record)}>
            {record.name}
          </Button>
          <Text type="secondary">ID {record.id}</Text>
        </Space>
      ),
    },
    {
      title: '调度',
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <Tag>{record.schedule_kind}</Tag>
          <Text type="secondary">{formatScheduleExpr(record)}</Text>
        </Space>
      ),
    },
    { title: '下次执行', dataIndex: 'next_run_time', render: formatTime },
    {
      title: '状态',
      dataIndex: 'enabled',
      width: 90,
      render: (value: boolean) => <Tag color={value ? 'green' : 'default'}>{value ? '启用' : '停用'}</Tag>,
    },
    {
      title: '操作',
      width: 200,
      render: (_, record) => (
        <Space>
          <Tooltip title="触发">
            <Button size="small" icon={<PlayCircleOutlined />} onClick={() => trigger(record.id)} />
          </Tooltip>
          <Tooltip title="编辑">
            <Button size="small" icon={<EditOutlined />} onClick={() => openEdit(record)} />
          </Tooltip>
          <Popconfirm title="删除任务" onConfirm={() => remove(record.id)}>
            <Button size="small" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <section className="view">
      <AppGate appId={props.appId} />
      <div className="view-head">
        <div>
          <Title level={3}>任务管理</Title>
          <Text type="secondary">维护 Job(cron / fix_rate / fix_delay / delay / run_at / manual),支持手动触发。</Text>
        </div>
        <Button type="primary" icon={<PlusOutlined />} onClick={openCreate} disabled={!props.appId}>
          新建任务
        </Button>
      </div>
      <div className="toolbar">
        <Button icon={<ReloadOutlined />} onClick={() => load()} loading={loading}>
          刷新
        </Button>
      </div>
      <Table
        rowKey="id"
        columns={columns}
        dataSource={jobs}
        loading={loading}
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
      <JobModal open={modalOpen} editingJob={editingJob} form={form} onCancel={() => setModalOpen(false)} onOk={submit} />
      <JobDetailDrawer job={detailJob} onClose={() => setDetailJob(undefined)} />
    </section>
  );
}

function scheduleExprFromForm(kind: ScheduleKind | undefined, expr: unknown, runAt: unknown): string | undefined {
  switch (kind) {
    case 'cron':
      return (expr as string) || undefined;
    case 'fix_rate':
    case 'fix_delay':
    case 'delay':
      return expr === undefined || expr === null || expr === '' ? undefined : String(expr);
    case 'run_at':
      return runAt ? (runAt as dayjs.Dayjs).toISOString() : undefined;
    case 'manual':
      return undefined;
    default:
      return undefined;
  }
}

function formatScheduleExpr(job: JobView): string {
  switch (job.schedule_kind) {
    case 'fix_rate':
    case 'fix_delay':
      return job.schedule_expr ? `${job.schedule_expr}ms` : '-';
    case 'delay':
      return job.schedule_expr ? `${job.schedule_expr}s` : '-';
    case 'run_at':
      return formatTime(job.schedule_expr);
    case 'cron':
      return job.schedule_expr || '-';
    default:
      return '-';
  }
}

function JobModal(props: {
  open: boolean;
  editingJob?: JobView;
  form: ReturnType<typeof Form.useForm>[0];
  onCancel: () => void;
  onOk: () => void;
}) {
  const kind = Form.useWatch('schedule_kind', props.form) as ScheduleKind | undefined;
  return (
    <Modal title={props.editingJob ? '编辑任务' : '新建任务'} open={props.open} onCancel={props.onCancel} onOk={props.onOk} width={780} destroyOnClose>
      <Form form={props.form} layout="vertical">
        <div className="form-grid">
          <Form.Item name="name" label="任务名称" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="schedule_kind" label="调度类型" rules={[{ required: true }]}>
            <Select options={scheduleKindOptions} />
          </Form.Item>
          {kind === 'cron' && (
            <Form.Item name="schedule_expr" label="Cron 表达式" rules={[{ required: true }]}>
              <Input placeholder="0 9 * * *" />
            </Form.Item>
          )}
          {kind === 'fix_rate' && (
            <Form.Item name="schedule_expr" label="固定频率(毫秒)" rules={[{ required: true }]}>
              <InputNumber min={1} className="full" />
            </Form.Item>
          )}
          {kind === 'fix_delay' && (
            <Form.Item name="schedule_expr" label="固定延迟(毫秒)" rules={[{ required: true }]}>
              <InputNumber min={1} className="full" />
            </Form.Item>
          )}
          {kind === 'delay' && (
            <Form.Item name="schedule_expr" label="延迟秒数" rules={[{ required: true }]}>
              <InputNumber min={1} className="full" />
            </Form.Item>
          )}
          {kind === 'run_at' && (
            <Form.Item name="run_at" label="计划执行时间" rules={[{ required: true }]}>
              <DatePicker showTime className="full" />
            </Form.Item>
          )}
        </div>
        <Form.Item name="job_params" label="任务参数(job_params)">
          <Input.TextArea rows={3} placeholder="随每次执行下发的参数字符串" />
        </Form.Item>
        <div className="form-grid">
          <Form.Item name="tag" label="标签(worker 匹配)">
            <Input placeholder="如 gpu / highmem" />
          </Form.Item>
          <Form.Item name="timeout_sec" label="超时秒数">
            <InputNumber min={1} className="full" />
          </Form.Item>
          <Form.Item name="max_concurrency" label="最大并发">
            <InputNumber min={1} className="full" />
          </Form.Item>
          <Form.Item name="max_wait_seconds" label="排队等待秒">
            <InputNumber min={0} className="full" />
          </Form.Item>
          <Form.Item name="retry_count" label="重试次数">
            <InputNumber min={0} className="full" />
          </Form.Item>
          <Form.Item name="retry_interval_sec" label="重试间隔秒">
            <InputNumber min={0} className="full" />
          </Form.Item>
          <Form.Item name="default_priority" label="默认优先级">
            <InputNumber className="full" />
          </Form.Item>
          <Form.Item name="enabled" label="启用" valuePropName="checked">
            <Switch />
          </Form.Item>
        </div>
      </Form>
    </Modal>
  );
}

function JobDetailDrawer(props: { job?: JobView; onClose: () => void }) {
  const job = props.job;
  return (
    <Drawer open={Boolean(job)} onClose={props.onClose} width={600} title="任务详情">
      {job && (
        <Descriptions column={1} bordered size="small">
          <Descriptions.Item label="ID">{job.id}</Descriptions.Item>
          <Descriptions.Item label="名称">{job.name}</Descriptions.Item>
          <Descriptions.Item label="调度">{job.schedule_kind} · {formatScheduleExpr(job)}</Descriptions.Item>
          <Descriptions.Item label="标签">{job.tag || '-'}</Descriptions.Item>
          <Descriptions.Item label="任务参数">{job.job_params || '-'}</Descriptions.Item>
          <Descriptions.Item label="下次执行">{formatTime(job.next_run_time)}</Descriptions.Item>
          <Descriptions.Item label="创建时间">{formatTime(job.created_at)}</Descriptions.Item>
          <Descriptions.Item label="更新时间">{formatTime(job.updated_at)}</Descriptions.Item>
        </Descriptions>
      )}
    </Drawer>
  );
}

// ===== 实例 =====

function InstancesView(props: { appId?: number; onError: (error: unknown) => void }) {
  const [list, setList] = useState<InstanceView[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [size, setSize] = useState(PAGE_SIZE);
  const [loading, setLoading] = useState(false);
  const [filters, setFilters] = useState<{ job_id?: number; status?: string }>({});
  const [logLines, setLogLines] = useState<string[]>([]);
  const [logTitle, setLogTitle] = useState('');
  const [logOpen, setLogOpen] = useState(false);

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
      const data = await api.instances.logs(props.appId, instance.id, { group: true, limit: 500 });
      setLogLines(data.lines || []);
      setLogTitle(`实例 ${instance.id} 日志(同 root 聚合,共 ${data.total} 行)`);
      setLogOpen(true);
    } catch (error) {
      props.onError(error);
    }
  };

  const columns: ColumnsType<InstanceView> = [
    { title: '实例 ID', dataIndex: 'id', width: 90 },
    { title: 'Job ID', dataIndex: 'job_id', width: 90 },
    { title: '触发', dataIndex: 'trigger_type', width: 90, render: (v) => v || '-' },
    { title: '重试', dataIndex: 'retry_index', width: 70, render: (v) => v || 0 },
    {
      title: '状态',
      dataIndex: 'status',
      width: 130,
      render: (value: string) => <Tag color={statusColor[value] || 'default'}>{value}</Tag>,
    },
    { title: 'Worker', dataIndex: 'worker_address', width: 150, render: (v) => v || '-' },
    { title: '耗时', dataIndex: 'duration_ms', width: 90, render: (v) => (v ? `${v}ms` : '-') },
    { title: '触发时间', dataIndex: 'trigger_time', render: formatTime },
    {
      title: '操作',
      width: 100,
      render: (_, record) => (
        <Button size="small" icon={<FileTextOutlined />} onClick={() => openLogs(record)}>
          日志
        </Button>
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
        dataSource={list}
        loading={loading}
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
    </section>
  );
}

// ===== Worker(在线节点)=====

function WorkersView(props: { appId?: number; onError: (error: unknown) => void }) {
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
      render: (v: string) => <Tag color={v === 'powerjob' ? 'purple' : 'blue'}>{v || '-'}</Tag>,
    },
    {
      title: '标签',
      dataIndex: 'tags',
      render: (tags?: string[]) => (tags && tags.length ? tags.map((t) => <Tag key={t}>{t}</Tag>) : '-'),
    },
    { title: 'Score', dataIndex: 'score', width: 80, render: (v?: number) => v ?? '-' },
    {
      title: 'CPU',
      width: 130,
      render: (_, r) => (r.cpu_processors ? `${(r.cpu_load ?? 0).toFixed(2)} / ${r.cpu_processors}核` : '-'),
    },
    { title: '最后心跳', dataIndex: 'last_heartbeat', render: formatTime },
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
      <Table rowKey="worker_address" columns={columns} dataSource={workers} loading={loading} pagination={false} />
    </section>
  );
}

// ===== 日志抽屉(渲染字符串行)=====

function LogsDrawer(props: { title: string; open: boolean; lines: string[]; onClose: () => void }) {
  return (
    <Drawer title={props.title} open={props.open} onClose={props.onClose} width={720}>
      {props.lines.length ? (
        <pre className="log-pre">{props.lines.join('\n')}</pre>
      ) : (
        <Empty description="暂无日志" />
      )}
    </Drawer>
  );
}

// ===== 通用 =====

function AppGate(props: { appId?: number }) {
  if (props.appId) return null;
  return (
    <div className="gate">
      <Empty description={adminHint()} />
    </div>
  );
}

function adminHint() {
  return '请先选择一个应用';
}
