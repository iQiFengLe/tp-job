import { ApiOutlined, AppstoreOutlined, BugOutlined, ClockCircleOutlined, CloudServerOutlined, DownOutlined, LogoutOutlined, UserOutlined } from '@ant-design/icons';
import { Badge, Button, Dropdown, Layout, Menu, Select, Space, Tag } from 'antd';
import { useEffect, useState } from 'react';
import { Navigate, Route, Routes, useLocation, useNavigate } from 'react-router-dom';
import { api } from '../api';
import { useErrorHandler } from '../hooks';
import ThemeSwitcher from '../theme/ThemeSwitcher';
import type { AppView, MeResp } from '../types';
import AccountModal from './AccountModal';
import AppsView from './AppsView';
import InstancesView from './InstancesView';
import JobsView from './JobsView';
import WorkersView from './WorkersView';

const { Header, Sider, Content } = Layout;

const SELECTED_APP_KEY = 'dida.selectedAppId';
function storedAppId(): number | undefined {
  const v = localStorage.getItem(SELECTED_APP_KEY);
  if (!v) return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}

type ViewKey = 'apps' | 'jobs' | 'instances' | 'workers';

export default function Console(props: {
  me: MeResp;
  onLoggedOut: () => void;
  onUsernameChange: (username: string) => void;
}) {
  const handleError = useErrorHandler();
  const { me } = props;
  const isAdmin = me.role === 'admin';
  // 路由驱动菜单:URL(/jobs 等) ↔ Menu 选中 ↔ 内容区。根 / 与未匹配均重定向到默认页。
  const navigate = useNavigate();
  const location = useLocation();
  const home = isAdmin ? '/apps' : '/jobs';
  const currentKey: ViewKey = (() => {
    const seg = location.pathname.split('/')[1] as ViewKey;
    return ['apps', 'jobs', 'instances', 'workers'].includes(seg) ? seg : (isAdmin ? 'apps' : 'jobs');
  })();
  const [apps, setApps] = useState<AppView[]>([]);
  const [selectedAppId, setSelectedAppId] = useState<number | undefined>(() => storedAppId() ?? me.app_id);
  const [appsLoading, setAppsLoading] = useState(false);
  const [health, setHealth] = useState<{ status: string; driver: string }>();
  const [accountOpen, setAccountOpen] = useState(false);

  const loadApps = async () => {
    if (!isAdmin) return;
    setAppsLoading(true);
    try {
      const data = await api.apps.list({ page: 1, size: 200 });
      const list = data.list || [];
      setApps(list);
      // 选中 app 不在列表(被删/首次/历史残留)→ 回退首个并持久化,避免落到无效 id
      if (list.length && !list.some((a) => a.id === selectedAppId)) {
        setSelectedAppId(list[0].id);
        localStorage.setItem(SELECTED_APP_KEY, String(list[0].id));
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
  const chooseApp = (id: number) => {
    setSelectedAppId(id);
    localStorage.setItem(SELECTED_APP_KEY, String(id));
  };

  const menuItems = [
    ...(isAdmin ? [{ key: 'apps', icon: <AppstoreOutlined />, label: '应用' }] : []),
    { key: 'jobs', icon: <ClockCircleOutlined />, label: '任务' },
    { key: 'instances', icon: <BugOutlined />, label: '实例' },
    { key: 'workers', icon: <CloudServerOutlined />, label: 'Worker' },
  ];

  return (
    <Layout className="shell" >
      {/* Sider 不写死 theme:背景由 theme/index.tsx 注入 Layout.siderBg=transparent,透出 body 的
          colorBgLayout 跟随主题(与顶栏同一套机制)。Menu 显式 theme="light" 让其 token 受 algorithm
          派生(dark 主题下字色/选中态自动变深),而非继承 Sider 默认 dark 的硬编码深色风格。 */}
      <Sider width={248} className="side">
        <div className="brand">
          <ApiOutlined />
          <div>
            <strong>Task Schedule</strong>
            <span>调度管理台</span>
          </div>
        </div>
        <Menu mode="inline" theme="light" selectedKeys={[currentKey]} onClick={({ key }) => navigate(`/${key}`)} items={menuItems} />
      </Sider>
      <Layout>
        <Header className="topbar">
          <Space size={12} wrap>
            {isAdmin ? (
              <Select
                className="app-switch"
                placeholder="选择应用"
                value={selectedAppId}
                onChange={chooseApp}
                options={apps.map((item) => ({ label: `${item.app_name} (${item.id})`, value: item.id }))}
                loading={appsLoading}
              />
            ) : (
              <Tag icon={<ApiOutlined />} color="blue" style={{ fontWeight: 500, padding: '4px 12px' }}>
                {me.app_name || `App ${me.app_id}`}
              </Tag>
            )}
            {isAdmin && currentApp && (
              <Tag color={currentApp.status === 1 ? 'success' : 'default'} style={{ fontWeight: 500 }}>
                {currentApp.status === 1 ? '启用' : '禁用'}
              </Tag>
            )}
            {health && (
              <Badge
                status={health.status === 'ok' ? 'success' : 'error'}
                text={`DB ${health.driver}`}
                style={{ fontWeight: 400 }}
              />
            )}
          </Space>
          <Space>
            <Tag color={isAdmin ? 'gold' : 'geekblue'} style={{ fontWeight: 500, padding: '4px 12px' }}>
              {isAdmin ? '管理员' : '应用'}
            </Tag>
            <Dropdown
              menu={{
                items: [{ key: 'account', label: '账户设置', onClick: () => setAccountOpen(true) }],
              }}
            >
              <Button type="text" style={{ fontWeight: 500 }}>
                <UserOutlined /> {me.username} <DownOutlined />
              </Button>
            </Dropdown>
            <ThemeSwitcher />
            <Button icon={<LogoutOutlined />} onClick={props.onLoggedOut} style={{ fontWeight: 500 }}>
              登出
            </Button>
          </Space>
        </Header>
        <Content className="content">
          <Routes>
            <Route path="/" element={<Navigate to={home} replace />} />
            <Route
              path="/apps"
              element={isAdmin ? <AppsView apps={apps} loading={appsLoading} onReload={loadApps} onError={handleError} /> : <Navigate to={home} replace />}
            />
            <Route path="/jobs" element={<JobsView appId={currentAppId} isAdmin={isAdmin} onError={handleError} />} />
            <Route path="/instances" element={<InstancesView appId={currentAppId} onError={handleError} />} />
            <Route path="/workers" element={<WorkersView appId={currentAppId} onError={handleError} />} />
            <Route path="*" element={<Navigate to={home} replace />} />
          </Routes>
        </Content>
      </Layout>
      <AccountModal
        open={accountOpen}
        username={me.username}
        onUsernameChanged={props.onUsernameChange}
        onClose={() => setAccountOpen(false)}
      />
    </Layout>
  );
}
