import { ApiOutlined, LoginOutlined } from '@ant-design/icons';
import { Button, Card, Form, Input } from 'antd';
import { useState } from 'react';
import { api } from '../api';
import { useErrorHandler } from '../hooks';
import ThemeSwitcher from '../theme/ThemeSwitcher';
import type { LoginReq, MeResp, Role } from '../types';

export default function LoginView(props: { onLoggedIn: (token: string, me: MeResp) => void }) {
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
      <div style={{ position: 'fixed', top: 16, right: 16, zIndex: 1000 }}>
        <ThemeSwitcher />
      </div>
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
