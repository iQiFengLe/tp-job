import { KeyOutlined, UserOutlined } from '@ant-design/icons';
import { App as AntApp, Form, Input, Modal, Tabs, type FormInstance, type TabsProps } from 'antd';
import { useEffect, useState } from 'react';
import { api } from '../api';
import { useErrorHandler } from '../hooks';
import type { AccountChangePasswordReq, AccountUpdateProfileReq } from '../types';

interface AccountModalProps {
  open: boolean;
  username: string;
  onUsernameChanged: (username: string) => void;
  onClose: () => void;
}

export default function AccountModal(props: AccountModalProps) {
  const handleError = useErrorHandler();
  const { message } = AntApp.useApp();
  const [profileForm] = Form.useForm<AccountUpdateProfileReq>();
  const [passwordForm] = Form.useForm<AccountChangePasswordReq & { confirm: string }>();
  const [activeKey, setActiveKey] = useState('profile');
  const [savingProfile, setSavingProfile] = useState(false);
  const [savingPassword, setSavingPassword] = useState(false);

  // Modal open 变化时同步表单:打开预填用户名,关闭清空两个表单(避免密码残留)。
  useEffect(() => {
    if (props.open) {
      profileForm.setFieldValue('username', props.username);
    } else {
      (profileForm as FormInstance).resetFields();
      (passwordForm as FormInstance).resetFields();
    }
  }, [props.open, props.username, profileForm, passwordForm]);

  const submitProfile = async (values: AccountUpdateProfileReq) => {
    setSavingProfile(true);
    try {
      await api.account.updateProfile({ username: values.username });
      message.success('用户名已更新,下次登录请使用新用户名');
      props.onUsernameChanged(values.username);
      props.onClose();
    } catch (error) {
      handleError(error);
    } finally {
      setSavingProfile(false);
    }
  };

  const submitPassword = async (values: AccountChangePasswordReq) => {
    setSavingPassword(true);
    try {
      await api.account.changePassword({ old_password: values.old_password, new_password: values.new_password });
      message.success('密码已更新');
      props.onClose();
    } catch (error) {
      handleError(error);
    } finally {
      setSavingPassword(false);
    }
  };

  const tabs: TabsProps['items'] = [
    {
      key: 'profile',
      label: (
        <span>
          <UserOutlined /> 用户名
        </span>
      ),
      children: (
        <Form<AccountUpdateProfileReq>
          form={profileForm}
          layout="vertical"
          onFinish={submitProfile}
          preserve={false}
        >
          <Form.Item
            name="username"
            label="用户名"
            rules={[
              { required: true, message: '请输入用户名' },
              { min: 3, message: '用户名至少 3 个字符' },
            ]}
          >
            <Input autoComplete="username" />
          </Form.Item>
        </Form>
      ),
    },
    {
      key: 'password',
      label: (
        <span>
          <KeyOutlined /> 密码
        </span>
      ),
      children: (
        <Form<AccountChangePasswordReq & { confirm: string }>
          form={passwordForm}
          layout="vertical"
          onFinish={submitPassword}
          preserve={false}
        >
          <Form.Item
            name="old_password"
            label="原密码"
            rules={[{ required: true, message: '请输入原密码' }]}
          >
            <Input.Password autoComplete="current-password" />
          </Form.Item>
          <Form.Item
            name="new_password"
            label="新密码"
            rules={[
              { required: true, message: '请输入新密码' },
              { min: 6, message: '新密码至少 6 个字符' },
            ]}
          >
            <Input.Password autoComplete="new-password" />
          </Form.Item>
          <Form.Item
            name="confirm"
            label="确认新密码"
            dependencies={['new_password']}
            rules={[
              { required: true, message: '请再次输入新密码' },
              ({ getFieldValue }) => ({
                validator(_, value) {
                  if (!value || getFieldValue('new_password') === value) return Promise.resolve();
                  return Promise.reject(new Error('两次输入的新密码不一致'));
                },
              }),
            ]}
          >
            <Input.Password autoComplete="new-password" />
          </Form.Item>
        </Form>
      ),
    },
  ];

  const submitCurrent = () => {
    if (activeKey === 'profile') {
      (profileForm as FormInstance).submit();
    } else {
      (passwordForm as FormInstance).submit();
    }
  };

  const loading = activeKey === 'profile' ? savingProfile : savingPassword;
  const okText = activeKey === 'profile' ? '保存用户名' : '保存密码';

  return (
    <Modal
      title="账户设置"
      open={props.open}
      onCancel={props.onClose}
      onOk={submitCurrent}
      okText={okText}
      confirmLoading={loading}
      forceRender
      mask={{ closable: false }}
    >
      <Tabs
        activeKey={activeKey}
        onChange={setActiveKey}
        items={tabs}
        tabBarStyle={{ marginBottom: 16 }}
      />
    </Modal>
  );
}
