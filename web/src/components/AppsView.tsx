import { DeleteOutlined, EditOutlined, PlusOutlined, ReloadOutlined } from '@ant-design/icons';
import { App as AntApp, Button, Form, Input, Modal, Popconfirm, Select, Space, Table, Tag, Tooltip, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useState } from 'react';
import { api } from '../api';
import { compactObject, formatTime } from '../lib';
import type { AppView } from '../types';

const { Text, Title } = Typography;

export default function AppsView(props: {
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
        <Space orientation="vertical" size={0}>
          <Text strong style={{ fontSize: 15 }}>{record.app_name}</Text>
          <Text type="secondary" style={{ fontSize: 12 }}>ID {record.id}</Text>
        </Space>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      width: 100,
      render: (value: number) => (
        <Tag color={value === 1 ? 'success' : 'default'} style={{ fontWeight: 500 }}>
          {value === 1 ? '启用' : '禁用'}
        </Tag>
      ),
    },
    { title: '创建时间', dataIndex: 'created_at', render: formatTime, width: 180 },
    { title: '更新时间', dataIndex: 'updated_at', render: formatTime, width: 180 },
    {
      title: '操作',
      width: 160,
      render: (_, record) => (
        <Space size="small">
          <Tooltip title="编辑">
            <Button size="small" icon={<EditOutlined />} onClick={() => openEdit(record)} />
          </Tooltip>
          <Popconfirm title="删除应用" description="仅无任务的应用可删除。" onConfirm={() => remove(record.id)}>
            <Tooltip title="删除">
              <Button size="small" danger icon={<DeleteOutlined />} />
            </Tooltip>
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
      <Table rowKey="id" columns={columns} dataSource={props.apps} loading={props.loading} pagination={false} scroll={{ x: 1000 }} />

      <Modal title={editingApp ? '编辑应用' : '新建应用'} open={modalOpen} onOk={submit} onCancel={() => setModalOpen(false)} destroyOnHidden>
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
