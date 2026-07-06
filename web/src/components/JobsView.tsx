import {CloudSyncOutlined, DeleteOutlined, EditOutlined, PlayCircleOutlined, PlusOutlined, ReloadOutlined} from '@ant-design/icons';
import {App as AntApp, Button, Form, Popconfirm, Space, Table, Tag, Tooltip, Typography} from 'antd';
import type {ColumnsType} from 'antd/es/table';
import dayjs from 'dayjs';
import {useEffect, useState} from 'react';
import {api} from '../api';
import {PAGE_SIZE, compactObject, formatTime} from '../lib';
import {formatScheduleExpr, isAutoKind, scheduleExprFromForm, scheduleKindOptions} from '../schedule';
import type {JobCreateValues, JobUpdateValues, JobView} from '../types';
import AppGate from './AppGate';
import ImportPowerJobModal from './ImportPowerJobModal';
import JobDetailDrawer from './JobDetailDrawer';
import JobModal from './JobModal';

const {Text, Title} = Typography;

export default function JobsView(props: { appId?: number; isAdmin?: boolean; onError: (error: unknown) => void }) {
    const {message} = AntApp.useApp();
    const [form] = Form.useForm();
    const [jobs, setJobs] = useState<JobView[]>([]);
    const [total, setTotal] = useState(0);
    const [page, setPage] = useState(1);
    const [size, setSize] = useState(PAGE_SIZE);
    const [loading, setLoading] = useState(false);
    const [modalOpen, setModalOpen] = useState(false);
    const [editingJob, setEditingJob] = useState<JobView>();
    const [detailJob, setDetailJob] = useState<JobView>();
    const [importOpen, setImportOpen] = useState(false);

    const load = async (p = page, s = size) => {
        if (!props.appId) return;
        setLoading(true);
        try {
            const data = await api.jobs.list(props.appId, {page: p, size: s});
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
            schedule_kind: 'api',
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
            start_time: job.start_time ? dayjs(job.start_time) : undefined,
            end_time: job.end_time ? dayjs(job.end_time) : undefined,
        });
        setModalOpen(true);
    };

    const submit = async () => {
        if (!props.appId) return;
        const raw = await form.validateFields();
        // schedule_expr 按 kind 整理:run_at→ISO;fix_rate/fix_delay/delay→String(number);cron→原值;api→忽略
        const expr = scheduleExprFromForm(raw.schedule_kind, raw.schedule_expr, raw.run_at);
        // 生效窗口仅对自动调度类型(cron/fix_rate/fix_delay/delay)有意义;其余类型强制 0(清空),
        // 避免从 cron 切到 api/run_at 时残留的窗口值被误存。
        const isAuto = isAutoKind(raw.schedule_kind);
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
            start_time: isAuto && raw.start_time ? (raw.start_time as dayjs.Dayjs).valueOf() : 0,
            end_time: isAuto && raw.end_time ? (raw.end_time as dayjs.Dayjs).valueOf() : 0,
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
                <Space orientation="vertical" size={0}>
                    <Button type="link" className="link-button" onClick={() => setDetailJob(record)}>
                        {record.name}
                    </Button>
                    <Text type="secondary" style={{fontSize: 12}}>ID {record.id}</Text>
                </Space>
            ),
        },
        {
            title: '调度',
            render: (_, record) => {
                const kindLabel = scheduleKindOptions.find((opt) => opt.value === record.schedule_kind)?.label || record.schedule_kind;
                return (
                    <Space orientation="vertical" size={0}>
                        <Tag style={{fontWeight: 500}}>{kindLabel}</Tag>
                        <Text type="secondary" style={{fontSize: 12}}>{formatScheduleExpr(record)}</Text>
                    </Space>
                );
            },
        },
        {title: '下次执行', dataIndex: 'next_run_time', render: formatTime, width: 180},
        {
            title: '状态',
            dataIndex: 'enabled',
            width: 90,
            render: (value: boolean) => (
                <Tag color={value ? 'success' : 'default'} style={{fontWeight: 500}}>
                    {value ? '启用' : '停用'}
                </Tag>
            ),
        },
        {
            title: '操作',
            width: 180,
            render: (_, record) => (
                <Space size="small">
                    <Tooltip title="触发">
                        <Button size="small" icon={<PlayCircleOutlined/>} onClick={() => trigger(record.id)}/>
                    </Tooltip>
                    <Tooltip title="编辑">
                        <Button size="small" icon={<EditOutlined/>} onClick={() => openEdit(record)}/>
                    </Tooltip>
                    <Popconfirm title="删除任务" onConfirm={() => remove(record.id)}>
                        <Tooltip title="删除">
                            <Button size="small" danger icon={<DeleteOutlined/>}/>
                        </Tooltip>
                    </Popconfirm>
                </Space>
            ),
        },
    ];

    return (
        <section className="view">
            <AppGate appId={props.appId}/>
            <div className="view-head">
                <div>
                    <Title level={3}>任务管理</Title>
                    <Text type="secondary">维护 Job(cron / fix_rate / fix_delay / delay / run_at /
                        api),支持手动触发。</Text>
                </div>
                <Button type="primary" icon={<PlusOutlined/>} onClick={openCreate} disabled={!props.appId}>
                    新建任务
                </Button>
            </div>
            <div className="toolbar">
                <Button icon={<ReloadOutlined/>} onClick={() => load()} loading={loading}>
                    刷新
                </Button>
                {props.isAdmin && (
                    <Button icon={<CloudSyncOutlined/>} onClick={() => setImportOpen(true)} disabled={!props.appId}>
                        从 PowerJob 导入
                    </Button>
                )}
            </div>
            <Table
                rowKey="id"
                columns={columns}
                className="table-container"
                dataSource={jobs}
                loading={loading}
                scroll={{x: 1200}}
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
            <JobModal open={modalOpen} editingJob={editingJob} form={form} onCancel={() => setModalOpen(false)}
                      onOk={submit}/>
            <JobDetailDrawer job={detailJob} onClose={() => setDetailJob(undefined)}/>
            <ImportPowerJobModal
                open={importOpen}
                appId={props.appId}
                onClose={() => setImportOpen(false)}
                onImported={() => {
                    message.success('任务列表已刷新');
                    load();
                }}
                onError={props.onError}
            />
        </section>
    );
}
