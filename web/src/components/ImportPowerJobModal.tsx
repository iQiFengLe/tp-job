import {CloudSyncOutlined} from '@ant-design/icons';
import {App as AntApp, Alert, Button, Form, Input, Modal, Space, Statistic, Table, Tag, type FormInstance} from 'antd';
import type {ColumnsType} from 'antd/es/table';
import {useState} from 'react';
import {api} from '../api';
import type {ImportPowerJobItem, ImportPowerJobReq, ImportPowerJobResp} from '../types';

const KIND_LABEL: Record<string, string> = {
    cron: 'CRON',
    fix_rate: 'FIX_RATE',
    fix_delay: 'FIX_DELAY',
    api: 'API',
};

// ImportPowerJobModal 从外部 PowerJob server 拉取任务定义导入当前 app(两步:预览 → 确认)。
// 仅展示与触发,落库/转换/SSRF 防护均在后端。
export default function ImportPowerJobModal(props: {
    open: boolean;
    appId?: number;
    onClose: () => void;
    onImported: () => void;
    onError: (e: unknown) => void;
}) {
    const {message} = AntApp.useApp();
    const [form] = Form.useForm<{server_address: string; app_name: string; password?: string; token?: string}>();
    const [step, setStep] = useState<'form' | 'preview'>('form');
    const [loading, setLoading] = useState(false);
    const [result, setResult] = useState<ImportPowerJobResp>();
    // 缓存预览时的请求参数:确认导入时 Form 已卸载(preserve={false} 清空字段),
    // 再 validateFields 拿不到值会触发 undefined.trim() 报错;且预览后表单不可改,确认本就该用同一份参数。
    const [reqValues, setReqValues] = useState<ImportPowerJobReq>();

    const reset = () => {
        setStep('form');
        setResult(undefined);
        setReqValues(undefined);
        (form as FormInstance).resetFields();
    };
    const close = () => {
        reset();
        props.onClose();
    };

    const callApi = (dryRun: boolean) => async () => {
        if (!props.appId) return;
        // 预览:从 Form 取值并 trim 后缓存(Form 此时尚挂载)。确认导入:复用预览参数——此时 Form 已卸载,
        // preserve={false} 清空了字段,validateFields 返回 undefined 字段,.trim() 报错。
        let req: ImportPowerJobReq;
        if (dryRun) {
            const v = await form.validateFields();
            req = {
                server_address: v.server_address.trim(),
                app_name: v.app_name.trim(),
                password: v.password?.trim() || undefined,
                token: v.token?.trim() || undefined,
                dry_run: true,
            };
            setReqValues(req);
        } else {
            if (!reqValues) return;
            req = {...reqValues, dry_run: false};
        }
        setLoading(true);
        try {
            const resp = await api.jobs.importPowerJob(props.appId, req);
            if (dryRun) {
                setResult(resp);
                setStep('preview');
            } else {
                message.success(`导入完成:新增 ${resp.imported},更新 ${resp.updated},跳过 ${resp.skipped}`);
                props.onImported();
                close();
            }
        } catch (e) {
            props.onError(e);
        } finally {
            setLoading(false);
        }
    };

    const columns: ColumnsType<ImportPowerJobItem> = [
        {title: '任务名', dataIndex: 'name', ellipsis: true},
        {
            title: '调度',
            render: (_, r) => (
                <Space orientation="vertical" size={0}>
                    <Tag style={{fontWeight: 500}}>{KIND_LABEL[r.schedule_kind] || r.schedule_kind}</Tag>
                    <span style={{fontSize: 12, color: 'var(--ant-color-text-secondary)'}}>{r.schedule_expr || '-'}</span>
                </Space>
            ),
        },
        {
            title: '启用',
            dataIndex: 'enabled',
            width: 70,
            render: (v: boolean) => <Tag color={v ? 'success' : 'default'}>{v ? '启用' : '停用'}</Tag>,
        },
        {
            title: '导入动作',
            width: 110,
            render: (_, r) => {
                if (r.error) return <Tag color="error">跳过</Tag>;
                if (r.conflict) return <Tag color="warning">将更新</Tag>;
                return <Tag color="processing">新增</Tag>;
            },
        },
        {
            title: '说明',
            dataIndex: 'error',
            ellipsis: true,
            render: (e?: string) => (e ? <span style={{color: 'var(--ant-color-error)', fontSize: 12}}>{e}</span> : '-'),
        },
    ];

    const canConfirm = !!result && result.imported + result.updated > 0;

    return (
        <Modal
            title={<Space><CloudSyncOutlined/> 从 PowerJob 导入任务</Space>}
            open={props.open}
            onCancel={close}
            width={880}
            destroyOnClose
            footer={
                step === 'form' ? (
                    <Space>
                        <Button onClick={close}>取消</Button>
                        <Button type="primary" icon={<CloudSyncOutlined/>} loading={loading} onClick={callApi(true)}>
                            预览
                        </Button>
                    </Space>
                ) : (
                    <Space>
                        <Button onClick={() => setStep('form')}>返回修改</Button>
                        <Button type="primary" loading={loading} onClick={callApi(false)} disabled={!canConfirm}>
                            确认导入
                        </Button>
                    </Space>
                )
            }
        >
            {step === 'form' ? (
                <Form form={form} layout="vertical" preserve={false}>
                    <Alert
                        type="info"
                        showIcon
                        style={{marginBottom: 16}}
                        message="只同步定时任务(CRON / FIX_RATE / FIX_DELAY)与 API 任务"
                        description="执行模型不同(PowerJob Java processor → 本系统 http 派发),仅搬调度定义;同步后需当前 app 下有匹配 tag 的在线 worker 才能真正执行。"
                    />
                    <Form.Item name="server_address" label="PowerJob 地址" rules={[{required: true, message: '请输入地址'}]}>
                        <Input placeholder="http://powerjob-host:7700" autoComplete="off"/>
                    </Form.Item>
                    <Form.Item name="app_name" label="PowerJob App 名称" rules={[{required: true, message: '请输入 appName'}]}>
                        <Input placeholder="如 powerjob-master" autoComplete="off"/>
                    </Form.Item>
                    <Form.Item name="password" label="App 密码(可选)" tooltip="仅当 /appInfo/list 不可用、回退 assert 且 app 设了密码时需要填写">
                        <Input.Password placeholder="多数情况留空即可" autoComplete="off"/>
                    </Form.Item>
                    <Form.Item name="token" label="Token(可选)" tooltip="PowerJob 4.3.3+ 开启 OpenAPI 认证时填写">
                        <Input.Password placeholder="未开启认证则留空" autoComplete="off"/>
                    </Form.Item>
                </Form>
            ) : (
                result && (
                    <Space orientation="vertical" size="middle" style={{width: '100%'}}>
                        <Space size="large">
                            <Statistic title="拉取" value={result.fetched}/>
                            <Statistic title="新增" value={result.imported} valueStyle={{color: '#1677ff'}}/>
                            <Statistic title="更新" value={result.updated} valueStyle={{color: '#fa8c16'}}/>
                            <Statistic
                                title="跳过"
                                value={result.skipped}
                                valueStyle={result.skipped ? {color: '#ff4d4f'} : undefined}
                            />
                        </Space>
                        {result.skipped > 0 && (
                            <Alert
                                type="warning"
                                showIcon
                                message={`${result.skipped} 个任务因表达式非法或无未来触发将被跳过(详见下表「说明」列)`}
                            />
                        )}
                        <Table
                            rowKey={(r) => `${r.name}|${r.schedule_kind}|${r.schedule_expr}`}
                            size="small"
                            columns={columns}
                            dataSource={result.preview}
                            pagination={{pageSize: 8, size: 'small'}}
                            scroll={{x: 560}}
                        />
                    </Space>
                )
            )}
        </Modal>
    );
}
