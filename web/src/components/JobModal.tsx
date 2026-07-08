import { DatePicker, Form, Input, InputNumber, Modal, Select, Switch } from 'antd';
import { scheduleKindOptions } from '../schedule';
import type { JobView, ScheduleKind } from '../types';

export default function JobModal(props: {
  open: boolean;
  editingJob?: JobView;
  form: ReturnType<typeof Form.useForm>[0];
  onCancel: () => void;
  onOk: () => void;
}) {
  const kind = Form.useWatch('schedule_kind', props.form) as ScheduleKind | undefined;
  return (
    <Modal
      title={props.editingJob ? '编辑任务' : '新建任务'}
      open={props.open}
      onCancel={props.onCancel}
      onOk={props.onOk}
      width={900}
      destroyOnHidden
    >
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
          {(kind === 'cron' || kind === 'fix_rate' || kind === 'fix_delay' || kind === 'delay') && (
            <>
              <Form.Item name="start_time" label="生效开始时间">
                <DatePicker showTime className="full" />
              </Form.Item>
              <Form.Item name="end_time" label="生效结束时间">
                <DatePicker showTime className="full" />
              </Form.Item>
            </>
          )}
        </div>
        <Form.Item name="description" label="任务描述">
          <Input.TextArea rows={2} placeholder="可选,任务的说明备注" />
        </Form.Item>
        <Form.Item name="job_params" label="任务参数(job_params)">
          <Input.TextArea rows={3} placeholder="随每次执行下发的参数字符串" />
        </Form.Item>
        <Form.Item name="callback_url" label="回调 URL(可选)">
          <Input placeholder="https://example.com/hook(状态变化时 POST 通知,至少一次)" />
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
          <Form.Item name="retry_interval_sec" label="重试间隔秒" tooltip="退避基数:首次重试等 N 秒,此后每次翻倍封顶 30min;0=默认 1s">
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
