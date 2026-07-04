import { App as AntApp } from 'antd';
import { ApiError } from './api';

export function useErrorHandler() {
  const { message } = AntApp.useApp();
  return (error: unknown) => {
    const text = error instanceof ApiError || error instanceof Error ? error.message : '操作失败';
    message.error(text);
  };
}
