import type {
  AccountChangePasswordReq,
  AccountProfile,
  AccountUpdateProfileReq,
  ApiBody,
  AppCreateValues,
  AppUpdateValues,
  AppView,
  ImportPowerJobReq,
  ImportPowerJobResp,
  InstanceView,
  JobCreateValues,
  JobUpdateValues,
  JobView,
  LogResult,
  LoginReq,
  LoginResp,
  MeResp,
  PageResult,
  WorkerView,
} from './types';

export class ApiError extends Error {
  constructor(
    message: string,
    public status: number,
    public code?: number,
  ) {
    super(message);
  }
}

// ===== 会话 token =====
//
// 登录后由 setToken 写入,所有受保护请求自动带 Authorization: Bearer。
// 401 时 clearToken 并触发 onUnauthorized 回调(App 层注册→回登录页)。

let authToken: string | null = null;
let onUnauthorized: (() => void) | null = null;

export function setToken(token: string | null) {
  authToken = token;
}

export function hasToken() {
  return Boolean(authToken);
}

export function setUnauthorizedHandler(fn: (() => void) | null) {
  onUnauthorized = fn;
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...((init.headers as Record<string, string>) || {}),
  };
  if (authToken) headers['Authorization'] = `Bearer ${authToken}`;
  const res = await fetch(path, { ...init, headers });
  const text = await res.text();
  const body = text ? (JSON.parse(text) as ApiBody<T>) : undefined;
  if (res.status === 401) {
    clearToken();
    onUnauthorized?.();
    throw new ApiError(body?.msg || '认证已失效,请重新登录', 401, body?.code);
  }
  if (!res.ok || !body || body.code !== 0) {
    throw new ApiError(body?.msg || res.statusText || '请求失败', res.status, body?.code);
  }
  return body.data;
}

export function clearToken() {
  authToken = null;
}

const qs = (params: Record<string, string | number | boolean | undefined>) => {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== '') search.set(key, String(value));
  });
  const value = search.toString();
  return value ? `?${value}` : '';
};

export const api = {
  health: () => request<{ status: string; driver: string }>('/health'),

  auth: {
    login: (values: LoginReq) =>
      request<LoginResp>('/api/auth/login', { method: 'POST', body: JSON.stringify(values) }),
    me: () => request<MeResp>('/api/auth/me'),
    logout: () => request<{ logged_out: boolean }>('/api/auth/logout', { method: 'POST' }),
  },

  account: {
    profile: () => request<AccountProfile>('/api/account/profile'),
    updateProfile: (v: AccountUpdateProfileReq) =>
      request<{ id: number }>('/api/account/profile', { method: 'PUT', body: JSON.stringify(v) }),
    changePassword: (v: AccountChangePasswordReq) =>
      request<{ id: number }>('/api/account/password', { method: 'PUT', body: JSON.stringify(v) }),
  },

  apps: {
    list: (params: { keyword?: string; page?: number; size?: number }) =>
      request<PageResult<AppView>>(`/api/apps${qs(params)}`),
    create: (values: AppCreateValues) =>
      request<AppView>('/api/apps', { method: 'POST', body: JSON.stringify(values) }),
    update: (appId: number, values: AppUpdateValues) =>
      request<{ id: number }>(`/api/apps/${appId}`, { method: 'PUT', body: JSON.stringify(values) }),
    remove: (appId: number) => request<{ id: number }>(`/api/apps/${appId}`, { method: 'DELETE' }),
  },

  jobs: {
    list: (appId: number, params: { page?: number; size?: number }) =>
      request<PageResult<JobView>>(`/api/apps/${appId}/jobs${qs(params)}`),
    create: (appId: number, values: JobCreateValues) =>
      request<JobView>(`/api/apps/${appId}/jobs`, { method: 'POST', body: JSON.stringify(values) }),
    update: (appId: number, id: number, values: JobUpdateValues) =>
      request<{ id: number }>(`/api/apps/${appId}/jobs/${id}`, { method: 'PUT', body: JSON.stringify(values) }),
    remove: (appId: number, id: number) =>
      request<{ id: number }>(`/api/apps/${appId}/jobs/${id}`, { method: 'DELETE' }),
    trigger: (appId: number, id: number, params: { priority?: number; instance_params?: string }) =>
      request<{ id: number; triggered: boolean; priority: number }>(
        `/api/apps/${appId}/jobs/${id}/trigger${qs(params)}`,
        { method: 'POST' },
      ),
    importPowerJob: (appId: number, req: ImportPowerJobReq) =>
      request<ImportPowerJobResp>(`/api/apps/${appId}/jobs/import-powerjob`, {
        method: 'POST',
        body: JSON.stringify(req),
      }),
  },

  instances: {
    list: (
      appId: number,
      params: { job_id?: number; status?: string; page?: number; size?: number },
    ) => request<PageResult<InstanceView>>(`/api/apps/${appId}/instances${qs(params)}`),
    get: (appId: number, iid: number) =>
      request<InstanceView>(`/api/apps/${appId}/instances/${iid}`),
    logs: (appId: number, iid: number, params: { group?: boolean; offset?: number; limit?: number }) =>
      request<LogResult>(`/api/apps/${appId}/instances/${iid}/logs${qs(params)}`),
  },

  workers: {
    list: (appId: number) =>
      request<{ list: WorkerView[]; count: number }>(`/api/apps/${appId}/workers`),
  },
};
