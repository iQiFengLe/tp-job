import type { ConfigProviderProps } from 'antd';
import { theme } from 'antd';

export const defaultMeta = { key: 'default', label: '默认' } as const;

// antd 原生浅色:仅指定 algorithm,其余 token 取默认。
export const defaultConfig: ConfigProviderProps = {
  theme: { algorithm: theme.defaultAlgorithm },
};
