import type { ConfigProviderProps } from 'antd';
import { theme } from 'antd';

export const darkMeta = { key: 'dark', label: '暗色' } as const;

// antd 原生深色 algorithm。
export const darkConfig: ConfigProviderProps = {
  theme: { algorithm: theme.darkAlgorithm },
};
