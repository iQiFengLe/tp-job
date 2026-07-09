import type { ConfigProviderProps } from 'antd';
import { theme } from 'antd';

export const darkMeta = { key: 'dark', label: '暗色' } as const;

// 暗色版现代极简:与 default 同一套圆角/表格风格,深色 algorithm 派生色板。
export const darkConfig: ConfigProviderProps = {
  theme: {
    algorithm: theme.darkAlgorithm,
    token: {
      borderRadius: 10,
      borderRadiusLG: 14,
      borderRadiusSM: 8,
    },
    components: {
      Table: {
        headerBg: 'transparent',
        headerSplitColor: 'transparent',
        rowHoverBg: 'rgba(255, 255, 255, 0.05)',
        cellPaddingBlock: 14,
      },
    },
  },
};
