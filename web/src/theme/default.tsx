import type { ConfigProviderProps } from 'antd';
import { theme } from 'antd';

export const defaultMeta = { key: 'default', label: '默认' } as const;

// 现代极简卡片风(对标 Vercel/Linear):干净冷灰底色衬托白色卡片、柔和阴影、较大圆角、
// 表头弱化 + 行 hover 高亮。主色沿用 antd 蓝,不强行换色——卡片/留白/圆角才是风格主调。
export const defaultConfig: ConfigProviderProps = {
  theme: {
    algorithm: theme.defaultAlgorithm,
    token: {
      borderRadius: 10,
      borderRadiusLG: 14,
      borderRadiusSM: 8,
      colorBgLayout: '#f5f7fa',
    },
    components: {
      Card: {
        paddingLG: 20,
      },
      Table: {
        headerBg: 'transparent',
        headerSplitColor: 'transparent',
        rowHoverBg: 'rgba(22, 119, 255, 0.04)',
        cellPaddingBlock: 14,
      },
    },
  },
};
