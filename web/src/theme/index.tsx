import { App as AntApp, ConfigProvider, theme as antdTheme } from 'antd';
import type { ConfigProviderProps } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';

import { darkConfig, darkMeta } from './dark';
import { defaultConfig, defaultMeta } from './default';
import { glassMeta, useGlassTheme } from './glass';
import { cartoonMeta, useCartoonTheme } from './cartoon';
import { illustrationMeta, useIllustrationTheme } from './illustration';
import { muiMeta, useMuiTheme } from './mui';
import { shadcnMeta, useShadcnTheme } from './shadcn';

// 主题元数据 + 该主题应用到 ConfigProvider 的配置(token/algorithm/组件级 classNames)。
export interface ThemeEntry {
  key: string;
  label: string;
  config: ConfigProviderProps;
}

const STORAGE_KEY = 'dida-theme';

interface ThemeContextValue {
  themeKey: string;       // 用户选择:'auto'(跟随系统) 或具体主题 key(用户手选后固定)
  effectiveKey: string;   // auto 解析后的实际主题 key(auto → default/dark)
  setThemeKey: (key: string) => void;
  themes: ThemeEntry[];
}

const ThemeContext = createContext<ThemeContextValue | null>(null);

export function ThemeProvider({ children }: { children: ReactNode }) {
  // 5 个依赖 createStyles 的主题 hook 在此无条件全部调用(符合 hooks 规则),
  // 收集成表后按 themeKey 选取 config 注入同一个 ConfigProvider。
  // 这样切换主题时 children 不会 remount,不丢组件 state、不重发请求。
  // 代价是未选中主题的 css 规则闲置常驻(每套几十条,可忽略)。
  const glass = useGlassTheme();
  const cartoon = useCartoonTheme();
  const illustration = useIllustrationTheme();
  const mui = useMuiTheme();
  const shadcn = useShadcnTheme();

  const themes = useMemo<ThemeEntry[]>(
    () => [
      { ...defaultMeta, config: defaultConfig },
      { ...darkMeta, config: darkConfig },
      { ...glassMeta, config: glass },
      { ...cartoonMeta, config: cartoon },
      { ...illustrationMeta, config: illustration },
      { ...muiMeta, config: mui },
      { ...shadcnMeta, config: shadcn },
    ],
    [glass, cartoon, illustration, mui, shadcn],
  );

  // 系统明暗偏好:自动模式下据此在 default/dark 间切换。监听 change 实时跟随系统切换。
  const [systemDark, setSystemDark] = useState<boolean>(() =>
    typeof window !== 'undefined' && window.matchMedia
      ? window.matchMedia('(prefers-color-scheme: dark)').matches
      : false,
  );
  useEffect(() => {
    if (typeof window === 'undefined' || !window.matchMedia) return;
    const mq = window.matchMedia('(prefers-color-scheme: dark)');
    const handler = (e: MediaQueryListEvent) => setSystemDark(e.matches);
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }, []);

  // themeKey 为用户的选择:'auto' = 跟随系统(默认);某个具体 key = 用户手选后固定,不再随系统变。
  const [themeKey, setThemeKey] = useState<string>(() => {
    const saved = typeof localStorage !== 'undefined' ? localStorage.getItem(STORAGE_KEY) : null;
    if (saved && (saved === 'auto' || themes.some((t) => t.key === saved))) return saved;
    return 'auto';
  });

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, themeKey);
  }, [themeKey]);

  // auto 解析为实际主题:系统深色用 dark,否则 default。非 auto 直接用 themeKey。
  const effectiveKey = themeKey === 'auto' ? (systemDark ? darkMeta.key : defaultMeta.key) : themeKey;
  const current = themes.find((t) => t.key === effectiveKey) ?? themes[0];

  // antd Layout.Header 的 headerBg 默认是固定 navy(#001529),不随 algorithm / colorBgLayout 变化,
  // 导致无论切到哪个主题顶栏都是一片深蓝,与 body(colorBgLayout)割裂。统一注入 headerBg=transparent,
  // 让顶栏透出 body 背景——body 已由下方 BackgroundBinder 用 useToken 同步成 colorBgLayout,从而整页跟随主题。
  // (各主题若自定义了 Layout token,后置展开会覆盖此默认。)
  const prevLayout = (current.config.theme?.components as { Layout?: Record<string, unknown> } | undefined)?.Layout;
  const themedConfig: ConfigProviderProps = {
    ...current.config,
    theme: {
      ...current.config.theme,
      components: {
        ...current.config.theme?.components,
        Layout: { headerBg: 'transparent', siderBg: 'transparent', ...prevLayout },
      },
    },
  };

  const value = useMemo<ThemeContextValue>(
    () => ({ themeKey, effectiveKey, setThemeKey, themes }),
    [themeKey, effectiveKey, themes],
  );

  return (
    <ThemeContext.Provider value={value}>
      <ConfigProvider locale={zhCN} {...themedConfig}>
        <AntApp>
          <BackgroundBinder />
          {children}
        </AntApp>
      </ConfigProvider>
    </ThemeContext.Provider>
  );
}

export function useTheme() {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error('useTheme 必须在 <ThemeProvider> 内使用');
  return ctx;
}

// 把当前 antd 主题的背景/文字色同步到 <body>,使登录页、启动页等 antd 组件之外的
// 区域也跟随主题(dark 主题下整页背景变暗,而非只有组件变暗、页面大片留白)。
// 必须在 ConfigProvider 内才能 useToken,故独立为子组件。
function BackgroundBinder() {
  const { token } = antdTheme.useToken();
  useEffect(() => {
    document.body.style.background = token.colorBgLayout;
    document.body.style.color = token.colorText;
  }, [token.colorBgLayout, token.colorText]);
  return null;
}
