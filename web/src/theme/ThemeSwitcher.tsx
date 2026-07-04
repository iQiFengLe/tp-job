import { BgColorsOutlined } from '@ant-design/icons';
import { Button, Dropdown, type MenuProps } from 'antd';
import { useTheme } from './index';

// 主题切换器:Dropdown 顶部"跟随系统"(auto) + 全部主题。auto 时按系统 prefers-color-scheme
// 在 default/dark 间自动切换;用户手选任一具体主题后即固定,不再随系统变。Console 顶栏与
// LoginView 复用,登录前/登录后都能切(切换不需登录)。
export default function ThemeSwitcher() {
  const { themeKey, effectiveKey, setThemeKey, themes } = useTheme();
  const effectiveLabel = themes.find((t) => t.key === effectiveKey)?.label;
  const items: MenuProps['items'] = [
    { key: 'auto', label: effectiveLabel ? `跟随系统 · ${effectiveLabel}` : '跟随系统' },
    { type: 'divider' },
    ...themes.map((t) => ({ key: t.key, label: t.label })),
  ];
  const currentLabel = themeKey === 'auto' ? '跟随系统' : themes.find((t) => t.key === themeKey)?.label;
  return (
    <Dropdown
      trigger={['click']}
      menu={{ items, selectedKeys: [themeKey], onClick: ({ key }) => setThemeKey(key) }}
    >
      <Button icon={<BgColorsOutlined />} style={{ fontWeight: 500 }}>
        {currentLabel}
      </Button>
    </Dropdown>
  );
}
