import { Spin } from 'antd';
import { useEffect, useState } from 'react';
import { api, clearToken, setToken, setUnauthorizedHandler } from './api';
import Console from './components/Console';
import LoginView from './components/LoginView';
import { loadToken, removeToken, saveToken } from './token';
import type { MeResp } from './types';

export default function App() {
  const [me, setMe] = useState<MeResp | null>(null);
  const [booting, setBooting] = useState(true);

  // 401 → 清身份回登录页
  useEffect(() => {
    setUnauthorizedHandler(() => {
      removeToken();
      setToken(null);
      setMe(null);
    });
    return () => setUnauthorizedHandler(null);
  }, []);

  // 启动:有 token 则 me() 校验,失效回退自动登录;无 token 直接自动登录(开发便利;
  // 生产环境会被 release 模式"拒默认密码"或登录限流挡住,失败回退到手动登录页)
  useEffect(() => {
    const autoLogin = () =>
      api.auth
        .login({ ident: 'admin', password: 'change-me-admin' })
        .then((resp) => {
          saveToken(resp.token);
          setToken(resp.token);
          setMe({ role: resp.role, username: resp.username, app_id: resp.app_id, app_name: resp.app_name });
        })
        .catch(() => {
          removeToken();
          setToken(null);
        });

    const tok = loadToken();
    if (tok) {
      setToken(tok);
      api.auth
        .me()
        .then((m) => setMe(m))
        .catch(() => autoLogin())
        .finally(() => setBooting(false));
      return;
    }
    autoLogin().finally(() => setBooting(false));
  }, []);

  const onLoggedIn = (token: string, m: MeResp) => {
    saveToken(token);
    setToken(token);
    setMe(m);
  };
  const onLoggedOut = async () => {
    try {
      await api.auth.logout();
    } catch {
      // 忽略:本地 token 无论如何清掉
    }
    removeToken();
    clearToken();
    setMe(null);
  };

  if (booting) {
    return (
      <div className="boot">
        <Spin size="large" />
      </div>
    );
  }
  if (!me) {
    return <LoginView onLoggedIn={onLoggedIn} />;
  }
  return <Console me={me} onLoggedOut={onLoggedOut} />;
}
