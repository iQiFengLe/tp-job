package config

import "testing"

// 管理员账户已迁出 config(入库 admin_user 表,首次 seed admin/admin123,由 dservice 管)。
// config 现仅负责:会话 TTL 默认、applyEnv 对非账户配置生效、Validate 的登录限流 release 强制。
// 旧 admins / env 注入 / bcrypt 占位密码相关测试随机制一并移除。

func TestApplyDefaultsSessionTTL(t *testing.T) {
	c := &Config{}
	c.applyDefaults()
	if c.Auth.Session.TTLSeconds != 86400 {
		t.Fatalf("会话 TTL 应默认 86400, got %d", c.Auth.Session.TTLSeconds)
	}
}

// TestApplyDefaultsInstanceRetention 实例日志保留兜底:0(未设置)→90;-1(显式不清理)保留不被覆盖。
func TestApplyDefaultsInstanceRetention(t *testing.T) {
	c := &Config{}
	c.applyDefaults()
	if c.Log.InstanceRetentionDays != 90 {
		t.Fatalf("InstanceRetentionDays 未设置应兜底 90, got %d", c.Log.InstanceRetentionDays)
	}
	c2 := &Config{Log: Log{InstanceRetentionDays: -1}}
	c2.applyDefaults()
	if c2.Log.InstanceRetentionDays != -1 {
		t.Fatalf("-1(显式不清理)不应被兜底覆盖, got %d", c2.Log.InstanceRetentionDays)
	}
}

func TestApplyEnvStillHandlesNonAccount(t *testing.T) {
	// admin 账户 env 已废弃(走 DB),设了也不应有副作用;DB 驱动等非账户 env 仍生效。
	t.Setenv("TP_JOB_ADMIN_USERNAME", "ops")
	t.Setenv("TP_JOB_ADMIN_PASSWORD", "whatever")
	t.Setenv("TP_JOB_DB_DRIVER", "mysql")
	c := &Config{}
	c.applyEnv()
	if c.Database.Driver != "mysql" {
		t.Fatalf("DB_DRIVER env 应生效, got %q", c.Database.Driver)
	}
}

func TestValidateReleaseRequiresLoginRateLimit(t *testing.T) {
	cases := []struct {
		name    string
		auth    Auth
		release bool
		wantErr bool
	}{
		{"release 未配登录限流", Auth{}, true, true},
		{"release 配了限流", Auth{Login: LoginConfig{MaxAttemptsPerMin: 10}}, true, false},
		{"非 release 无需限流", Auth{}, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.auth.Validate(c.release)
			if c.wantErr && err == nil {
				t.Fatalf("%s: 应报错, got nil", c.name)
			}
			if !c.wantErr && err != nil {
				t.Fatalf("%s: 不应报错, got %v", c.name, err)
			}
		})
	}
}
