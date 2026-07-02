package config

import (
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func matchesBcrypt(hash, plain string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain)) == nil
}

func TestApplyDefaultsSeedsAdmin(t *testing.T) {
	c := &Config{}
	c.applyDefaults()
	if len(c.Auth.Admins) != 1 {
		t.Fatalf("空 admins 应种 1 个默认, got %d", len(c.Auth.Admins))
	}
	if c.Auth.Admins[0].Username != defaultAdminUsername {
		t.Fatalf("默认 username 应 %q, got %q", defaultAdminUsername, c.Auth.Admins[0].Username)
	}
	if !matchesBcrypt(c.Auth.Admins[0].Password, defaultAdminPassword) {
		t.Fatalf("默认 password 应是 %q 的 bcrypt 哈希", defaultAdminPassword)
	}
	if c.Auth.Session.TTLSeconds != 86400 {
		t.Fatalf("会话 TTL 应默认 86400, got %d", c.Auth.Session.TTLSeconds)
	}
}

func TestApplyEnvAdminPassword(t *testing.T) {
	t.Setenv("TASK_SCHEDULE_ADMIN_PASSWORD", "super-secret")
	t.Setenv("TASK_SCHEDULE_ADMIN_USERNAME", "ops")
	c := &Config{}
	c.applyEnv()
	if len(c.Auth.Admins) != 1 || c.Auth.Admins[0].Username != "ops" {
		t.Fatalf("env 注入后 admins[0].Username 应 ops, got %+v", c.Auth.Admins)
	}
	if !matchesBcrypt(c.Auth.Admins[0].Password, "super-secret") {
		t.Fatal("env 明文密码应被哈希后可校验")
	}
}

func TestApplyEnvAppendsSlotIfMissing(t *testing.T) {
	// Admins 为空时 env 注入应自动建 slot(ensureAdminSlot)。
	t.Setenv("TASK_SCHEDULE_ADMIN_USERNAME", "ops")
	c := &Config{}
	c.applyEnv()
	if len(c.Auth.Admins) != 1 || c.Auth.Admins[0].Username != "ops" {
		t.Fatalf("应建 1 个 slot 且 username=ops, got %+v", c.Auth.Admins)
	}
}

func TestValidateReleaseCases(t *testing.T) {
	validHash := hashPassword("strong-pwd")

	cases := []struct {
		name    string
		auth    Auth
		wantErr bool
	}{
		{"空 admins", Auth{}, true},
		{"默认占位密码", Auth{Admins: []AdminAccount{{Username: "admin", Password: hashPassword(defaultAdminPassword)}}}, true},
		{"密码格式非法", Auth{Admins: []AdminAccount{{Username: "admin", Password: "not-a-hash"}}}, true},
		{"用户名空", Auth{Admins: []AdminAccount{{Username: "  ", Password: validHash}}}, true},
		{"用户名重复", Auth{Admins: []AdminAccount{
			{Username: "a", Password: validHash},
			{Username: "a", Password: validHash},
		}}, true},
		{"合法", Auth{Admins: []AdminAccount{{Username: "admin", Password: validHash}}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.auth.Validate(true)
			if c.wantErr && err == nil {
				t.Fatalf("%s: release 应报错, got nil", c.name)
			}
			if !c.wantErr && err != nil {
				t.Fatalf("%s: 不应报错, got %v", c.name, err)
			}
		})
	}
}

func TestValidateNonReleaseAlwaysOK(t *testing.T) {
	// 非 release 即便配置有问题(空 admins / 默认密码)也不硬性拒绝,留给日志提示。
	for _, a := range []Auth{
		{},
		{Admins: []AdminAccount{{Username: "admin", Password: hashPassword(defaultAdminPassword)}}},
	} {
		if err := a.Validate(false); err != nil {
			t.Fatalf("非 release 不应报错, got %v", err)
		}
	}
}

func TestIsBcryptHash(t *testing.T) {
	// 真 bcrypt 哈希:长度 60、前缀 $2a$/$2b$/$2y$。
	valid := hashPassword("x")
	if len(valid) != 60 {
		t.Fatalf("真 bcrypt 哈希应 60 字符, got %d", len(valid))
	}
	wrongPrefix := "$1$" + strings.Repeat("a", 57) // 长度 60 但前缀错(传统 crypt MD5)
	cases := []struct {
		s    string
		want bool
	}{
		{valid, true},
		{"not-a-hash", false},
		{"$2a$short", false},
		{"", false},
		{wrongPrefix, false},
	}
	for _, c := range cases {
		if got := isBcryptHash(c.s); got != c.want {
			t.Fatalf("isBcryptHash(%q) 应 %v, got %v", c.s, c.want, got)
		}
	}
}
