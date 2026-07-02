package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

// Config 全局配置,由 config.yaml 加载。字段对应统一 worker 派发模型所需(旧 webhook /
// X-Admin-Token / PowerJob 独立调度循环等已随重构移除)。
type Config struct {
	Server struct {
		Port             int    `yaml:"port"`
		Mode             string `yaml:"mode"` // gin 模式: debug / release / test
		MaxRequestBodyMB int    `yaml:"max_request_body_mb"`
	} `yaml:"server"`

	Database  Database  `yaml:"database"`
	Log       Log       `yaml:"log"`
	Auth      Auth      `yaml:"auth"`
	Scheduler Scheduler `yaml:"scheduler"`
	Worker    Worker    `yaml:"worker"`
	PowerJob  PowerJob  `yaml:"powerjob"`
}

type Database struct {
	Driver string `yaml:"driver"` // sqlite | mysql
	SQLite struct {
		Path string `yaml:"path"`
	} `yaml:"sqlite"`
	MySQL struct {
		DSN          string `yaml:"dsn"`
		MaxOpenConns int    `yaml:"max_open_conns"`
		MaxIdleConns int    `yaml:"max_idle_conns"`
	} `yaml:"mysql"`
}

type Log struct {
	Level      string `yaml:"level"`
	Dir        string `yaml:"dir"`
	FileName   string `yaml:"file_name"`
	MaxSizeMB  int    `yaml:"max_size_mb"`
	MaxBackups int    `yaml:"max_backups"`
	MaxAgeDays int    `yaml:"max_age_days"`

	// InstanceRetentionDays 实例日志文件保留天数;0=不自动清理。清理按文件 mtime。
	InstanceRetentionDays int `yaml:"instance_retention_days"`
}

// Scheduler 统一调度器参数。新 dispatch 调度器自带并发槽/排队语义,无需触发并发数等旧参数。
type Scheduler struct {
	IntervalMs int `yaml:"interval_ms"` // 扫描周期(毫秒);reaper/retry 复用
}

type Worker struct {
	TimeoutSeconds int `yaml:"timeout_seconds"` // worker 心跳超时(秒),超过视为离线
}

// PowerJob 兼容协议配置。/server/* 端点始终挂载(标准 PowerJob Java worker 不改源码接入);
// 这里只保留 worker 可达地址。无独立调度循环——所有 job(含派发到 powerjob worker 的)统一由
// dispatch 调度器处理。
type PowerJob struct {
	ServerAddress string `yaml:"server_address"` // /server/acquire 返回值;PowerJob worker 可达的 host:port
}

// Auth 管理端鉴权配置:管理员账户(配置注入,不入库)+ 登录会话参数。
// 应用账户走 app 表(AppName + bcrypt Password)。worker 心跳不走登录(靠 appName + 网络隔离)。
type Auth struct {
	Admins  []AdminAccount `yaml:"admins"`  // 管理员账户(配置注入,不入库)
	Session SessionConfig  `yaml:"session"` // 登录会话参数
}

// AdminAccount 管理员账户。Password 为 bcrypt 哈希;env TASK_SCHEDULE_ADMIN_PASSWORD
// 注入明文时由 applyEnv 哈希后落入 Admins[0]。
type AdminAccount struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"` // bcrypt 哈希
}

// SessionConfig 登录会话参数。
type SessionConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"` // 会话有效期(秒);默认 86400(24h)
}

// 默认管理员占位凭据(开发便利):applyDefaults 在 Admins 为空时种入;release 防呆拒绝之。
const (
	defaultAdminUsername = "admin"
	defaultAdminPassword = "change-me-admin"
)

// Load 从指定路径加载配置,path 为空时使用默认 config.yaml
func Load(path string) (*Config, error) {
	if path == "" {
		path = "config.yaml"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败 %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}
	cfg.applyDefaults()
	cfg.applyEnv()
	return &cfg, nil
}

// applyEnv 用环境变量覆盖配置(优先级高于配置文件),便于容器/CI 注入密钥与开关,
// 避免把 mysql dsn / 管理员密码等明文写进 config.yaml。空值视为未设置,不覆盖。
func (c *Config) applyEnv() {
	// 管理员账户:env 注入明文用户名/密码,加载时哈希后落入 Admins[0](便于容器注入)。
	if v := os.Getenv("TASK_SCHEDULE_ADMIN_USERNAME"); v != "" {
		c.ensureAdminSlot()
		c.Auth.Admins[0].Username = v
	}
	if v := os.Getenv("TASK_SCHEDULE_ADMIN_PASSWORD"); v != "" {
		c.ensureAdminSlot()
		c.Auth.Admins[0].Password = hashPassword(v)
	}
	if v := os.Getenv("TASK_SCHEDULE_DB_DRIVER"); v != "" {
		c.Database.Driver = v
	}
	if v := os.Getenv("TASK_SCHEDULE_MYSQL_DSN"); v != "" {
		c.Database.MySQL.DSN = v
	}
	if v := os.Getenv("TASK_SCHEDULE_SERVER_MODE"); v != "" {
		c.Server.Mode = v
	}
	if v := os.Getenv("TASK_SCHEDULE_POWERJOB_SERVER_ADDRESS"); v != "" {
		c.PowerJob.ServerAddress = v
	}
}

// applyDefaults 填充缺失配置的默认值
func (c *Config) applyDefaults() {
	if c.Server.Port == 0 {
		c.Server.Port = 8080
	}
	if c.Server.Mode == "" {
		c.Server.Mode = "debug"
	}
	if c.Server.MaxRequestBodyMB == 0 {
		c.Server.MaxRequestBodyMB = 2
	}
	if c.Database.Driver == "" {
		c.Database.Driver = "sqlite"
	}
	if c.Database.SQLite.Path == "" {
		c.Database.SQLite.Path = "./data/task-schedule.db"
	}
	if c.Database.MySQL.MaxOpenConns == 0 {
		c.Database.MySQL.MaxOpenConns = 50
	}
	if c.Database.MySQL.MaxIdleConns == 0 {
		c.Database.MySQL.MaxIdleConns = 10
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Dir == "" {
		c.Log.Dir = "./logs"
	}
	if c.Log.FileName == "" {
		c.Log.FileName = "task-schedule.log"
	}
	if c.Log.MaxSizeMB == 0 {
		c.Log.MaxSizeMB = 100
	}
	if c.Log.MaxBackups == 0 {
		c.Log.MaxBackups = 10
	}
	if c.Log.MaxAgeDays == 0 {
		c.Log.MaxAgeDays = 30
	}
	if c.Scheduler.IntervalMs == 0 {
		c.Scheduler.IntervalMs = 1000
	}
	if c.Worker.TimeoutSeconds == 0 {
		c.Worker.TimeoutSeconds = 600
	}
	// 管理员账户为空时种默认占位(dev 便利);release 防呆由 Auth.Validate 拒绝。
	if len(c.Auth.Admins) == 0 {
		c.Auth.Admins = []AdminAccount{{
			Username: defaultAdminUsername,
			Password: hashPassword(defaultAdminPassword),
		}}
	}
	if c.Auth.Session.TTLSeconds == 0 {
		c.Auth.Session.TTLSeconds = 86400
	}
}

// ensureAdminSlot 保证 Admins[0] 存在(供 env 注入 admin 用户名/密码时写入)。
func (c *Config) ensureAdminSlot() {
	if len(c.Auth.Admins) == 0 {
		c.Auth.Admins = append(c.Auth.Admins, AdminAccount{})
	}
}

// hashPassword bcrypt 哈希明文。生成失败(如超 72 字节)返回原值——由 Validate 在
// release 模式兜底拒绝(非 bcrypt 格式校验失败),避免这里 panic。
func hashPassword(plain string) string {
	h, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil {
		return plain
	}
	return string(h)
}

// isBcryptHash 判断是否为合法 bcrypt 哈希格式($2a/$2b/$2y$ 前缀,60 字符)。
func isBcryptHash(s string) bool {
	if len(s) != 60 {
		return false
	}
	return strings.HasPrefix(s, "$2a$") || strings.HasPrefix(s, "$2b$") || strings.HasPrefix(s, "$2y$")
}

// Validate 校验管理员账户配置安全性。release=true 时严格(拒绝空/默认占位/格式非法/重名);
// release=false 时仅做结构性兜底(返回 nil,问题由启动日志提示)。由装配层(main.go)在
// Load 后按 Server.Mode 调用。
func (a *Auth) Validate(release bool) error {
	if !release {
		return nil
	}
	if len(a.Admins) == 0 {
		return errors.New("release 模式必须配置至少一个管理员账户(auth.admins 或 TASK_SCHEDULE_ADMIN_USERNAME/PASSWORD)")
	}
	seen := make(map[string]bool)
	for _, ad := range a.Admins {
		if strings.TrimSpace(ad.Username) == "" {
			return errors.New("管理员账户 username 不能为空")
		}
		if ad.Password == "" {
			return fmt.Errorf("管理员账户 %s 的 password 不能为空", ad.Username)
		}
		if !isBcryptHash(ad.Password) {
			return fmt.Errorf("管理员账户 %s 的 password 非合法 bcrypt 哈希(60 字符,$2a/$2b/$2y$ 前缀)", ad.Username)
		}
		if bcrypt.CompareHashAndPassword([]byte(ad.Password), []byte(defaultAdminPassword)) == nil {
			return fmt.Errorf("管理员账户 %s 使用了默认占位密码 %q,release 模式禁止(请通过环境变量 TASK_SCHEDULE_ADMIN_PASSWORD 注入强密码)", ad.Username, defaultAdminPassword)
		}
		if seen[ad.Username] {
			return fmt.Errorf("管理员用户名重复: %s", ad.Username)
		}
		seen[ad.Username] = true
	}
	return nil
}
