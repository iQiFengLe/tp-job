package config

import (
	"errors"
	"fmt"
	"os"

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
	Pprof     Pprof     `yaml:"pprof"`
	Debug     Debug     `yaml:"debug"`
}

type Database struct {
	Driver string `yaml:"driver"` // sqlite | mysql
	SQLite struct {
		Path         string `yaml:"path"`
		MaxOpenConns int    `yaml:"max_open_conns"` // 连接池上限;WAL 下读并发、写串行(busy_timeout 排队),默认 8
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

	// InstanceRetentionDays 实例日志文件保留天数(清理按文件 mtime)。
	// 0=未设置(applyDefaults 兜底 90);>0=保留天数;-1=不清理(显式逃生口,instancelog 以 retention<=0 判定)。
	InstanceRetentionDays int `yaml:"instance_retention_days"`
}

// Scheduler 统一调度器参数。新 dispatch 调度器自带并发槽/排队语义,无需触发并发数等旧参数。
type Scheduler struct {
	IntervalMs int         `yaml:"interval_ms"` // 扫描周期(毫秒);reaper/retry/callback pump 复用
	Callback   CallbackCfg `yaml:"callback"`    // 实例状态变更回调
}

// CallbackCfg 实例状态变更回调配置。Job 配 callback_url 时,实例每次状态变化 POST 通知,至少一次。
type CallbackCfg struct {
	Enabled        bool `yaml:"enabled"`          // 总开关;false 时 hook 不构造回调,零开销
	MaxAttempts    int  `yaml:"max_attempts"`     // 最大投递尝试次数,达上限置 dead;默认 8
	BackoffBaseSec int  `yaml:"backoff_base_sec"` // 指数退避基数(秒) 2^attempt;默认 10
	BackoffMaxSec  int  `yaml:"backoff_max_sec"`  // 退避上限(秒);默认 3600
	TimeoutSec     int  `yaml:"timeout_sec"`      // 单次 POST 超时(秒);默认 10
	RetentionDays  int  `yaml:"retention_days"`   // sent/dead 记录保留天数(审计);默认 7
}

type Worker struct {
	TimeoutSeconds int `yaml:"timeout_seconds"` // worker 心跳超时(秒),超过视为离线
	WarmupSeconds  int `yaml:"warmup_seconds"`  // reaper 启动宽限(秒):窗口内跳过"worker 失联"判定,给重启后 worker 重新心跳注册留时间,避免误杀重启前在飞的正常实例(默认 30)
}

// PowerJob 兼容协议配置。/server/* + /openApi/* 端点始终挂载(供遵循 PowerJob 协议的自研 http worker /
// 业务系统接入;不支持官方 Java processor)。这里只保留 worker 可达地址。无独立调度循环——所有 job
// 统一由 dispatch 调度器处理。
type PowerJob struct {
	ServerAddress string `yaml:"server_address"` // /server/acquire 返回值;PowerJob worker 可达的 host:port
}

// Pprof pprof 诊断端点配置(net/http/pprof,注册到 DefaultServeMux,独立 listener,主 gin 服务卡死时
// 仍可抓栈定位)。默认关闭——生产不开,排查死锁/阻塞时在 config 开启。⚠ pprof 无鉴权,listen 用
// 0.0.0.0 时会暴露给可达网络(可读 goroutine 栈/堆 profile),仅本机(127.0.0.1)或可信网络启用。
type Pprof struct {
	Enabled bool   `yaml:"enabled"` // 是否启用;默认 false
	Listen  string `yaml:"listen"`  // 监听地址 host:port;默认 127.0.0.1:6060
}

// Debug 调试便利开关。仅供本地/开发环境,生产(release)必须全部关闭。
// 字段零值即"关闭"——未显式配置时默认安全(不开放任何便利特性)。
type Debug struct {
	// AutoLogin 是否启用 POST /api/auth/auto-login 端点:开启时,前端无 token 可匿名用默认管理员
	// 账户(admin/admin123)自动登录,免开发手输。⚠ 仅本地调试:生产必须 false,否则任何人可匿名登入
	// (即便登录限流 + 首登改密,也等于把登录页向公网敞开)。未配置=false。
	AutoLogin bool `yaml:"auto_login"`
}

// Auth 管理端鉴权配置:登录会话参数 + 登录端点限流。管理员账户走 admin_user 表
// (首次启动 seed admin/admin123,之后 Web 可改用户名/密码),不再经配置/环境变量注入。
// 应用账户走 app 表(AppName + bcrypt Password)。worker 心跳不走登录(靠 appName + 网络隔离)。
type Auth struct {
	Session SessionConfig `yaml:"session"` // 登录会话参数
	Login   LoginConfig   `yaml:"login"`   // 登录端点限流(防爆破 + bcrypt DoS)
}

// SessionConfig 登录会话参数。
type SessionConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"` // 会话有效期(秒);默认 86400(24h)
}

// LoginConfig 登录端点限流参数。bcrypt 单次校验耗时较高,无限流时登录端点可被当作
// 资源放大型 DoS(海量请求消耗 CPU);同时为密码爆破设一道 IP 级闸门。
type LoginConfig struct {
	MaxAttemptsPerMin int `yaml:"max_attempts_per_min"` // 每 IP 每分钟最大尝试次数;0=不限(默认,向后兼容)
}

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
// 避免把 mysql dsn 等明文写进 config.yaml。空值视为未设置,不覆盖。
//
// server.mode 不支持 env 覆盖:env 把 release 降级 debug 会绕过登录限流强制(Validate 据 Mode
// 判定 release),故 mode 仅由 config.yaml 决定。部署需 release 时改 yaml 或用 config.release.yaml。
func (c *Config) applyEnv() {
	if v := os.Getenv("DIDA_DB_DRIVER"); v != "" {
		c.Database.Driver = v
	}
	if v := os.Getenv("DIDA_MYSQL_DSN"); v != "" {
		c.Database.MySQL.DSN = v
	}
	if v := os.Getenv("DIDA_POWERJOB_SERVER_ADDRESS"); v != "" {
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
		c.Database.SQLite.Path = "./data/dida.db"
	}
	if c.Database.SQLite.MaxOpenConns == 0 {
		c.Database.SQLite.MaxOpenConns = 8
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
		c.Log.FileName = "dida.log"
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
	if c.Log.InstanceRetentionDays == 0 {
		c.Log.InstanceRetentionDays = 90
	}
	if c.Scheduler.IntervalMs == 0 {
		c.Scheduler.IntervalMs = 1000
	}
	if c.Scheduler.Callback.MaxAttempts == 0 {
		c.Scheduler.Callback.MaxAttempts = 8
	}
	if c.Scheduler.Callback.BackoffBaseSec == 0 {
		c.Scheduler.Callback.BackoffBaseSec = 10
	}
	if c.Scheduler.Callback.BackoffMaxSec == 0 {
		c.Scheduler.Callback.BackoffMaxSec = 3600
	}
	if c.Scheduler.Callback.TimeoutSec == 0 {
		c.Scheduler.Callback.TimeoutSec = 10
	}
	if c.Scheduler.Callback.RetentionDays == 0 {
		c.Scheduler.Callback.RetentionDays = 7
	}
	if c.Worker.TimeoutSeconds == 0 {
		c.Worker.TimeoutSeconds = 600
	}
	if c.Worker.WarmupSeconds == 0 {
		c.Worker.WarmupSeconds = 30
	}
	if c.Auth.Session.TTLSeconds == 0 {
		c.Auth.Session.TTLSeconds = 86400
	}
	if c.Pprof.Listen == "" {
		c.Pprof.Listen = "127.0.0.1:6060"
	}
}

// Validate 登录安全校验。管理员账户已入库(首次 seed admin/admin123,无空账户/弱口令裸奔风险),
// 故只校验登录端点限流:bcrypt 单次校验耗 CPU,无限流时登录端点可被当作资源放大型 DoS,亦可被
// 密码爆破。release 模式强制要求显式配置(建议 10~20);debug 不校验。由装配层(main.go)在
// Load 后按 Server.Mode 调用。
func (a *Auth) Validate(release bool) error {
	if !release {
		return nil
	}
	if a.Login.MaxAttemptsPerMin <= 0 {
		return errors.New("release 模式必须显式配置 auth.login.max_attempts_per_min(登录限流,防爆破+bcrypt DoS;建议 10~20)")
	}
	return nil
}
