package repository

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"task-schedule/internal/config"
)

// OpenDatabase 按配置打开数据库(sqlite 或 mysql)并返回 *gorm.DB,不迁移表结构
// (AutoMigrate 由 FromDB 完成)。sqlite 走 WAL + 单写者连接池,保证调度器高并发下
// 不频繁 "database is locked";mysql 设连接池上限。
func OpenDatabase(cfg config.Database) (*gorm.DB, error) {
	gormCfg := &gorm.Config{
		Logger: logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags),
			logger.Config{
				SlowThreshold: 200 * time.Millisecond,
				LogLevel:      logger.Warn,
				Colorful:      false,
			},
		),
	}
	switch cfg.Driver {
	case "mysql":
		db, err := gorm.Open(mysql.Open(cfg.MySQL.DSN), gormCfg)
		if err != nil {
			return nil, fmt.Errorf("连接 mysql 失败: %w", err)
		}
		sqlDB, err := db.DB()
		if err != nil {
			return nil, err
		}
		sqlDB.SetMaxOpenConns(cfg.MySQL.MaxOpenConns)
		sqlDB.SetMaxIdleConns(cfg.MySQL.MaxIdleConns)
		sqlDB.SetConnMaxLifetime(time.Hour)
		return db, nil
	case "sqlite":
		if dir := filepath.Dir(cfg.SQLite.Path); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("创建数据库目录失败: %w", err)
			}
		}
		// PRAGMA 走 DSN query 参数:glebarez/modernc 在每个新连接初始化时执行 _pragma,
		// 保证池里每条连接都带 busy_timeout / foreign_keys / WAL。这些是连接级 PRAGMA
		// (只对当前连接生效),不能靠开池后 db.Exec 跑一次——那只命中第一个连接,后续新连接
		// 默认无 busy_timeout(写竞争直接 database is locked)、无外键约束。
		sep := "?"
		if strings.Contains(cfg.SQLite.Path, "?") {
			sep = "&"
		}
		dsn := cfg.SQLite.Path + sep + "_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)&_pragma=journal_mode(WAL)"
		db, err := gorm.Open(sqlite.Open(dsn), gormCfg)
		if err != nil {
			return nil, fmt.Errorf("打开 sqlite 失败: %w", err)
		}
		sqlDB, err := db.DB()
		if err != nil {
			return nil, fmt.Errorf("获取底层 sqlDB 失败: %w", err)
		}
		// WAL 模型:多读并发 + 单写者串行。写连接竞争写锁时由 busy_timeout(5s) 排队等,
		// 不再用 MaxOpenConns=1 强制读串行。连接数由配置控制(默认 8):读并发受益,写仍单写者,
		// 开太多只增加排队、无吞吐收益。⚠ 事务内严禁用根 db 再查——见 instance WithCallback
		// 注释(多连接下会拿到池里另一连接,读不到事务未提交数据,破坏隔离)。
		sqlDB.SetMaxOpenConns(cfg.SQLite.MaxOpenConns)
		sqlDB.SetMaxIdleConns(cfg.SQLite.MaxOpenConns) // idle=open,避免空闲回收后重建连接反复跑 PRAGMA
		return db, nil
	default:
		return nil, fmt.Errorf("不支持的数据库驱动: %s", cfg.Driver)
	}
}

// New 打开数据库并构建 domain 仓储(OpenDatabase + FromDB)。供 main 装配使用。
func New(cfg config.Database) (*Store, error) {
	db, err := OpenDatabase(cfg)
	if err != nil {
		return nil, err
	}
	st, err := FromDB(db)
	if err != nil {
		return nil, err
	}
	st.Driver = cfg.Driver
	return st, nil
}
