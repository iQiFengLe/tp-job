package repository

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
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
		db, err := gorm.Open(sqlite.Open(cfg.SQLite.Path), gormCfg)
		if err != nil {
			return nil, fmt.Errorf("打开 sqlite 失败: %w", err)
		}
		// SQLite 是单写者模型:连接池限制为 1,保证写串行,避免调度器高并发触发下
		// 多个连接竞争写锁而频繁 "database is locked"。读也随之串行——对本服务读写比可接受,
		// 可靠性优先于吞吐。
		sqlDB, err := db.DB()
		if err != nil {
			return nil, fmt.Errorf("获取底层 sqlDB 失败: %w", err)
		}
		sqlDB.SetMaxOpenConns(1)
		sqlDB.SetMaxIdleConns(1)
		// 开启 WAL 与忙等待,提升并发读写性能。PRAGMA 正常不失败;失败不阻断启动
		// (此时全局 slog 尚未就绪),用标准 log 记录以便排查。
		for _, p := range []string{"PRAGMA journal_mode=WAL", "PRAGMA busy_timeout=5000", "PRAGMA foreign_keys=ON"} {
			if res := db.Exec(p); res.Error != nil {
				log.Printf("sqlite 执行 %q 失败: %v", p, res.Error)
			}
		}
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
