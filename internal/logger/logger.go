package logger

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"gopkg.in/natefinch/lumberjack.v2"

	"tp-job/internal/config"
)

var defaultLogger *slog.Logger

// Init 初始化全局日志：同时输出到控制台和滚动文件。
func Init(cfg config.Log) (*slog.Logger, error) {
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("创建日志目录失败: %w", err)
	}

	lumber := &lumberjack.Logger{
		Filename:   filepath.Join(cfg.Dir, cfg.FileName),
		MaxSize:    cfg.MaxSizeMB,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxAgeDays,
		Compress:   true,
	}

	// 控制台 + 文件双写
	multi := io.MultiWriter(os.Stdout, lumber)

	level := parseLevel(cfg.Level)
	handler := slog.NewJSONHandler(multi, &slog.HandlerOptions{
		Level:     level,
		AddSource: false,
	})
	defaultLogger = slog.New(handler).With("service", "tp-job")
	slog.SetDefault(defaultLogger)
	return defaultLogger, nil
}

// L 返回全局 logger
func L() *slog.Logger {
	if defaultLogger == nil {
		// 兜底，未初始化时使用默认 logger
		return slog.Default()
	}
	return defaultLogger
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
