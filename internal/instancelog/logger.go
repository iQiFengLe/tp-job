// Package instancelog 把每次实例执行的过程日志落到本地文件:
//
//	{baseDir}/instances/{appID}/{instanceID}_{rootInstanceID}.log
//
// 调度事件(CREATE/SCHEDULE/DISPATCH/STATUS/RETRY/REAP)与 worker 上报日志混写同一文件,
// 呈现一次执行的完整时间线。per-file mutex 保证多 goroutine 写有序。
//
// 文件名 {instanceID}_{rootInstanceID} 是面向 ssh/外部程序的可观测性契约:首次实例 root=0、
// 重试挂链首 id,在文件系统按名即可还原一次触发的链路;程序内不再提供聚合读取(网页按单实例读)。
package instancelog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// LogEntry 一条实例日志事件。调度事件与 worker 上报日志统一用此结构。
type LogEntry struct {
	Time    time.Time
	Kind    string // CREATE/SCHEDULE/DISPATCH/STATUS/WORKER/RETRY/REAP
	Level   string // info/warn/error;worker 上报保留原级
	Message string
}

// LogQuery Read 的分页参数。
type LogQuery struct {
	Offset int // 行偏移(从 0 起)
	Limit  int // 最多返回行数;<=0 表示不限
}

// Logger 实例日志器。
type Logger struct {
	baseDir   string        // .../instances
	retention time.Duration // >0 时 Sweep 据此删旧文件

	mu    sync.Mutex
	locks map[string]*sync.Mutex // per-file lock
}

// New 创建 Logger。baseDir 通常为 cfg.Log.Dir;retention<=0 表示不自动清理。
func New(baseDir string, retention time.Duration) *Logger {
	return &Logger{baseDir: filepath.Join(baseDir, "instances"), retention: retention, locks: make(map[string]*sync.Mutex)}
}

func (l *Logger) path(appID, instanceID, rootID int64) string {
	return filepath.Join(l.baseDir, fmt.Sprintf("%d/%d_%d.log", appID, instanceID, rootID))
}

func (l *Logger) fileLock(path string) *sync.Mutex {
	l.mu.Lock()
	defer l.mu.Unlock()
	if m, ok := l.locks[path]; ok {
		return m
	}
	m := &sync.Mutex{}
	l.locks[path] = m
	return m
}

// Append 追加一条事件到实例日志文件。
func (l *Logger) Append(appID, instanceID, rootID int64, e LogEntry) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	line := fmt.Sprintf("[%s] [%s] (%s) %s\n",
		e.Time.Format(time.RFC3339Nano), strings.ToLower(e.Level), strings.ToUpper(e.Kind), e.Message)
	path := l.path(appID, instanceID, rootID)
	mu := l.fileLock(path)
	mu.Lock()
	defer mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.WriteString(line)
}

// Read 读取某实例日志文件(按行,带 offset/limit 分页)。文件不存在返回空。
func (l *Logger) Read(appID, instanceID, rootID int64, q LogQuery) ([]string, int, error) {
	data, err := os.ReadFile(l.path(appID, instanceID, rootID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	return paginate(splitLines(data), q)
}

// Sweep 删除 mtime 早于 retention 的实例日志,返回清理数。retention<=0 时 noop。
func (l *Logger) Sweep(now time.Time) int {
	if l.retention <= 0 {
		return 0
	}
	cutoff := now.Add(-l.retention)
	removed := 0
	_ = filepath.WalkDir(l.baseDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if info.ModTime().Before(cutoff) {
			if os.Remove(path) == nil {
				removed++
				// 同步回收 per-file 锁条目,避免 locks map 长期增长
				l.mu.Lock()
				delete(l.locks, path)
				l.mu.Unlock()
			}
		}
		return nil
	})
	return removed
}

// Run 周期清理过期实例日志(retention>0 时);retention<=0 时 no-op(不清理,立即返回)。
// 间隔取 retention/2(下限 1h、上限 24h):retention=7d→每天扫;retention=2h→每 2h 扫。
// 装配层应 `go il.Run(ctx)`。Sweep 按 mtime 删旧文件并回收 per-file 锁条目。
func (l *Logger) Run(ctx context.Context) {
	if l.retention <= 0 {
		return
	}
	interval := l.retention / 2
	if interval > 24*time.Hour {
		interval = 24 * time.Hour
	}
	if interval < time.Hour {
		interval = time.Hour
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.Sweep(time.Now())
		}
	}
}

func paginate(lines []string, q LogQuery) ([]string, int, error) {
	total := len(lines)
	offset := q.Offset
	if offset < 0 {
		offset = 0
	}
	if offset > total {
		offset = total
	}
	end := total
	if q.Limit > 0 && offset+q.Limit < end {
		end = offset + q.Limit
	}
	return lines[offset:end], total, nil
}

func splitLines(data []byte) []string {
	s := strings.TrimRight(string(data), "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}
