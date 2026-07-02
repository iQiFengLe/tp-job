package instancelog

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAppendAndRead(t *testing.T) {
	l := New(t.TempDir(), 0)
	l.Append(1, 10, 0, LogEntry{Time: time.Now(), Kind: "CREATE", Level: "info", Message: "ins created"})
	l.Append(1, 10, 0, LogEntry{Time: time.Now(), Kind: "STATUS", Level: "info", Message: "queued->running"})

	lines, total, err := l.Read(1, 10, 0, LogQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(lines) != 2 {
		t.Fatalf("应 2 行, got total=%d len=%d", total, len(lines))
	}
}

func TestReadMissing(t *testing.T) {
	l := New(t.TempDir(), 0)
	lines, total, err := l.Read(1, 99, 0, LogQuery{})
	if err != nil || total != 0 || len(lines) != 0 {
		t.Fatalf("不存在应返回空, got lines=%d total=%d err=%v", len(lines), total, err)
	}
}

// 并发写同一文件:per-file mutex 保证不丢行。
func TestConcurrentAppend(t *testing.T) {
	l := New(t.TempDir(), 0)
	const n = 200
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			l.Append(1, 10, 0, LogEntry{Kind: "WORKER", Message: "m"})
		}()
	}
	wg.Wait()
	_, total, err := l.Read(1, 10, 0, LogQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if total != n {
		t.Fatalf("并发写应得 %d 行, got %d", n, total)
	}
}

// ReadGroup:同一次触发的首次+重试聚合(按 instanceID 排序),不含其他触发的噪音。
func TestReadGroup(t *testing.T) {
	l := New(t.TempDir(), 0)
	// 触发 A:首次 id=5(root=0),重试 id=8(root=5)
	l.Append(1, 5, 0, LogEntry{Kind: "CREATE", Message: "first"})
	l.Append(1, 8, 5, LogEntry{Kind: "CREATE", Message: "retry1"})
	// 触发 B(噪音):id=9(root=0)
	l.Append(1, 9, 0, LogEntry{Kind: "CREATE", Message: "other"})

	lines, total, err := l.ReadGroup(1, 5, LogQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("root=5 应聚合首次+重试共 2 行, got %d: %v", total, lines)
	}
	// 首次(id=5)在重试(id=8)前
	if !strings.Contains(lines[0], "first") || !strings.Contains(lines[1], "retry1") {
		t.Fatalf("应按 instanceID 排序(首次在前), got %v", lines)
	}
}

func TestSweep(t *testing.T) {
	l := New(t.TempDir(), time.Microsecond*100) // 极短保留期
	l.Append(1, 1, 0, LogEntry{Kind: "CREATE", Message: "old"})
	time.Sleep(time.Millisecond * 50)
	l.Append(1, 2, 0, LogEntry{Kind: "CREATE", Message: "new"})

	// 等首条超过保留期(用当前时刻扫;旧文件 mtime 已 >100µs)
	time.Sleep(time.Millisecond * 100)
	removed := l.Sweep(time.Now())
	if removed < 1 {
		t.Fatalf("应至少清理 1 个旧文件, got %d", removed)
	}
}
