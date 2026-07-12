package dservice

import (
	"strings"
	"testing"

	"tp-job/internal/dispatch"
	"tp-job/internal/domain"
	"tp-job/internal/instancelog"
)

// TestTruncate 验证按 rune 截断 + 超长追加省略号(按 rune 避免切断 UTF-8 多字节字符)。
func TestTruncate(t *testing.T) {
	cases := []struct {
		in   string
		max  int
		want string
	}{
		{"", 80, ""},
		{"短", 80, "短"},
		{"abcdef", 3, "abc…"},
		{"中文按rune", 2, "中文…"},
		{"ab", 2, "ab"}, // 等长不截断
	}
	for _, c := range cases {
		if got := truncate(c.in, c.max); got != c.want {
			t.Fatalf("truncate(%q,%d)=%q want %q", c.in, c.max, got, c.want)
		}
	}
}

// TestReportStatusWritesStatusLog 验证 worker 回报真状态变化时写 STATUS 事件到实例日志,
// 终态守护(rows==0 终态重放)不重复写——锁定单实例时间线的状态变迁埋点。
func TestReportStatusWritesStatusLog(t *testing.T) {
	st, sch, il := newSvc(t)
	isvc := NewInstanceService(st, sch, il, dispatch.NoopCallbackBuilder{})

	_ = st.App.Create(&domain.App{ID: 1, AppName: "a"})
	_ = st.Job.Create(&domain.Job{ID: 1, AppID: 1, Name: "j"})
	ins := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusWaitingReceive}
	if err := st.Instance.Create(ins); err != nil {
		t.Fatal(err)
	}
	read := func() []string {
		l, _, _ := il.Read(ins.AppID, ins.ID, ins.RootInstanceID, instancelog.LogQuery{})
		return l
	}

	// 真变化:waiting_receive → running → success,各写一条 STATUS
	if err := isvc.ReportStatus(ins.ID, domain.StatusRunning, "ok"); err != nil {
		t.Fatal(err)
	}
	if err := isvc.ReportStatus(ins.ID, domain.StatusSuccess, "done"); err != nil {
		t.Fatal(err)
	}
	lines := read()
	if len(lines) != 2 {
		t.Fatalf("应 2 条 STATUS 日志, got %d: %v", len(lines), lines)
	}
	if !strings.Contains(lines[0], "状态变迁") || !strings.Contains(lines[0], "→running") {
		t.Fatalf("首行应含 '状态变迁...→running', got %q", lines[0])
	}
	if !strings.Contains(lines[1], "→success") {
		t.Fatalf("次行应含 '→success', got %q", lines[1])
	}

	// 终态守护:success 已终态,再 ReportStatus 应 rows==0,不新增日志
	if err := isvc.ReportStatus(ins.ID, domain.StatusSuccess, "again"); err != nil {
		t.Fatal(err)
	}
	if got := len(read()); got != 2 {
		t.Fatalf("终态重放不应新增日志, got %d", got)
	}
}
