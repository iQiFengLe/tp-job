package domain

import "testing"

func TestStatusTerminal(t *testing.T) {
	for _, s := range []string{StatusSuccess, StatusFailed, StatusTimeout, StatusSkipped, StatusCanceled, StatusStopped} {
		if !StatusTerminal(s) {
			t.Errorf("%q 应为终态", s)
		}
	}
	for _, s := range []string{StatusQueued, StatusWaitingReceive, StatusRunning} {
		if StatusTerminal(s) {
			t.Errorf("%q 不应为终态", s)
		}
	}
	if StatusTerminal("hacked") {
		t.Error("非法状态不应判为终态")
	}
}

func TestStatusValid(t *testing.T) {
	for _, s := range []string{StatusQueued, StatusWaitingReceive, StatusRunning, StatusSuccess,
		StatusFailed, StatusTimeout, StatusSkipped, StatusCanceled, StatusStopped} {
		if !StatusValid(s) {
			t.Errorf("%q 应合法", s)
		}
	}
	if StatusValid("hacked") {
		t.Error("非法状态不应通过校验")
	}
	if got := TerminalStatuses(); len(got) != 6 {
		t.Errorf("终态应为 6 个, got %d (%v)", len(got), got)
	}
}

func TestStatusRetryable(t *testing.T) {
	for _, s := range []string{StatusFailed, StatusTimeout} {
		if !StatusRetryable(s) {
			t.Errorf("%q 应可重试", s)
		}
	}
	for _, s := range []string{StatusQueued, StatusWaitingReceive, StatusRunning, StatusSuccess,
		StatusSkipped, StatusCanceled, StatusStopped} {
		if StatusRetryable(s) {
			t.Errorf("%q 不应可重试", s)
		}
	}
}

func TestRootOf(t *testing.T) {
	if got := RootOf(nil); got != 0 {
		t.Errorf("nil 应返回 0, got %d", got)
	}
	root := &Instance{ID: 7, RootInstanceID: 0}
	if got := RootOf(root); got != 7 {
		t.Errorf("root 自身应返回 ID=7, got %d", got)
	}
	retry := &Instance{ID: 9, RootInstanceID: 7}
	if got := RootOf(retry); got != 7 {
		t.Errorf("重试实例应返回链首 id=7, got %d", got)
	}
}
