package repository

import (
	"testing"
	"time"

	"tp-job/internal/domain"
)

func TestCallbackListDueAndTransitions(t *testing.T) {
	st := newTestStore(t)
	now := time.Now()
	future := now.Add(time.Hour)
	pending := &domain.Callback{JobID: 1, AppID: 1, EventStatus: "running", URL: "http://x",
		State: domain.CallbackPending, NextRetryAt: &now}
	notDue := &domain.Callback{JobID: 1, AppID: 1, EventStatus: "running", URL: "http://x",
		State: domain.CallbackPending, NextRetryAt: &future}
	if err := st.Callback.Create(pending); err != nil {
		t.Fatal(err)
	}
	if err := st.Callback.Create(notDue); err != nil {
		t.Fatal(err)
	}

	list, err := st.Callback.ListDue(now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != pending.ID {
		t.Errorf("ListDue 应只返回到期的 pending, got %d 条", len(list))
	}

	// MarkSent → 不再被 ListDue 扫到
	if err := st.Callback.MarkSent(pending.ID); err != nil {
		t.Fatal(err)
	}
	if list2, _ := st.Callback.ListDue(now, 10); len(list2) != 0 {
		t.Errorf("sent 后 ListDue 应空, got %d", len(list2))
	}

	// MarkRetry 推进 attempt + next_retry_at
	if err := st.Callback.MarkRetry(notDue.ID, 1, now, "e"); err != nil {
		t.Fatal(err)
	}
	var got domain.Callback
	st.DB.First(&got, notDue.ID)
	if got.Attempt != 1 {
		t.Errorf("attempt 应 1, got %d", got.Attempt)
	}

	// MarkDead
	if err := st.Callback.MarkDead(notDue.ID, "boom"); err != nil {
		t.Fatal(err)
	}
	st.DB.First(&got, notDue.ID)
	if got.State != domain.CallbackDead {
		t.Errorf("应 dead, got %s", got.State)
	}
}

// PurgeOld 只删 sent/dead 且超保留期;pending 永不删(未投递保证)。
func TestCallbackPurgeOld(t *testing.T) {
	st := newTestStore(t)
	old := time.Now().Add(-8 * 24 * time.Hour) // 8 天前(超 7 天保留)
	for _, st2 := range []string{domain.CallbackSent, domain.CallbackDead, domain.CallbackPending} {
		cb := &domain.Callback{JobID: 1, AppID: 1, EventStatus: "running", URL: "http://x",
			State: st2, NextRetryAt: &old}
		if err := st.Callback.Create(cb); err != nil {
			t.Fatal(err)
		}
	}
	// 全部 updated_at 改到 8 天前
	st.DB.Model(&domain.Callback{}).Where("1=1").Update("updated_at", old)

	n, err := st.Callback.PurgeOld(time.Now().Add(-7 * 24 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("应删 2 条(sent+dead), got %d", n)
	}
	var cnt int64
	st.DB.Model(&domain.Callback{}).Where("state = ?", domain.CallbackPending).Count(&cnt)
	if cnt != 1 {
		t.Errorf("pending 永不删,应剩 1, got %d", cnt)
	}
}
