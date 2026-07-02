package repository

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"

	"task-schedule/internal/domain"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "test.db")), &gorm.Config{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	st, err := FromDB(db)
	if err != nil {
		t.Fatalf("FromDB: %v", err)
	}
	return st
}

func TestAppCRUD(t *testing.T) {
	st := newTestStore(t)
	app := &domain.App{AppName: "demo", Password: "h", Status: 1}
	if err := st.App.Create(app); err != nil {
		t.Fatalf("create: %v", err)
	}
	if app.ID == 0 {
		t.Fatal("ID 应自增(不可用户指定)")
	}
	got, err := st.App.GetByName("demo")
	if err != nil || got.ID != app.ID {
		t.Fatalf("GetByName 失败: got=%v err=%v", got, err)
	}
}

func TestJobAdvanceNextRun(t *testing.T) {
	st := newTestStore(t)
	now := time.Now()
	later := now.Add(time.Hour)
	job := &domain.Job{AppID: 1, Name: "j", ExecuteType: "http", NextRunTime: &now}
	if err := st.Job.Create(job); err != nil {
		t.Fatal(err)
	}
	// AppName 唯一约束:同 app 第二个 job 应允许(不要求 job 名唯一)
	if _, _, err := st.Job.List(1, 1, 20); err != nil {
		t.Fatalf("List: %v", err)
	}

	ok, err := st.Job.AdvanceNextRun(job.ID, now, &later)
	if err != nil || !ok {
		t.Fatalf("首次推进应成功: ok=%v err=%v", ok, err)
	}
	// oldNext 不匹配 → 认领失败(防并发重复触发)
	ok, _ = st.Job.AdvanceNextRun(job.ID, now, &later)
	if ok {
		t.Fatal("oldNext 不匹配应判为已被认领")
	}
}

func TestInstanceTerminalGuard(t *testing.T) {
	st := newTestStore(t)
	ins := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusRunning}
	if err := st.Instance.Create(ins); err != nil {
		t.Fatal(err)
	}
	if err := st.Instance.UpdateResult(ins.ID, domain.StatusSuccess, "ok"); err != nil {
		t.Fatal(err)
	}
	// 终态守护:迟到 failed 不应覆盖 success
	if err := st.Instance.UpdateResult(ins.ID, domain.StatusFailed, "late"); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Instance.Get(ins.ID)
	if got.Status != domain.StatusSuccess {
		t.Fatalf("终态不应被乱序上报覆盖, got %s", got.Status)
	}
}

func TestInstanceRetryDue(t *testing.T) {
	st := newTestStore(t)
	ins := &domain.Instance{JobID: 1, AppID: 1, Status: domain.StatusFailed}
	if err := st.Instance.Create(ins); err != nil {
		t.Fatal(err)
	}
	if err := st.Instance.SetNextRetryTime(ins.ID, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	list, err := st.Instance.ListRetryDue(time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != ins.ID {
		t.Fatalf("应命中 1 个到期重试, got %d", len(list))
	}
	// ClearNextRetryTime 去重
	got, _ := st.Instance.ClearNextRetryTime(ins.ID)
	if !got {
		t.Fatal("首次清应抢到")
	}
	got2, _ := st.Instance.ClearNextRetryTime(ins.ID)
	if got2 {
		t.Fatal("二次清应空手而归(去重)")
	}
}
