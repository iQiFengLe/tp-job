package schedtime

import (
	"testing"
	"time"
)

// Quartz 周日=1,周一=2,...,周六=7;Go time.Weekday Sunday=0,...,Saturday=6。
// 业务 buildCron WEEK 模式把 Java 周一(1)→quartz 2、周二→3;下方断言用 Go Weekday。

func TestValidateCron(t *testing.T) {
	valid := []string{
		"*/5 * * * *", // robfig 5 段
		"@daily",      // robfig Descriptor
		"0 0 9 * * ? *",
		"0 0 9 ? * 3 *", // Quartz 周二
		"0 0 9 6 * ? *",
		"0 0 9 6 7 ? *",
		"0 0 9 6 7 ? 2099",
		"0 0 0 L * ?",    // Quartz 月末
		"0 15 10 ? * 6L", // 每月最后一个周五
	}
	for _, e := range valid {
		if err := ValidateCron(e); err != nil {
			t.Errorf("ValidateCron(%q) 期望合法,得 %v", e, err)
		}
	}
	invalid := []string{
		"",        // 空
		"9 9 9",   // 缺字段(robfig 5 段不够、go-quartz 6/7 段也不够)
		"abc def", // 乱码
	}
	for _, e := range invalid {
		if err := ValidateCron(e); err == nil {
			t.Errorf("ValidateCron(%q) 期望非法", e)
		}
	}
}

func TestNextCronRobfig(t *testing.T) {
	now := time.Now()
	next, err := NextCron("*/5 * * * *", now)
	if err != nil {
		t.Fatalf("robfig */5: %v", err)
	}
	if !next.After(now) {
		t.Errorf("robfig */5 next %v 不在 now %v 之后", next, now)
	}
}

func TestNextCronQuartzModes(t *testing.T) {
	now := time.Now()
	for _, expr := range []string{
		"0 0 9 * * ? *", // DAY
		"0 0 9 ? * 3 *", // WEEK(周二)
		"0 0 9 6 * ? *", // MONTH
		"0 0 9 6 7 ? *", // YEAR
		"0 0 0 L * ?",   // 月末
	} {
		next, err := NextCron(expr, now)
		if err != nil {
			t.Errorf("NextCron(%q): %v", expr, err)
			continue
		}
		if !next.After(now) {
			t.Errorf("NextCron(%q) next %v 不在 now 之后", expr, next)
		}
	}
	// 周二语义核验:0 0 9 ? * 3 * → 下次落在周二(Go Weekday)
	next, err := NextCron("0 0 9 ? * 3 *", now)
	if err != nil {
		t.Fatalf("周二 cron: %v", err)
	}
	if w := next.Weekday(); w != time.Tuesday {
		t.Errorf("期望周二,得 %v(next=%v)", w, next)
	}
}

func TestNextCronNoneExpired(t *testing.T) {
	now := time.Now()
	_, err := NextCron("0 0 9 1 1 ? 2020", now) // 2020 已过
	if err == nil {
		t.Error("过期 NONE cron 期望返回无未来触发 error")
	}
}

func TestNextCronNoneFuture(t *testing.T) {
	now := time.Now()
	next, err := NextCron("0 0 9 1 1 ? 2099", now) // 2099-01-01 09:00
	if err != nil {
		t.Fatalf("未来 NONE: %v", err)
	}
	if next.Year() != 2099 {
		t.Errorf("期望 2099,得 %v", next.Year())
	}
}
