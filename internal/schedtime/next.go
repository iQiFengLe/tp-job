package schedtime

import (
	"fmt"
	"strconv"
	"time"

	"github.com/robfig/cron/v3"
)

// 标准 5 段 cron + 部分描述符(@ hourly 等)，与多数运维预期一致。
var defaultParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// ValidateCron 校验 cron 表达式是否合法。
func ValidateCron(expr string) error {
	if expr == "" {
		return fmt.Errorf("cron 表达式不能为空")
	}
	if _, err := defaultParser.Parse(expr); err != nil {
		return fmt.Errorf("非法 cron 表达式 %q: %w", expr, err)
	}
	return nil
}

// NextCron 返回 cron 表达式在 from 之后的下一次触发时间。
func NextCron(expr string, from time.Time) (time.Time, error) {
	sch, err := defaultParser.Parse(expr)
	if err != nil {
		return time.Time{}, fmt.Errorf("非法 cron 表达式 %q: %w", expr, err)
	}
	return sch.Next(from), nil
}

// NextByKind 按统一 ScheduleKind 计算下次执行时间(domain 模型用)。
//   - cron:标准 5 段表达式
//   - fix_rate / fix_delay:毫秒数(正整数)
//   - delay:秒数(正整数)
//   - api / run_at:返回 nil(不自动调度)
func NextByKind(kind, expr string, from time.Time) (*time.Time, error) {
	switch kind {
	case "cron":
		if expr == "" {
			return nil, fmt.Errorf("cron 缺少表达式")
		}
		n, err := NextCron(expr, from)
		if err != nil {
			return nil, err
		}
		return &n, nil
	case "fix_rate", "fix_delay":
		ms, err := strconv.ParseInt(expr, 10, 64)
		if err != nil || ms <= 0 {
			return nil, fmt.Errorf("%s 表达式必须是正整数毫秒", kind)
		}
		n := from.Add(time.Duration(ms) * time.Millisecond)
		return &n, nil
	case "delay":
		sec, err := strconv.Atoi(expr)
		if err != nil || sec <= 0 {
			return nil, fmt.Errorf("delay 表达式必须是正整数秒")
		}
		n := from.Add(time.Duration(sec) * time.Second)
		return &n, nil
	case "api", "run_at", "":
		return nil, nil
	}
	return nil, fmt.Errorf("未知 schedule_kind: %s", kind)
}

