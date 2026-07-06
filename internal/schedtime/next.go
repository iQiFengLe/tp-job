package schedtime

import (
	"fmt"
	"strconv"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"github.com/robfig/cron/v3"
)

// 标准 5 段 cron + 部分描述符(@ hourly 等),与多数运维预期一致。
// 仅消费自建 job(Web 表单填的标准 cron)。PowerJob/Quartz 6~7 段表达式(含秒/年/?/L/W/#)
// 走 go-quartz 引擎,见 NextCron/ValidateCron。
var defaultParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// ValidateCron 校验 cron 表达式合法性(双引擎):
//  1. robfig 5 段 + Descriptor(自建 cron 优先,语义不变);
//  2. go-quartz Quartz 6/7 段(含秒/年/?/L/W/#,PowerJob 同步任务用)。
func ValidateCron(expr string) error {
	if expr == "" {
		return fmt.Errorf("cron 表达式不能为空")
	}
	if _, err := defaultParser.Parse(expr); err == nil {
		return nil
	}
	if _, err := quartz.NewCronTrigger(expr); err != nil {
		return fmt.Errorf("非法 cron 表达式 %q: %w", expr, err)
	}
	return nil
}

// NextCron 返回 cron 表达式在 from 之后的下一次触发时间(双引擎,见 ValidateCron)。
// Quartz 引擎用 time.Local,与 robfig(sch.Next 沿用 from 的 loc,from=time.Now() 即 Local)
// 对齐,确保业务时区(如 Asia/Shanghai)写入的"9 点"按本地时区触发。
// 无未来触发(如 Quartz 一次性 cron 已过期)→ 返回 error,调用方据此置 next_run=nil。
func NextCron(expr string, from time.Time) (time.Time, error) {
	if sch, err := defaultParser.Parse(expr); err == nil {
		return sch.Next(from), nil
	}
	trigger, err := quartz.NewCronTriggerWithLoc(expr, time.Local)
	if err != nil {
		return time.Time{}, fmt.Errorf("非法 cron 表达式 %q: %w", expr, err)
	}
	nextNs, err := trigger.NextFireTime(from.UnixNano()) // go-quartz 入参/返回均为 UnixNano
	if err != nil {
		return time.Time{}, fmt.Errorf("cron %q 无未来触发时间: %w", expr, err)
	}
	return time.Unix(0, nextNs).In(time.Local), nil
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
		d := time.Duration(ms) * time.Millisecond
		// 防溢出:ms 极大时 Duration(ms)*Millisecond 可能溢出 int64 变负 → next 变过去时间 → 每 tick 触发的暴走 job。
		// 上限 1 年,杜绝极端配置。
		if d <= 0 || d > 365*24*time.Hour {
			return nil, fmt.Errorf("%s 表达式超出合理范围(0~1年)", kind)
		}
		n := from.Add(d)
		return &n, nil
	case "delay":
		sec, err := strconv.Atoi(expr)
		if err != nil || sec <= 0 {
			return nil, fmt.Errorf("delay 表达式必须是正整数秒")
		}
		d := time.Duration(sec) * time.Second
		if d <= 0 || d > 365*24*time.Hour {
			return nil, fmt.Errorf("delay 表达式超出合理范围(0~1年)")
		}
		n := from.Add(d)
		return &n, nil
	case "api", "run_at", "":
		return nil, nil
	}
	return nil, fmt.Errorf("未知 schedule_kind: %s", kind)
}

