package powerjob

import "testing"

// TestPriorityFromInstanceParams 锁定 instanceParams 内 JSON priority 的解析与容错。
// 约定:非 JSON / 无 priority / 类型不可识别一律回退 0(保持原触发行为)。
func TestPriorityFromInstanceParams(t *testing.T) {
	cases := []struct {
		name   string
		params string
		want   int
	}{
		{"空串回退0", "", 0},
		{"非JSON回退0", "not-a-json", 0},
		{"JSON无priority回退0", `{"foo":"bar"}`, 0},
		{"priority数字", `{"priority":5}`, 5},
		{"priority夹在其他业务字段中", `{"bizId":42,"priority":7,"extra":"x"}`, 7},
		{"priority字符串数字", `{"priority":"3"}`, 3},
		{"priority字符串带空白", `{"priority":" 9 "}`, 9},
		{"priority非数字字符串回退0", `{"priority":"high"}`, 0},
		{"priority为null回退0", `{"priority":null}`, 0},
		{"priority为布尔回退0", `{"priority":true}`, 0},
		{"priority浮点截断", `{"priority":5.9}`, 5},
		{"priority零值", `{"priority":0}`, 0},
		{"priority负值", `{"priority":-2}`, -2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := priorityFromInstanceParams(tc.params); got != tc.want {
				t.Fatalf("priorityFromInstanceParams(%q) = %d, want %d", tc.params, got, tc.want)
			}
		})
	}
}
