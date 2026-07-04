// Package wire 提供协议层(wire)共用的 JSON 解组工具。
//
// 多语言 worker(.NET/Python 等)上报的 JSON 数值字段,值类型常常不一致:同一个 worker 在
// reportLog 里把 instanceId 发成字符串("5"),在 reportInstanceStatus 里又发成数字(5)。原版
// Java server 用 Jackson 自动兼容两者,但 Go encoding/json 严格按类型匹配——字符串写法解析到
// int64 字段会直接置零,导致关联不到实例/日志丢失。FlexInt64 兼容两种写法,供所有协议层 DTO 复用。
package wire

import (
	"strconv"
	"strings"
)

// FlexInt64 兼容 JSON 中「数字」与「字符串」两种写法的 int64(对齐 Java Jackson 的宽松解析)。
// 解析失败(空/null/非法数字)置 0 且不返回 error——避免单条字段解析失败阻断整个批量请求。
// 底层是 int64,可直接 int64(f) 转换用于查 DB / map key。
type FlexInt64 int64

// UnmarshalJSON 兼容 5 / "5" / "" / null 四种写法,均解析为零值或对应数值。
func (f *FlexInt64) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), `"`)
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		*f = 0
		return nil
	}
	*f = FlexInt64(v)
	return nil
}
