package workerreg

import (
	"testing"
	"time"

	"task-schedule/internal/domain"
)

func TestAddressPolicy(t *testing.T) {
	pol, err := NewAddressPolicy([]string{"10.0.0.0/8", "192.168.1.0/24", "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		addr string
		want bool
	}{
		{"10.1.2.3:8080", true},          // 在 10/8
		{"192.168.1.50:9000", true},      // 在 192.168.1/24
		{"127.0.0.1:1234", true},         // 单 IP /32
		{"8.8.8.8:80", false},            // 公网
		{"169.254.169.254:80", false},    // 云元数据地址(典型 SSRF 目标)应拒绝
		{"not-an-address", false},        // 不可解析
		{"", false},
	}
	for _, c := range cases {
		if got := pol.Allowed(c.addr); got != c.want {
			t.Errorf("Allowed(%q)=%v, want %v", c.addr, got, c.want)
		}
	}
}

func TestAddressPolicyEmpty(t *testing.T) {
	if p, err := NewAddressPolicy(nil); err != nil || p != nil {
		t.Fatalf("nil 入参应返回 nil policy, got %v err=%v", p, err)
	}
	if p, err := NewAddressPolicy([]string{"", "  "}); err != nil || p != nil {
		t.Fatalf("全空白应返回 nil, got %v err=%v", p, err)
	}
}

func TestAddressPolicyInvalid(t *testing.T) {
	if _, err := NewAddressPolicy([]string{"not-a-cidr!!"}); err == nil {
		t.Fatal("非法 CIDR 应报错")
	}
}

// 无 policy 时 AllowedAddress 恒 true(向后兼容)。
func TestRegistryAllowedAddressNoPolicy(t *testing.T) {
	r := New(time.Minute, nil)
	if !r.AllowedAddress("8.8.8.8:80") {
		t.Fatal("无 policy 时应允许任意地址")
	}
}

// 设了 policy 后,非白名单地址的 Heartbeat 被静默拒绝(不注册)。
func TestRegistryHeartbeatBlockedByPolicy(t *testing.T) {
	r := New(time.Minute, nil)
	pol, _ := NewAddressPolicy([]string{"10.0.0.0/8"})
	r.SetPolicy(pol)

	r.Heartbeat(WorkerInfo{AppID: 1, WorkerAddress: "10.1.1.1:80", Metrics: domain.SystemMetrics{}})
	r.Heartbeat(WorkerInfo{AppID: 1, WorkerAddress: "8.8.8.8:80", Metrics: domain.SystemMetrics{}})

	online := r.Online(1)
	if len(online) != 1 {
		t.Fatalf("仅白名单地址应注册, got %d", len(online))
	}
	if online[0].WorkerAddress != "10.1.1.1:80" {
		t.Fatalf("应只含白名单地址, got %+v", online[0])
	}
}
