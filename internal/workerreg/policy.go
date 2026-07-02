package workerreg

import (
	"fmt"
	"net"
	"strings"
)

// AddressPolicy worker 地址白名单策略:可选的 CIDR/IP 集合,限制可注册的 worker 地址来源。
// nil 表示不限制(默认,向后兼容)。用于 /server、/worker 无鉴权场景下的 SSRF 纵深防御——
// 即便前置网络隔离失效,非白名单网段的地址也无法注册并被服务端 POST(派发)。
type AddressPolicy struct {
	cidrs []*net.IPNet
}

// NewAddressPolicy 解析 CIDR 列表(如 ["10.0.0.0/8","192.168.0.0/16"])。
// 单个 IP(无掩码)视为 /32(IPv4)/ /128(IPv6)。全空/全空白返回 nil(不限制)。
// 任一非法条目返回 error(装配层应据此拒绝启动,避免白名单静默失效)。
func NewAddressPolicy(cidrs []string) (*AddressPolicy, error) {
	p := &AddressPolicy{}
	for _, c := range cidrs {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		_, ipnet, err := net.ParseCIDR(c)
		if err != nil {
			// 单 IP(无掩码):补 /32(IPv4) 或 /128(IPv6)
			if ip := net.ParseIP(c); ip != nil {
				mask := 32
				if ip.To4() == nil {
					mask = 128
				}
				_, ipnet, err = net.ParseCIDR(fmt.Sprintf("%s/%d", c, mask))
			}
		}
		if err != nil {
			return nil, fmt.Errorf("非法 CIDR/IP %q: %w", c, err)
		}
		p.cidrs = append(p.cidrs, ipnet)
	}
	if len(p.cidrs) == 0 {
		return nil, nil
	}
	return p, nil
}

//Allowed 地址(通常为 host:port)的 IP 是否落在任一白名单 CIDR 内。
//
//	保守拒绝:无法解析地址/IP 时返回 false。调用方在 policy 为 nil 时应直接放行(见 AllowedAddress)。
func (p *AddressPolicy) Allowed(addr string) bool {
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil && h != "" {
		host = h
	}
	host = strings.Trim(host, "[]") // IPv6 字面量 [::1] -> ::1
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, c := range p.cidrs {
		if c.Contains(ip) {
			return true
		}
	}
	return false
}
