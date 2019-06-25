// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ipaddr

import (
	"math"
	"net"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/uint128"
)

func TestIPAddrParseInet(t *testing.T) {
	testCases := []struct {
		s   string
		exp *IPAddr
		err string
	}{
		// Basic IPv4.
		{"192.168.1.2", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("192.168.1.2")))), Mask: 32}, ""},
		// Test we preserve masked bits.
		{"192.168.1.2/16", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("192.168.1.2")))), Mask: 16}, ""},
		// Test the ability to have following '.'.
		{"192.168.1.2.", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("192.168.1.2")))), Mask: 32}, ""},
		{"192.168.1.2./10", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("192.168.1.2")))), Mask: 10}, ""},
		// Basic IPv6.
		{"2001:4f8:3:ba::/64", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("2001:4f8:3:ba::")))), Mask: 64}, ""},
		{"2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("2001:4f8:3:ba:2e0:81ff:fe22:d1f1")))), Mask: 128}, ""},
		{"::ffff:1.2.3.1/120", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("::ffff:1.2.3.1")))), Mask: 120}, ""},
		{"::ffff:1.2.3.1/128", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("::ffff:1.2.3.1")))), Mask: 128}, ""},
		{"::ffff:1.2.3.1/128", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("::ffff:1.2.3.1")))), Mask: 128}, ""},
		{"::ffff:1.2.3.1/20", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("::ffff:1.2.3.1")))), Mask: 20}, ""},
		{"::ffff:1.2.3.1/120", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("::ffff:1.2.3.1")))), Mask: 120}, ""},
		{"::1", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("::1")))), Mask: 128}, ""},
		{"9ec6:78fc:c3ae:a65a:9ac7:2081:ac81:e0aa/101", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("9ec6:78fc:c3ae:a65a:9ac7:2081:ac81:e0aa")))), Mask: 101}, ""},

		// Test bad IPs.
		{"abc", nil, "invalid IP"},
		{"", nil, "invalid IP"},
		{"20000", nil, "invalid IP"},
		{"123.abc", nil, "invalid IP"},
		{"123.2000", nil, "invalid IP"},
		{"192.168", nil, "invalid IP"},
		{"192.168.0.0.0", nil, "invalid IP"},
		{"192.168.0.2/a", nil, "invalid mask"},
		{"192.168.0.2/100", nil, "invalid mask"},
		{"192.168.0.a/20", nil, "invalid IP"},

		// Edge cases.
		{"0.0.0.0", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.IPv4zero))), Mask: 32}, ""},
		{"0.0.0.0/32", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.IPv4zero))), Mask: 32}, ""},
		{"0.0.0.0/0", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.IPv4zero))), Mask: 0}, ""},
		{"0.0.0.0/10", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.IPv4zero))), Mask: 10}, ""},

		{"255.255.255.255", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.IPv4bcast))), Mask: 32}, ""},
		{"255.255.255.255/32", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.IPv4bcast))), Mask: 32}, ""},
		{"255.255.255.255/0", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.IPv4bcast))), Mask: 0}, ""},
		{"255.255.255.255/10", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.IPv4bcast))), Mask: 10}, ""},

		{"::0", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.IPv6zero))), Mask: 128}, ""},
		{"::0/0", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.IPv6zero))), Mask: 0}, ""},
		{"::0/10", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.IPv6zero))), Mask: 10}, ""},

		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")))), Mask: 128}, ""},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/0", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")))), Mask: 0}, ""},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/10", &IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")))), Mask: 10}, ""},

		// Postgres compatibility edge cases: IPv4 missing octets.
		{"192.168/24", nil, "mask is larger than provided octets"},
		{"192/10", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("192.0.0.0")))), Mask: 10}, ""},
		{"192.168/23", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("192.168.0.0")))), Mask: 23}, ""},
		{"192.168./10", &IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("192.168.0.0")))), Mask: 10}, ""},
	}
	for i, testCase := range testCases {
		var actual IPAddr
		if err := ParseINet(testCase.s, &actual); err != nil {
			if len(testCase.err) > 0 {
				if !strings.Contains(err.Error(), testCase.err) {
					t.Errorf("%d: ParseINet(%s) caused an incorrect error actual:%s, expected:%s", i, testCase.s,
						err, testCase.err)
				}
			} else {
				t.Errorf("%d: ParseINet(%s) caused an unexpected error:%s", i, testCase.s, err)
			}
		} else if testCase.exp != nil && !actual.Equal(testCase.exp) {
			t.Errorf("%d: ParseINet(%s) actual:%v does not match expected:%v", i, testCase.s, actual,
				testCase.exp)
		}
	}
}

func TestIPAddrBinaryMarshalling(t *testing.T) {
	testCases := []struct {
		input *IPAddr
	}{
		{&IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("192.168.1.2")))), Mask: 32}},
		{&IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("192.168.1.2")))), Mask: 16}},
		{&IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("192.168.1.2")))), Mask: 32}},
		{&IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("192.168.1.2")))), Mask: 10}},
		{&IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("2001:4f8:3:ba::")))), Mask: 64}},
		{&IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("2001:4f8:3:ba:2e0:81ff:fe22:d1f1")))), Mask: 128}},
		{&IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("::ffff:1.2.3.1")))), Mask: 120}},
		{&IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("::ffff:1.2.3.1")))), Mask: 128}},
		{&IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("::ffff:1.2.3.1")))), Mask: 128}},
		{&IPAddr{Family: IPv6family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("::1")))), Mask: 128}},
		{&IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("192.168.0.0")))), Mask: 23}},
		{&IPAddr{Family: IPv4family, Addr: Addr(uint128.FromBytes([]byte(net.ParseIP("192.168.0.0")))), Mask: 10}},
	}
	for i, testCase := range testCases {
		var data []byte
		data = testCase.input.ToBuffer(data)
		var actual IPAddr
		if remaining, err := actual.FromBuffer(data); err != nil {
			t.Errorf("%d: UnmarshalBinary(%s) caused an unexpected error:%s", i, testCase.input, err)
		} else if !actual.Equal(testCase.input) {
			t.Errorf("%d: Binary marshaling round trip failed. actual:%v does not match expected:%v", i, actual, testCase.input)
		} else if len(remaining) != 0 {
			t.Errorf("%d: Binary marshaling left extraneous bytes in buffer. Leftover len: %d bytes: %s", i, len(remaining), remaining)
		}
	}
}

func TestIPAddrGetFamily(t *testing.T) {
	testCases := []struct {
		s   string
		exp IPFamily
	}{
		// Basic IPv4
		{"192.168.1.2", IPv4family},
		{"192.168.1.2/16", IPv4family},
		// Basic IPv6
		{"2001:4f8:3:ba::/64", IPv6family},
		{"2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128", IPv6family},
		{"::ffff:1.2.3.1/120", IPv6family},
		{"::ffff:1.2.3.1/128", IPv6family},
		{"::ffff:1.2.3.1/128", IPv6family},
		{"::1", IPv6family},

		// Postgres compatibility edge cases: IPv4 missing octets
		{"192.168/24", IPv4family},
		{"192/10", IPv4family},
		{"192.168/23", IPv4family},
		{"192.168./10", IPv4family},
	}
	for i, testCase := range testCases {
		actual := getFamily(testCase.s)

		if actual != testCase.exp {
			t.Errorf("%d: getFamily(%q) actual:%v does not match expected:%v", i, testCase.s, actual,
				testCase.exp)
		}
	}
}

func TestIPAddrCompare(t *testing.T) {
	testCases := []struct {
		s1  string
		s2  string
		exp int
	}{
		// Basic IPv4
		{"192.168.1.2", "192.168.1.2", 0},
		{"192.168.1.2", "192.168.1.3", -1},
		{"192.168.1.2", "192.168.1.1", 1},
		{"192.168.1.2", "192.168.0.2", 1},
		{"192.168.1.2", "192.168.1.2/16", 1},
		{"192.168.1.2/17", "192.168.1.2/16", 1},
		{"192.168.1.2/17", "192.168.1.3/1", 1},
		{"192.168.1.2/1", "192.168.1.3/17", -1},
		{"192.168.1.2", "::ffff:192.168.1.2", -1},
		{"::ffff:192.168.1.2", "192.168.1.2", 1},
		{"::ffff:192.168.1.2", "::ffff:192.168.1.2", 0},
		{"c33e:9867:5c98:f0a2:2b2:abf9:c7a5:67d", "c33e:9867:5c98:f0a2:2b2:abf9:c7a5:67d", 0},
		{"6e32:8a01:373b:c9ce:8ed5:9f7f:dc7e:5cfc", "c33e:9867:5c98:f0a2:2b2:abf9:c7a5:67d", -1},
		{"192.168.1.2", "192.168.1.2", 0},
	}
	for i, testCase := range testCases {
		var ip1 IPAddr
		var ip2 IPAddr
		if err := ParseINet(testCase.s1, &ip1); err != nil {
			t.Fatalf("%d: Bad test input s1:%s", i, testCase.s1)
		}
		if err := ParseINet(testCase.s2, &ip2); err != nil {
			t.Fatalf("%d: Bad test input s2:%s", i, testCase.s2)
		}

		if actual := ip1.Compare(&ip2); actual != testCase.exp {
			t.Errorf("%d: Compare(%q, %q) actual:%v does not match expected:%v", i, testCase.s1, testCase.s2, actual,
				testCase.exp)
		}
	}
}

func TestIPAddrEqual(t *testing.T) {
	testCases := []struct {
		s1  string
		s2  string
		exp bool
	}{
		// Basic IPv4
		{"192.168.1.2", "192.168.1.2", true},
		{"192.168.1.2", "192.168.1.3", false},
		{"192.168.1.2", "192.168.1.2/10", false},
		{"192.168.1.2", "::ffff:192.168.1.2", false},
		{"192.168.1.2", "::ffff:192.168.1.2", false},
		{"0.0.0.0", "0.0.0.0", true},
		{"0.0.0.0/0", "0.0.0.0/0", true},
		{"0.0.0.0/0", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128", false},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128", true},
	}
	for i, testCase := range testCases {
		var ip1 IPAddr
		var ip2 IPAddr
		if err := ParseINet(testCase.s1, &ip1); err != nil {
			t.Fatalf("%d: Bad test input s1:%s", i, testCase.s1)
		}
		if err := ParseINet(testCase.s2, &ip2); err != nil {
			t.Fatalf("%d: Bad test input s2:%s", i, testCase.s2)
		}

		if actual := ip1.Equal(&ip2); actual != testCase.exp {
			t.Errorf("%d: Equal(%q, %q) actual:%v does not match expected:%v", i, testCase.s1, testCase.s2, actual,
				testCase.exp)
		}
	}
}

func TestIPAddrString(t *testing.T) {
	testCases := []struct {
		s   string
		exp string
	}{
		{"0.0.0.0", "0.0.0.0"},
		{"::0", "::"},
		{"255.255.255.255", "255.255.255.255"},
		{"192.168.1.2", "192.168.1.2"},
		{"192.168.1.2/10", "192.168.1.2/10"},
		// Test omission of mask
		{"192.168.1.2/32", "192.168.1.2"},
		{"::ffff/128", "::ffff"},

		// Test retention of IPv6 format if IPv4-mapped IPv6 for postgres compatibility
		{"::ffff:192.168.1.2", "::ffff:192.168.1.2"},
		{"::ffff:192.168.1.2/120", "::ffff:192.168.1.2/120"},
	}
	for i, testCase := range testCases {
		var ip IPAddr
		if err := ParseINet(testCase.s, &ip); err != nil {
			t.Fatalf("%d: Bad test input s:%s", i, testCase.s)
		}
		actual := ip.String()
		if actual != testCase.exp {
			t.Errorf("%d: String(%q) actual:%v does not match expected:%v", i, testCase.s, actual,
				testCase.exp)
		}
	}
}

func TestIPAddrBroadcast(t *testing.T) {
	testCases := []struct {
		s   string
		exp string
	}{
		// Basic IPv4
		{"192.168.1.2", "192.168.1.2"},
		{"192.168.1.2/16", "192.168.255.255/16"},
		{"192.168.1.2/10", "192.191.255.255/10"},
		{"192.0.0.0/10", "192.63.255.255/10"},
		// Basic IPv6
		{"2001:4f8:3:ba::/64", "2001:4f8:3:ba:ffff:ffff:ffff:ffff/64"},
		{"2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128", "2001:4f8:3:ba:2e0:81ff:fe22:d1f1"},
		{"::ffff:1.2.3.1/120", "::ffff:1.2.3.255/120"},
		{"::ffff:1.2.3.1/128", "::ffff:1.2.3.1"},
		{"::ffff:1.2.3.1/20", "0:fff:ffff:ffff:ffff:ffff:ffff:ffff/20"},
		{"::1", "::1"},
	}
	for i, testCase := range testCases {
		var ip IPAddr
		if err := ParseINet(testCase.s, &ip); err != nil {
			t.Fatalf("%d: bad test case: %s got error %s", i, testCase.s, err)
		}
		actual := ip.Broadcast()
		if actual.String() != testCase.exp {
			t.Errorf("%d: Broadcast(%s) actual:%s does not match expected:%s", i, testCase.s, actual.String(),
				testCase.exp)
		}
	}
}

func TestIPAddrHostmask(t *testing.T) {
	testCases := []struct {
		s   string
		exp string
	}{
		// Basic IPv4
		{"192.168.1.2", "0.0.0.0"},
		{"192.168.1.2/16", "0.0.255.255"},
		{"192.168.1.2/10", "0.63.255.255"},
		{"192.168.1.2/0", "255.255.255.255"},
		// Basic IPv6
		{"2001:4f8:3:ba::/64", "::ffff:ffff:ffff:ffff"},
		{"2001:4f8:3:ba::/0", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},
		{"2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128", "::"},
		{"::ffff:1.2.3.1/120", "::ff"},
		{"::ffff:1.2.3.1/128", "::"},
		{"::ffff:1.2.3.1/20", "0:fff:ffff:ffff:ffff:ffff:ffff:ffff"},
	}
	for i, testCase := range testCases {
		var ip IPAddr
		if err := ParseINet(testCase.s, &ip); err != nil {
			t.Fatalf("%d: bad test case: %s got error %s", i, testCase.s, err)
		}

		var expIP IPAddr
		if err := ParseINet(testCase.exp, &expIP); err != nil {
			t.Fatalf("%d: bad test case: %s got error %s", i, testCase.exp, err)
		}

		actual := ip.Hostmask()
		if actual.String() != testCase.exp {
			t.Errorf("%d: Hostmask(%s) actual:%#v does not match expected:%#v", i, testCase.s, actual,
				expIP)
		}
	}
}

func TestIPAddrNetmask(t *testing.T) {
	testCases := []struct {
		s   string
		exp string
	}{
		// Basic IPv4
		{"192.168.1.2", "255.255.255.255"},
		{"192.168.1.2/16", "255.255.0.0"},
		{"192.168.1.2/10", "255.192.0.0"},
		{"192.168.1.2/0", "0.0.0.0"},
		// Basic IPv6
		{"2001:4f8:3:ba::/64", "ffff:ffff:ffff:ffff::"},
		{"2001:4f8:3:ba::/0", "::"},
		{"2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},
		{"::ffff:1.2.3.1/120", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ff00"},
		{"::ffff:1.2.3.1/20", "ffff:f000::"},
	}
	for i, testCase := range testCases {
		var ip IPAddr
		if err := ParseINet(testCase.s, &ip); err != nil {
			t.Fatalf("%d: bad test case: %s got error %s", i, testCase.s, err)
		}

		actual := ip.Netmask()
		if actual.String() != testCase.exp {
			t.Errorf("%d: Netmask(%s) actual:%s does not match expected:%s", i, testCase.s, actual,
				testCase.exp)
		}
	}
}

func TestComplement(t *testing.T) {
	testCases := []struct {
		s   string
		exp string
	}{
		{"192.168.1.2", "63.87.254.253"},
		{"192.168.1.2/16", "63.87.254.253/16"},
		{"192.168.1.2/10", "63.87.254.253/10"},
		{"192.168.1.2/0", "63.87.254.253/0"},
		// Basic IPv6
		{"2001:4f8:3:ba::/64", "dffe:fb07:fffc:ff45:ffff:ffff:ffff:ffff/64"},
		{"2001:4f8:3:ba::/0", "dffe:fb07:fffc:ff45:ffff:ffff:ffff:ffff/0"},
		{"2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128", "dffe:fb07:fffc:ff45:fd1f:7e00:1dd:2e0e"},
		{"::ffff:1.2.3.1/120", "ffff:ffff:ffff:ffff:ffff:0:fefd:fcfe/120"},
		{"::ffff:1.2.3.1/20", "ffff:ffff:ffff:ffff:ffff:0:fefd:fcfe/20"},
	}
	for i, testCase := range testCases {
		var ip IPAddr
		if err := ParseINet(testCase.s, &ip); err != nil {
			t.Fatalf("%d: bad test case: %s got error %s", i, testCase.s, err)
		}

		actual := ip.Complement()
		if actual.String() != testCase.exp {
			t.Errorf("%d: Complement(%s) actual:%s does not match expected:%s", i, testCase.s, actual,
				testCase.exp)
		}
	}
}

func TestAnd(t *testing.T) {
	testCases := []struct {
		s1  string
		s2  string
		exp string
	}{
		{"192.168.1.2", "192.168.1.2", "192.168.1.2"},
		{"255.255.255.250", "0.0.0.5", "0.0.0.0"},
		{"255.255.255.250/2", "0.5.0.5/17", "0.5.0.0/17"},
		{"192.168.1.2/17", "192.168.1.2/16", "192.168.1.2/17"},
		{"192.168.1.2/17", "192.168.1.3/1", "192.168.1.2/17"},
		{"192.168.1.2/1", "192.168.1.3/17", "192.168.1.2/17"},
		{"::ffff:192.168.1.2", "::ffff:192.168.1.2", "::ffff:192.168.1.2"},
		{"0000:0564:0000:0aab:0000:0000:0060:0005", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:0005", "0:564:0:aab::60:5"},
		{"c33e:9867:5c98:f0a2:2b2:abf9:c7a5:67d/99", "c33e:9867:5c98:f0a2:2b2:abf9:c7a5:67d", "c33e:9867:5c98:f0a2:2b2:abf9:c7a5:67d"},
		{"6e32:8a01:373b:c9ce:8ed5:9f7f:dc7e:5cfc", "c33e:9867:5c98:f0a2:2b2:abf9:c7a5:67d", "4232:8801:1418:c082:290:8b79:c424:47c"},
		{"8000:0000:0000:0000:0000:0000:0000:0005/68", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:0005/7", "8000::5/68"},
	}
	for i, testCase := range testCases {
		var ip1, ip2, exp IPAddr
		if err := ParseINet(testCase.s1, &ip1); err != nil {
			t.Fatalf("%d: Bad test input s1:%s", i, testCase.s1)
		}
		if err := ParseINet(testCase.s2, &ip2); err != nil {
			t.Fatalf("%d: Bad test input s2:%s", i, testCase.s2)
		}
		if err := ParseINet(testCase.exp, &exp); err != nil {
			t.Fatalf("%d: Bad test input exp:%s", i, testCase.exp)
		}

		actual, err := ip1.And(&ip2)
		if err != nil {
			t.Fatalf("%d: Bad test input s1:%s AND s2:%s", i, testCase.s1, testCase.s2)
		}
		if !actual.Equal(&exp) {
			t.Errorf("%d: And(%q, %q) actual:%v does not match expected:%v", i, testCase.s1, testCase.s2, actual,
				testCase.exp)
		}
	}
}

func TestOr(t *testing.T) {
	testCases := []struct {
		s1  string
		s2  string
		exp string
	}{
		{"192.168.1.2", "192.168.1.2", "192.168.1.2"},
		{"255.255.255.250", "0.0.0.5", "255.255.255.255"},
		{"255.255.255.250/2", "0.5.0.5/17", "255.255.255.255/17"},
		{"192.168.1.2/17", "192.168.1.2/16", "192.168.1.2/17"},
		{"192.168.1.2/17", "192.168.1.3/1", "192.168.1.3/17"},
		{"192.168.1.2/1", "192.168.1.3/17", "192.168.1.3/17"},
		{"::ffff:192.168.1.2", "::ffff:192.168.1.2", "::ffff:192.168.1.2"},
		{"0000:0564:0000:0aab:0000:0000:0060:0005", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:0005", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:5"},
		{"c33e:9867:5c98:f0a2:2b2:abf9:c7a5:67d/99", "c33e:9867:5c98:f0a2:2b2:abf9:c7a5:67d", "c33e:9867:5c98:f0a2:2b2:abf9:c7a5:67d"},
		{"6e32:8a01:373b:c9ce:8ed5:9f7f:dc7e:5cfc", "c33e:9867:5c98:f0a2:2b2:abf9:c7a5:67d", "ef3e:9a67:7fbb:f9ee:8ef7:bfff:dfff:5efd"},
		{"8000:0000:0000:0000:0000:0000:0000:0005/68", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:0005/7", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:5/68"},
	}
	for i, testCase := range testCases {
		var ip1, ip2, exp IPAddr
		if err := ParseINet(testCase.s1, &ip1); err != nil {
			t.Fatalf("%d: Bad test input s1:%s", i, testCase.s1)
		}
		if err := ParseINet(testCase.s2, &ip2); err != nil {
			t.Fatalf("%d: Bad test input s2:%s", i, testCase.s2)
		}
		if err := ParseINet(testCase.exp, &exp); err != nil {
			t.Fatalf("%d: Bad test input exp:%s", i, testCase.exp)
		}

		actual, err := ip1.Or(&ip2)
		if err != nil {
			t.Fatalf("%d: Bad test input s1:%s AND s2:%s", i, testCase.s1, testCase.s2)
		}
		if !actual.Equal(&exp) {
			t.Errorf("%d: Or(%q, %q) actual:%v does not match expected:%v", i, testCase.s1, testCase.s2, actual,
				testCase.exp)
		}
	}
}

func TestAdd(t *testing.T) {
	testCases := []struct {
		s   string
		i   int64
		exp string
	}{
		{"0.0.0.0", 4294967295, "255.255.255.255"},
		{"255.255.255.255", -4294967295, "0.0.0.0"},
		{"192.168.1.2", 184836468, "203.172.98.118"},
		{"192.168.1.2/17", 184836468, "203.172.98.118/17"},
		{"::ffff:192.168.1.2", 4684658846864486648, "::4104:4066:5de7:b1fa"},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", -9223372036854775807, "ffff:ffff:ffff:ffff:8000::"},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", -9223372036854775808, "ffff:ffff:ffff:ffff:7fff:ffff:ffff:ffff"},
		{"ffff:ffff:ffff:ffff:8000::/99", 9223372036854775807, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/99"},
		{"0:0:0:0000:8000:0:0:0", math.MinInt64, "::"},
		{"8000:0000:0000:0000:0000:0000:0000:0005/68", -5, "8000::/68"},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:0005/7", 10, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:000f/7"},
		{"::5/68", -5, "::/68"},
		{"::5/68", 5, "::a/68"},
	}
	for i, testCase := range testCases {
		var ip1, exp IPAddr
		if err := ParseINet(testCase.s, &ip1); err != nil {
			t.Fatalf("%d: Bad test input s:%s", i, testCase.s)
		}
		if err := ParseINet(testCase.exp, &exp); err != nil {
			t.Fatalf("%d: Bad test input exp:%s", i, testCase.exp)
		}

		actual, err := ip1.Add(testCase.i)
		if err != nil {
			t.Fatalf("%d: Bad test input s:%s + i:%d", i, testCase.s, testCase.i)
		}
		if !actual.Equal(&exp) {
			t.Errorf("%d: %q + %d actual:%v does not match expected:%v", i, testCase.s, testCase.i, actual,
				testCase.exp)
		}
	}

	errTestCases := []struct {
		s string
		i int64
	}{
		{"0.0.0.0", 4294967296},
		{"255.255.255.255", -4294967296},
		{"192.168.1.2", 1848364646486484868},
		{"192.168.1.2/17", -18483646544568},
		{"::ffff:192.168.1.2", -4684658846864486648},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 9223372036854775807},
		{"ffff:ffff:ffff:ffff:8000:0000:0000:0001/99", 9223372036854775807},
		{"0:0:0:0:7fff:ffff:ffff:ffff", -9223372036854775808},
		{"0:0:0:0:7fff:ffff:ffff:ffff/48", -9223372036854775808},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 1},
		{"0:0:0:0000:0800:0:0:0", math.MinInt64},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:0005/7", 4484545610},
		{"::5/68", -6},
	}
	for i, errTestCase := range errTestCases {
		var ip1 IPAddr
		if err := ParseINet(errTestCase.s, &ip1); err != nil {
			t.Fatalf("%d: Bad test input s:%s", i, errTestCase.s)
		}

		actual, err1 := ip1.Add(errTestCase.i)
		if err1 == nil {
			t.Fatalf("%d: s:%s + i:%d should return Error, but return valid INet value: %v", i, errTestCase.s, errTestCase.i, actual)
		}
	}
}

func TestSub(t *testing.T) {
	testCases := []struct {
		s   string
		i   int64
		exp string
	}{
		{"0.0.0.0", -4294967295, "255.255.255.255"},
		{"255.255.255.255", 4294967295, "0.0.0.0"},
		{"192.168.1.2", -184836468, "203.172.98.118"},
		{"203.172.98.118/30", 184836468, "192.168.1.2/30"},
		{"::4104:4066:5de7:b1fa", 4684658846864486648, "::ffff:192.168.1.2"},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 9223372036854775807, "ffff:ffff:ffff:ffff:8000::"},
		{"ffff:ffff:ffff:ffff:7fff:ffff:ffff:ffff", -9223372036854775808, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},
		{"ffff:ffff:ffff:ffff:8000::/1", -9223372036854775807, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/1"},
		{"0:0:0:0000:8000:0:0:0", math.MinInt64, "0:0:0:0001::"},
		{"8000:0000:0000:0000:0000:0000:0000:0005/68", 5, "8000::/68"},
		{"::5/128", 5, "::"},
		{"::5/0", -5, "::a/0"},
	}
	for i, testCase := range testCases {
		var ip1, exp IPAddr
		if err := ParseINet(testCase.s, &ip1); err != nil {
			t.Fatalf("%d: Bad test input s:%s", i, testCase.s)
		}
		if err := ParseINet(testCase.exp, &exp); err != nil {
			t.Fatalf("%d: Bad test input exp:%s", i, testCase.exp)
		}

		actual, err := ip1.Sub(testCase.i)
		if err != nil {
			t.Fatalf("%d: Bad test input s:%s + i:%d", i, testCase.s, testCase.i)
		}
		if !actual.Equal(&exp) {
			t.Errorf("%d: %q - %d actual:%v does not match expected:%v", i, testCase.s, testCase.i, actual,
				testCase.exp)
		}
	}
}

func TestContains(t *testing.T) {
	testCases := []struct {
		s1  string
		s2  string
		exp bool
	}{
		{"192.168.162.1", "192.168.162.1", false},
		{"192.168.162.1/20", "192.168.162.1", true},

		{"192.168.200.95", "192.168.162.1", false},
		{"192.168.200.95/8", "192.168.162.1", true},
		{"192.168.200.95/16", "192.168.162.1", true},
		{"192.168.200.95/17", "192.168.162.1", true},
		{"192.168.200.95/18", "192.168.162.1", false},
		{"192.168.200.95/24", "192.168.162.1", false},
		{"192.168.200.95/32", "192.168.162.1", false},

		{"192.168.200.95", "192.168.2.1/24", false},
		{"192.168.200.95/8", "192.168.2.1/24", true},
		{"192.168.200.95/16", "192.168.2.1/24", true},
		{"192.168.200.95/17", "192.168.2.1/24", false},
		{"192.168.200.95/18", "192.168.2.1/24", false},
		{"192.168.200.95/24", "192.168.2.1/24", false},
		{"192.168.200.95/32", "192.168.2.1/24", false},

		{"192.168.200.95", "192.168.2.1/8", false},
		{"192.168.200.95/8", "192.168.2.1/8", false},
		{"192.168.200.95/16", "192.168.2.1/8", false},
		{"192.168.200.95/17", "192.168.2.1/8", false},
		{"192.168.200.95/18", "192.168.2.1/8", false},
		{"192.168.200.95/24", "192.168.2.1/8", false},
		{"192.168.200.95/32", "192.168.2.1/8", false},
		{"2001:0db8:0500:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0000:0000:0000:0001/100", false},
		{"2001:0db8:0000:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0000:0000:0000:0001/100", true},
		{"2001:0db8:0000:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0500:5000:0000:0001/50", false},
	}
	for i, testCase := range testCases {
		var ip1, ip2 IPAddr
		if err := ParseINet(testCase.s1, &ip1); err != nil {
			t.Fatalf("%d: Bad test input s1:%s", i, testCase.s1)
		}
		if err := ParseINet(testCase.s2, &ip2); err != nil {
			t.Fatalf("%d: Bad test input s2:%s", i, testCase.s2)
		}

		actual := ip1.Contains(&ip2)

		if actual != testCase.exp {
			t.Errorf("%d: %q >> %q actual:%v does not match expected:%v", i, testCase.s1, testCase.s2, actual,
				testCase.exp)
		}
	}
}

func TestContainsOrEquals(t *testing.T) {
	testCases := []struct {
		s1  string
		s2  string
		exp bool
	}{
		{"192.168.162.1", "192.168.162.1", true},
		{"192.168.162.1/20", "192.168.162.1", true},

		{"192.168.200.95", "192.168.162.1", false},
		{"192.168.200.95/8", "192.168.162.1", true},
		{"192.168.200.95/16", "192.168.162.1", true},
		{"192.168.200.95/17", "192.168.162.1", true},
		{"192.168.200.95/18", "192.168.162.1", false},
		{"192.168.200.95/24", "192.168.162.1", false},
		{"192.168.200.95/32", "192.168.162.1", false},

		{"192.168.200.95", "192.168.2.1/24", false},
		{"192.168.200.95/8", "192.168.2.1/24", true},
		{"192.168.200.95/16", "192.168.2.1/24", true},
		{"192.168.200.95/17", "192.168.2.1/24", false},
		{"192.168.200.95/18", "192.168.2.1/24", false},
		{"192.168.200.95/24", "192.168.2.1/24", false},
		{"192.168.200.95/32", "192.168.2.1/24", false},

		{"192.168.200.95", "192.168.2.1/8", false},
		{"192.168.200.95/8", "192.168.2.1/8", true},
		{"192.168.200.95/16", "192.168.2.1/8", false},
		{"192.168.200.95/17", "192.168.2.1/8", false},
		{"192.168.200.95/18", "192.168.2.1/8", false},
		{"192.168.200.95/24", "192.168.2.1/8", false},
		{"192.168.200.95/32", "192.168.2.1/8", false},
		{"2001:0db8:0500:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0000:0000:0000:0001/100", false},
		{"2001:0db8:0000:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0000:0000:0000:0001/100", true},
		{"2001:0db8:0000:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0500:5000:0000:0001/50", true},
	}
	for i, testCase := range testCases {
		var ip1, ip2 IPAddr
		if err := ParseINet(testCase.s1, &ip1); err != nil {
			t.Fatalf("%d: Bad test input s1:%s", i, testCase.s1)
		}
		if err := ParseINet(testCase.s2, &ip2); err != nil {
			t.Fatalf("%d: Bad test input s2:%s", i, testCase.s2)
		}

		actual := ip1.ContainsOrEquals(&ip2)

		if actual != testCase.exp {
			t.Errorf("%d: %q >>= %q actual:%v does not match expected:%v", i, testCase.s1, testCase.s2, actual,
				testCase.exp)
		}
	}
}

func TestContainedBy(t *testing.T) {
	testCases := []struct {
		s1  string
		s2  string
		exp bool
	}{
		{"192.168.162.1", "192.168.162.1", false},
		{"192.168.162.1/20", "192.168.162.1", false},

		{"192.168.200.95", "192.168.162.1", false},
		{"192.168.200.95/8", "192.168.162.1", false},
		{"192.168.200.95/16", "192.168.162.1", false},
		{"192.168.200.95/17", "192.168.162.1", false},
		{"192.168.200.95/18", "192.168.162.1", false},
		{"192.168.200.95/24", "192.168.162.1", false},
		{"192.168.200.95/32", "192.168.162.1", false},

		{"192.168.200.95", "192.168.2.1/24", false},
		{"192.168.200.95/8", "192.168.2.1/24", false},
		{"192.168.200.95/16", "192.168.2.1/24", false},
		{"192.168.200.95/17", "192.168.2.1/24", false},
		{"192.168.200.95/18", "192.168.2.1/24", false},
		{"192.168.200.95/24", "192.168.2.1/24", false},
		{"192.168.200.95/32", "192.168.2.1/24", false},

		{"192.168.200.95", "192.168.2.1/8", true},
		{"192.168.200.95/8", "192.168.2.1/8", false},
		{"192.168.200.95/16", "192.168.2.1/8", true},
		{"192.168.200.95/17", "192.168.2.1/8", true},
		{"192.168.200.95/18", "192.168.2.1/8", true},
		{"192.168.200.95/24", "192.168.2.1/8", true},
		{"192.168.200.95/32", "192.168.2.1/8", true},
		{"2001:0db8:0500:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0000:0000:0000:0001/100", false},
		{"2001:0db8:0000:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0000:0000:0000:0001/100", false},
		{"2001:0db8:0000:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0500:5000:0000:0001/50", false},
	}
	for i, testCase := range testCases {
		var ip1, ip2 IPAddr
		if err := ParseINet(testCase.s1, &ip1); err != nil {
			t.Fatalf("%d: Bad test input s1:%s", i, testCase.s1)
		}
		if err := ParseINet(testCase.s2, &ip2); err != nil {
			t.Fatalf("%d: Bad test input s2:%s", i, testCase.s2)
		}

		actual := ip1.ContainedBy(&ip2)

		if actual != testCase.exp {
			t.Errorf("%d: %q << %q actual:%v does not match expected:%v", i, testCase.s1, testCase.s2, actual,
				testCase.exp)
		}
	}
}

func TestContainedByOrEquals(t *testing.T) {
	testCases := []struct {
		s1  string
		s2  string
		exp bool
	}{
		{"192.168.162.1", "192.168.162.1", true},
		{"192.168.162.1/20", "192.168.162.1", false},

		{"192.168.200.95", "192.168.162.1", false},
		{"192.168.200.95/8", "192.168.162.1", false},
		{"192.168.200.95/16", "192.168.162.1", false},
		{"192.168.200.95/17", "192.168.162.1", false},
		{"192.168.200.95/18", "192.168.162.1", false},
		{"192.168.200.95/24", "192.168.162.1", false},
		{"192.168.200.95/32", "192.168.162.1", false},

		{"192.168.200.95", "192.168.2.1/24", false},
		{"192.168.200.95/8", "192.168.2.1/24", false},
		{"192.168.200.95/16", "192.168.2.1/24", false},
		{"192.168.200.95/17", "192.168.2.1/24", false},
		{"192.168.200.95/18", "192.168.2.1/24", false},
		{"192.168.200.95/24", "192.168.2.1/24", false},
		{"192.168.200.95/32", "192.168.2.1/24", false},

		{"192.168.200.95", "192.168.2.1/8", true},
		{"192.168.200.95/8", "192.168.2.1/8", true},
		{"192.168.200.95/16", "192.168.2.1/8", true},
		{"192.168.200.95/17", "192.168.2.1/8", true},
		{"192.168.200.95/18", "192.168.2.1/8", true},
		{"192.168.200.95/24", "192.168.2.1/8", true},
		{"192.168.200.95/32", "192.168.2.1/8", true},
		{"2001:0db8:0500:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0000:0000:0000:0001/100", false},
		{"2001:0db8:0000:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0000:0000:0000:0001/100", false},
		{"2001:0db8:0000:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0500:5000:0000:0001/50", true},
	}
	for i, testCase := range testCases {
		var ip1, ip2 IPAddr
		if err := ParseINet(testCase.s1, &ip1); err != nil {
			t.Fatalf("%d: Bad test input s1:%s", i, testCase.s1)
		}
		if err := ParseINet(testCase.s2, &ip2); err != nil {
			t.Fatalf("%d: Bad test input s2:%s", i, testCase.s2)
		}

		actual := ip1.ContainedByOrEquals(&ip2)

		if actual != testCase.exp {
			t.Errorf("%d: %q <<= %q actual:%v does not match expected:%v", i, testCase.s1, testCase.s2, actual,
				testCase.exp)
		}
	}
}

func TestContainsOrContainedBy(t *testing.T) {
	testCases := []struct {
		s1  string
		s2  string
		exp bool
	}{
		{"192.168.162.1", "192.168.162.1", true},
		{"192.168.162.1/20", "192.168.162.1", true},

		{"192.168.200.95", "192.168.162.1", false},
		{"192.168.200.95/8", "192.168.162.1", true},
		{"192.168.200.95/16", "192.168.162.1", true},
		{"192.168.200.95/17", "192.168.162.1", true},
		{"192.168.200.95/18", "192.168.162.1", false},
		{"192.168.200.95/24", "192.168.162.1", false},
		{"192.168.200.95/32", "192.168.162.1", false},

		{"192.168.200.95", "192.168.2.1/24", false},
		{"192.168.200.95/8", "192.168.2.1/24", true},
		{"192.168.200.95/16", "192.168.2.1/24", true},
		{"192.168.200.95/17", "192.168.2.1/24", false},
		{"192.168.200.95/18", "192.168.2.1/24", false},
		{"192.168.200.95/24", "192.168.2.1/24", false},
		{"192.168.200.95/32", "192.168.2.1/24", false},

		{"192.168.200.95", "192.168.2.1/8", true},
		{"192.168.200.95/8", "192.168.2.1/8", true},
		{"192.168.200.95/16", "192.168.2.1/8", true},
		{"192.168.200.95/17", "192.168.2.1/8", true},
		{"192.168.200.95/18", "192.168.2.1/8", true},
		{"192.168.200.95/24", "192.168.2.1/8", true},
		{"192.168.200.95/32", "192.168.2.1/8", true},
		{"192.168.200.95/32", "2001:0db8:0000:0000:0000:0000:0000:0001/100", false},
		{"2001:0db8:0500:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0000:0000:0000:0001/100", false},
		{"2001:0db8:0000:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0000:0000:0000:0001/100", true},
		{"2001:0db8:0000:0000:0500:5000:0000:0001/50", "2001:0db8:0000:0000:0500:5000:0000:0001/50", true},
	}
	for i, testCase := range testCases {
		var ip1, ip2 IPAddr
		if err := ParseINet(testCase.s1, &ip1); err != nil {
			t.Fatalf("%d: Bad test input s1:%s", i, testCase.s1)
		}
		if err := ParseINet(testCase.s2, &ip2); err != nil {
			t.Fatalf("%d: Bad test input s2:%s", i, testCase.s2)
		}

		actual := ip1.ContainsOrContainedBy(&ip2)

		if actual != testCase.exp {
			t.Errorf("%d: %q && %q actual:%v does not match expected:%v", i, testCase.s1, testCase.s2, actual,
				testCase.exp)
		}
	}
}

func TestSubIPAddr(t *testing.T) {
	testCases := []struct {
		s1  string
		s2  string
		exp int64
	}{
		{"0.0.0.0", "255.255.255.255", -4294967295},
		{"255.255.255.255", "0.0.0.0", 4294967295},
		{"192.168.1.2", "203.172.98.118", -184836468},
		{"203.172.98.118/17", "192.168.1.2/1", 184836468},
		{"::4104:4066:5de7:b1fa", "::ffff:192.168.1.2/44", 4684658846864486648},
		{"ffff:ffff:ffff:ffff:8000::/7", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", -9223372036854775807},
		{"ffff:ffff:ffff:ffff:7fff:ffff:ffff:ffff", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", -9223372036854775808},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/91", "ffff:ffff:ffff:ffff:8000::/99", 9223372036854775807},
		{"::", "0:0:0:0000:8000:0:0:0", math.MinInt64},
		{"8000::/68", "8000:0000:0000:0000:0000:0000:0000:0005/68", -5},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:000f/57", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:0005/7", 10},
		{"::/68", "::5/68", -5},
		{"::a/68", "::5/68", 5},
	}
	for i, testCase := range testCases {
		var ip1, ip2 IPAddr
		if err := ParseINet(testCase.s1, &ip1); err != nil {
			t.Fatalf("%d: Bad test input s:%s", i, testCase.s1)
		}
		if err := ParseINet(testCase.s2, &ip2); err != nil {
			t.Fatalf("%d: Bad test input exp:%s", i, testCase.s2)
		}

		actual, err := ip1.SubIPAddr(&ip2)
		if err != nil {
			t.Fatalf("%d: Bad test input s1: %s - s2: %s", i, testCase.s1, testCase.s2)
		}
		if actual != testCase.exp {
			t.Errorf("%d: %s - %s actual:%v does not match expected:%v", i, testCase.s1, testCase.s2, actual,
				testCase.exp)
		}
	}

	errTestCases := []struct {
		s1 string
		s2 string
	}{
		{"2323:5475:9894:8745:4104:4066:5de7:b1fa", "::ffff:192.168.1.2"},
		{"ffff:ffff:ffff:ffff:7fff:ffff:ffff:fffe", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/99"},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/99", "ffff:ffff:ffff:ffff:7fff:ffff:ffff:ffff/91"},
		{"::", "0:0:0:0000:8000:0:0:0001"},
		{"0:0:0:0000:8000:0:0:0000", "::"},
		{"192.168.1.2/5", "::"},
		{"0:0:0:0000:8000:0:0:0000/4", "192.168.1.2"},
		{"10:0:0:0000:8000:0:0:0000", "10::"},
		{"10::", "10:0:0:0000:8000:0:0:0001"},
	}
	for i, errTestCase := range errTestCases {
		var ip1, ip2 IPAddr
		if err := ParseINet(errTestCase.s1, &ip1); err != nil {
			t.Fatalf("%d: Bad test input s:%s", i, errTestCase.s1)
		}
		if err := ParseINet(errTestCase.s2, &ip2); err != nil {
			t.Fatalf("%d: Bad test input s:%s", i, errTestCase.s2)
		}

		actual, err1 := ip1.SubIPAddr(&ip2)
		if err1 == nil {
			t.Fatalf("%d: s:%s - s:%s should return Error, but returns int64:%v", i, errTestCase.s1, errTestCase.s2, actual)
		}
	}
}
