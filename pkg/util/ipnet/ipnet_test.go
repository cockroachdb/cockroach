// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ipnet

import (
	"net"
	"strings"
	"testing"
)

func TestIPNetParseInet(t *testing.T) {
	testCases := []struct {
		s   string
		exp *IPNet
		err string
	}{
		// Basic IPv4.
		{"192.168.1.2", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("192.168.1.2"), Mask: net.CIDRMask(32, 32)}}, ""},
		// Test we preserve masked bits.
		{"192.168.1.2/16", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("192.168.1.2"), Mask: net.CIDRMask(16, 32)}}, ""},
		// Test the ability to have following '.'.
		{"192.168.1.2.", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("192.168.1.2"), Mask: net.CIDRMask(32, 32)}}, ""},
		{"192.168.1.2./10", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("192.168.1.2"), Mask: net.CIDRMask(10, 32)}}, ""},
		// Basic IPv6.
		{"2001:4f8:3:ba::/64", &IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("2001:4f8:3:ba::"), Mask: net.CIDRMask(64, 128)}}, ""},
		{"2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128", &IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("2001:4f8:3:ba:2e0:81ff:fe22:d1f1"), Mask: net.CIDRMask(128, 128)}}, ""},
		{"::ffff:1.2.3.1/120", &IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("::ffff:1.2.3.1"), Mask: net.CIDRMask(120, 128)}}, ""},
		{"::ffff:1.2.3.1/128", &IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("::ffff:1.2.3.1"), Mask: net.CIDRMask(128, 128)}}, ""},
		{"::ffff:1.2.3.1/128", &IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("::ffff:1.2.3.1"), Mask: net.CIDRMask(128, 128)}}, ""},
		{"::ffff:1.2.3.1/20", &IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("::ffff:1.2.3.1"), Mask: net.CIDRMask(20, 128)}}, ""},
		{"::ffff:1.2.3.1/120", &IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("::ffff:1.2.3.1"), Mask: net.CIDRMask(120, 128)}}, ""},
		{"::1", &IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("::1"), Mask: net.CIDRMask(128, 128)}}, ""},

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
		{"0.0.0.0", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("0.0.0.0"), Mask: net.CIDRMask(32, 32)}}, ""},
		{"0.0.0.0/32", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("0.0.0.0"), Mask: net.CIDRMask(32, 32)}}, ""},
		{"0.0.0.0/0", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("0.0.0.0"), Mask: net.CIDRMask(0, 32)}}, ""},
		{"0.0.0.0/10", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("0.0.0.0"), Mask: net.CIDRMask(10, 32)}}, ""},

		{"255.255.255.255", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("255.255.255.255"), Mask: net.CIDRMask(32, 32)}}, ""},
		{"255.255.255.255/32", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("255.255.255.255"), Mask: net.CIDRMask(32, 32)}}, ""},
		{"255.255.255.255/0", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("255.255.255.255"), Mask: net.CIDRMask(0, 32)}}, ""},
		{"255.255.255.255/10", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("255.255.255.255"), Mask: net.CIDRMask(10, 32)}}, ""},

		{"::0", &IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("::0"), Mask: net.CIDRMask(128, 128)}}, ""},
		{"::0/0", &IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("::0"), Mask: net.CIDRMask(0, 128)}}, ""},
		{"::0/10", &IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("::0"), Mask: net.CIDRMask(10, 128)}}, ""},

		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", &IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"), Mask: net.CIDRMask(128, 128)}}, ""},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/0", &IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"), Mask: net.CIDRMask(0, 128)}}, ""},
		{"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/10", &IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"), Mask: net.CIDRMask(10, 128)}}, ""},

		// Postgres compatibility edge cases: IPv4 missing octets.
		{"192.168/24", nil, "mask is larger than provided octets"},
		{"192/10", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("192.0.0.0"), Mask: net.CIDRMask(10, 32)}}, ""},
		{"192.168/23", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("192.168.0.0"), Mask: net.CIDRMask(23, 32)}}, ""},
		{"192.168./10", &IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("192.168.0.0"), Mask: net.CIDRMask(10, 32)}}, ""},
	}
	for i, testCase := range testCases {
		var actual IPNet
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

func TestIPNetBinaryMarshalling(t *testing.T) {
	testCases := []struct {
		input *IPNet
	}{
		{&IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("192.168.1.2"), Mask: net.CIDRMask(32, 32)}}},
		{&IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("192.168.1.2"), Mask: net.CIDRMask(16, 32)}}},
		{&IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("192.168.1.2"), Mask: net.CIDRMask(32, 32)}}},
		{&IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("192.168.1.2"), Mask: net.CIDRMask(10, 32)}}},
		{&IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("2001:4f8:3:ba::"), Mask: net.CIDRMask(64, 128)}}},
		{&IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("2001:4f8:3:ba:2e0:81ff:fe22:d1f1"), Mask: net.CIDRMask(128, 128)}}},
		{&IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("::ffff:1.2.3.1"), Mask: net.CIDRMask(120, 128)}}},
		{&IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("::ffff:1.2.3.1"), Mask: net.CIDRMask(128, 128)}}},
		{&IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("::ffff:1.2.3.1"), Mask: net.CIDRMask(128, 128)}}},
		{&IPNet{Family: IPv6Family, IPNet: net.IPNet{IP: net.ParseIP("::1"), Mask: net.CIDRMask(128, 128)}}},
		{&IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("192.168.0.0"), Mask: net.CIDRMask(23, 32)}}},
		{&IPNet{Family: IPv4Family, IPNet: net.IPNet{IP: net.ParseIP("192.168.0.0"), Mask: net.CIDRMask(10, 32)}}},
	}
	for i, testCase := range testCases {
		var data []byte
		data = testCase.input.ToBuffer(data)
		var actual IPNet
		if remaining, err := actual.FromBuffer(data); err != nil {
			t.Errorf("%d: UnmarshalBinary(%s) caused an unexpected error:%s", i, testCase.input, err)
		} else if !actual.Equal(testCase.input) {
			t.Errorf("%d: Binary marshalling round trip failed. actual:%v does not match expected:%v", i, actual, testCase.input)
		} else if len(remaining) != 0 {
			t.Errorf("%d: Binary marshalling left extraneous bytes in buffer. Leftover len: %d bytes: %s", i, len(remaining), remaining)
		}
	}
}

func TestIPNetGetFamily(t *testing.T) {
	testCases := []struct {
		s   string
		exp int
	}{
		// Basic IPv4
		{"192.168.1.2", IPv4Family},
		{"192.168.1.2/16", IPv4Family},
		// Basic IPv6
		{"2001:4f8:3:ba::/64", IPv6Family},
		{"2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128", IPv6Family},
		{"::ffff:1.2.3.1/120", IPv6Family},
		{"::ffff:1.2.3.1/128", IPv6Family},
		{"::ffff:1.2.3.1/128", IPv6Family},
		{"::1", IPv6Family},

		// Postgres compatibility edge cases: IPv4 missing octets
		{"192.168/24", IPv4Family},
		{"192/10", IPv4Family},
		{"192.168/23", IPv4Family},
		{"192.168./10", IPv4Family},
	}
	for i, testCase := range testCases {
		actual := getFamily(testCase.s)

		if actual != testCase.exp {
			t.Errorf("%d: getFamily(%q) actual:%v does not match expected:%v", i, testCase.s, actual,
				testCase.exp)
		}
	}
}

func TestIPNetCompare(t *testing.T) {
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
		{"192.168.1.2", "::ffff:192.168.1.2", -1},
		{"::ffff:192.168.1.2", "192.168.1.2", 1},
		{"::ffff:192.168.1.2", "::ffff:192.168.1.2", 0},
		{"c33e:9867:5c98:f0a2:2b2:abf9:c7a5:67d", "c33e:9867:5c98:f0a2:2b2:abf9:c7a5:67d", 0},
		{"6e32:8a01:373b:c9ce:8ed5:9f7f:dc7e:5cfc", "c33e:9867:5c98:f0a2:2b2:abf9:c7a5:67d", -1},
		{"192.168.1.2", "192.168.1.2", 0},
	}
	for i, testCase := range testCases {
		var ip1 IPNet
		var ip2 IPNet
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

func TestIPNetEqual(t *testing.T) {
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
	}
	for i, testCase := range testCases {
		var ip1 IPNet
		var ip2 IPNet
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

func TestIPNetString(t *testing.T) {
	testCases := []struct {
		s   string
		exp string
	}{
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
		var ip IPNet
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
