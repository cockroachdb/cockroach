// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package macaddr

import (
	"fmt"
	"net"
	"testing"
)

// TestParseMAC verifies that ParseMAC correctly parses various
// MAC address formats.
func TestParseMAC(t *testing.T) {
	tests := []struct {
		id      int
		input   string
		want    MACAddr
		wantErr bool
	}{
		// Valid
		{1, "08:00:2b:01:02:03", 0x08002b010203, false},
		{2, "08-00-2b-01-02-03", 0x08002b010203, false},
		{3, "08002b:010203", 0x08002b010203, false},
		{4, "08002b-010203", 0x08002b010203, false},
		{5, "0800.2b01.0203", 0x08002b010203, false},
		{6, "0800-2b01-0203", 0x08002b010203, false},
		{7, "08002b010203", 0x08002b010203, false},

		// Special valid variants supported by Postgres but
		// not included in Postgres documentation
		{8, "  08-00-2b-01-02-13 ", 0x08002b010213, false},
		{9, "08002b01020", 0x08002b010200, false},
		{10, "00-00-00-00", 0x000000000000, false},
		{11, "0:1:2:3:4:5", 0x000102030405, false},
		{12, "00-00-00-0", 0x000000000000, false},
		{13, "0-00-00-0", 0x000000000000, false},
		{14, "08:00:2b:01:02:03f", 0x08002b01023f, false},
		{15, "08:000:002b:0001:002:03f", 0x08002b01023f, false},
		{16, "00-00-00-0", 0x0, false},

		// Invalid formats
		{17, "0800:2b01:0203", 0, true},
		{18, "not even close", 0, true},
		{19, "-0800:2b01:020", 0, true},
		{20, "-0800:2b01:02-0", 0, true},
		{21, "-0800:2b01:020---", 0, true},
		{22, "-0800:2b01:020- ", 0, true},
		{23, "08-00:2b-01:02:03", 0, true}, // mixed separators
		{24, "02:00:5e:10:00:00:00:01", 0, true},
		{25, "0200.5e10.0000.0001", 0, true},
		{26, "02-00-5e-10-00-00-00-01", 0, true},
		{27, "0\t-0\r-5e-10-00-00-00-01", 0, true},
		{28, "00:00:00:00:fe:80:00:00:00:00:00:00:02:00:5e:10:00:00:00:01", 0, true},
		{29, "00-00-00-00-fe-80-00-00-00-00-00-00-02-00-5e-10-00-00-00-01", 0, true},
		{30, "0000.0000.fe80.0000.0000.0000.0200.5e10.0000.0001", 0, true},
		{31, "000000 -000000", 0, true},
		{32, " 000000 -000000", 0, true},
		{33, "0-1-2-3-4", 0, true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("TestCase%d", tt.id), func(t *testing.T) {
			got, err := parseMAC(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseMAC(%q) = %s, want %d error %s", tt.input, got, tt.want, err)
			}
		})
	}
}

// TestMACAddrNot checks the invert operation on a MAC address.
func TestMACAddrNot(t *testing.T) {
	tests := []struct {
		name string
		mac  MACAddr
	}{
		{"invert MAC 08:00:2b:01:02:03", MACAddr(0x08002b010203)},
		{"invert MAC 12:34:56:78:9a:bc", MACAddr(0x123456789abc)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MACAddrNot(tc.mac)
			want := MACAddr(^uint64(tc.mac))
			if got != want {
				t.Errorf("MacAddrNot(%x) = %x; want %x", tc.mac, got, want)
			}
			// Additionally, a & ~a should be zero.
			if MACAddrAnd(tc.mac, got) != 0 {
				t.Errorf("expected %x & ~%x == 0", tc.mac, tc.mac)
			}
			// And a | ~a should be all ones.
			if MACAddrOr(tc.mac, got) != MACAddr(^uint64(0)) {
				t.Errorf("expected %x | ~%x == 0xFFFFFFFFFFFFFFFF", tc.mac, tc.mac)
			}
		})
	}
}

// TestMACAddrAnd tests the bitwise AND operation on a MAC address.
func TestMacAddrAnd(t *testing.T) {
	tests := []struct {
		name string
		a, b MACAddr
		want MACAddr
	}{
		{"AND with all ones", MACAddr(0x08002b010203), MACAddr(0xFFFFFFFFFFFF), MACAddr(0x08002b010203)},
		{"AND non-trivial", MACAddr(0x123456789abc), MACAddr(0x0f0f0f0f0f0f), MACAddr(uint64(0x123456789abc) & uint64(0x0f0f0f0f0f0f))},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MACAddrAnd(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("MacAddrAnd(%x, %x) = %x; want %x", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// TestMACAddrOr tests the bitwise OR operation on a MAC address.
func TestMACAddrOr(t *testing.T) {
	tests := []struct {
		name string
		a, b MACAddr
		want MACAddr
	}{
		{"OR with all ones", MACAddr(0x08002b010203), MACAddr(0xFFFFFFFFFFFF), MACAddr(0xFFFFFFFFFFFF)},
		{"OR non-trivial", MACAddr(0x123456789abc), MACAddr(0x0f0f0f0f0f0f), MACAddr(uint64(0x123456789abc) | uint64(0x0f0f0f0f0f0f))},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MACAddrOr(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("MacAddrOr(%x, %x) = %x; want %x", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func BenchmarkParseMAC(b *testing.B) {

	var benchInputs = []string{
		"08:00:2b:01:02:03",
		"0800.2b01.0203",
	}

	b.Run("Custom", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, s := range benchInputs {
				if _, err := ParseMAC(s); err != nil {
					b.Fatalf("ParseMAC(%q) error: %v", s, err)
				}
			}
		}
	})
	b.Run("Stdlib", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, s := range benchInputs {
				if _, err := net.ParseMAC(s); err != nil {
					b.Fatalf("net.ParseMAC(%q) error: %v", s, err)
				}
			}
		}
	})
}
