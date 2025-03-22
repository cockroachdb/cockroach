// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package macaddr

import (
	"fmt"
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
		// Supported formats – all represent the MAC address 08:00:2b:01:02:03
		// (0x08002b010203).
		{1, "08:00:2b:01:02:03", 0x08002b010203, false},
		{2, "08-00-2b-01-02-03", 0x08002b010203, false},
		{3, "08002b:010203", 0x08002b010203, false},
		{4, "08002b-010203", 0x08002b010203, false},
		{5, "0800.2b01.0203", 0x08002b010203, false},
		{6, "0800-2b01-0203", 0x08002b010203, false},
		{7, "08002b010203", 0x08002b010203, false},

		// Invalid formats.
		{8, "0800:2b01:0203", 0, true},    // Incorrect grouping.
		{9, "not even close", 0, true},    // Completely wrong.
		{8, "-0800:2b01:020", 0, true},    // Incorrect grouping.
		{8, "-0800:2b01:02-0", 0, true},   // Incorrect separator placement.
		{8, "-0800:2b01:020---", 0, true}, // Incorrect length.
		{8, "-0800:2b01:020- ", 0, true},  // Incorrect with space.
		{1, "08-00:2b-01:02:03", 0, true}, // Mixed Seperators, not allowed.
		// Format's supported by net.ParseMAC but not Postgres.
		// Postgres’ macaddr type only supports six‐octet (48‑bit) addresses
		// unlike net.ParseMAC.
		{8, "02:00:5e:10:00:00:00:01", 0, true},
		{9, "0200.5e10.0000.0001", 0, true},
		{9, "02-00-5e-10-00-00-00-01", 0, true},
		{8, "00:00:00:00:fe:80:00:00:00:00:00:00:02:00:5e:10:00:00:00:01", 0, true},
		{9, "00-00-00-00-fe-80-00-00-00-00-00-00-02-00-5e-10-00-00-00-01", 0, true},
		{9, "0000.0000.fe80.0000.0000.0000.0200.5e10.0000.0001", 0, true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("TestCase%d", tt.id), func(t *testing.T) {
			got, err := ParseMAC(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseMAC(%q) = %x, want %x", tt.input, got, tt.want)
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

// TestMACAddrString tests the string representation of a MAC address.
func TestMACAddrString(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"colon-separated", "08:00:2b:01:02:03", "08:00:2b:01:02:03"},
		{"dash-separated", "08-00-2b-01-02-03", "08:00:2b:01:02:03"},
		{"single separator colon", "08002b:010203", "08:00:2b:01:02:03"},
		{"single separator dash", "08002b-010203", "08:00:2b:01:02:03"},
		{"grouped dot", "0800.2b01.0203", "08:00:2b:01:02:03"},
		{"grouped dash", "0800-2b01-0203", "08:00:2b:01:02:03"},
		{"no separator", "08002b010203", "08:00:2b:01:02:03"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, err := ParseMAC(tc.in)
			if err != nil {
				t.Errorf("")
			}
			got := m.String()
			if got != tc.want {
				t.Errorf("got: %s; want: %s", got, tc.want)
			}
		})
	}
}
