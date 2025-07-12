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

// TestParseMAC verifies that ParseMAC correctly parses various MAC address
// formats.
func TestParseMAC(t *testing.T) {
	tests := []struct {
		input   string
		want    MACAddr
		wantErr bool
	}{
		// Valid
		{"08:00:2b:01:02:03", 0x08002b010203, false},
		{"08-00-2b-01-02-03", 0x08002b010203, false},
		{"08002b:010203", 0x08002b010203, false},
		{"08002b-010203", 0x08002b010203, false},
		{"0800.2b01.0203", 0x08002b010203, false},
		{"0800-2b01-0203", 0x08002b010203, false},
		{"08002b010203", 0x08002b010203, false},
		{"fffff ff   fff", 0xffffffffff0f, false},
		{"ff:ff:ff:ff: ff:f", 0xffffffffff0f, false},
		{"fffff ff   fff", 0xffff0fffff0f, false},
		{"000000 -000000", 0x0, false},
		{"FF:FF:FF:FF:FF:FF", 0xffffffffffff, false},
		{"AA:BB:CC:DD:EE:FF", 0xaabbccddeeff, false},
		{"Aa:Bb:Cc:Dd:Ee:Ff", 0xaabbccddeeff, false},
		{"aA:bB:cC:dD:eE:fF", 0xaabbccddeeff, false},
		{"FF-FF-FF-FF-FF-FF", 0xffffffffffff, false},
		{"AA-BB-2C-DD-EE-FF", 0xaabb2cddeeff, false},
		{"Aa-Bb-Cc-Dd-Ee-Ff", 0xaabbccddeeff, false},
		{"aA-bB-cC-dD-eE-f1", 0xaabbccddeef1, false},

		// Extra long that is valid
		{"08:000:002b:0000000000000000000000000001:002:03f",
			0x08002b01023f,
			false},
		{"08-000-002b-0000000000000000000000000001-002-03f",
			0x08002b01023f,
			false},

		// Min/max values
		{"00:00:00:00:00:00", 0x000000000000, false},
		{"ff:ff:ff:ff:ff:ff", 0xffffffffffff, false},
		{"00-00-00-00-00-00", 0x000000000000, false},
		{"ff-ff-ff-ff-ff-ff", 0xffffffffffff, false},
		{"000000000000", 0x000000000000, false},
		{"ffffffffffff", 0xffffffffffff, false},

		// Special valid variants supported by Postgres but not included in Postgres
		// documentation
		{"  08-00-2b-01-02-13 ", 0x08002b010213, false},
		{"08002b01020", 0x08002b010200, false},
		{"00-00-00-00", 0x000000000000, false},
		{"0:1:2:3:4:5", 0x000102030405, false},
		{"00-00-00-0", 0x000000000000, false},
		{"0-00-00-0", 0x000000000000, false},
		{"08:00:2b:01:02:03f", 0x08002b01023f, false},
		{"08:000:002b:0001:002:03f", 0x08002b01023f, false},
		{"00-00-00-0", 0x0, false},

		// Invalid formats
		{"0800:2b01:0203", 0, true},
		{"not even close", 0, true},
		{"-0800:2b01:020", 0, true},
		{"-0800:2b01:02-0", 0, true},
		{"-0800:2b01:020---", 0, true},
		{"-0800:2b01:020- ", 0, true},
		{"08-00:2b-01:02:03", 0, true}, // mixed separators
		{"02:00:5e:10:00:00:00:01", 0, true},
		{"0200.5e10.0000.0001", 0, true},
		{"02-00-5e-10-00-00-00-01", 0, true},
		{"0\t-0\r-5e-10-00-00-00-01", 0, true},
		{"0000.0000.fe80.0000.0000.0000.0200.5e10.0000.0001", 0, true},
		{" 000000 -000000", 0, true},
		{"000000 -000000 ", 0, true},
		{"0-1-2-3-4", 0, true},
		{"  ff:ff:ff:ff: ff:f  ", 0, true},

		// Overflow cases
		{"fffffffffffff", 0, true},
		{"0000000000000", 0, true},
		{"ffffffffffffffff", 0, true},
		{"0000000000000000", 0, true},

		// Extra long invalid
		{"00:00:00:00:fe:80:00:00:00:00:00:00:02:00:5e:10:00:00:00:01",
			0, true},
		{"00-00-00-00-fe-80-00-00-00-00-00-00-02-00-5e-10-00-00-00-01",
			0, true},

		// Leading zeros in non-6 group formats (should fail)
		{"0800:002b01:0203", 0, true},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("TestCase: %d", i), func(t *testing.T) {
			got, err := parseMAC(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseMAC(%q) = %s, want %d error %s", tt.input, got,
					tt.want, err)
			}
		})
	}
}

// TestMACAddrNot checks the invert operation on a MAC address.
func TestMACAddrNot(t *testing.T) {
	tests := []struct {
		name string
		mac  MACAddr
		want MACAddr
	}{
		{
			name: "invert MAC 08:00:2b:01:02:03",
			mac:  MACAddr(0x08002b010203),
			want: MACAddr(0xf7ffd4fefdfc),
		},
		{
			name: "invert MAC 12:34:56:78:9a:bc",
			mac:  MACAddr(0x123456789abc),
			want: MACAddr(0xedcba9876543),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MACAddrNot(tc.mac)
			if got != tc.want {
				t.Errorf("MacAddrNot(%#x) = %#x; want %#x", uint64(tc.mac),
					uint64(got), uint64(tc.want))
			}
			// Additionally, a & ~a should be zero.
			if MACAddrAnd(tc.mac, got) != 0 {
				t.Errorf("expected %#x & ~%#x == 0", uint64(tc.mac),
					uint64(tc.mac))
			}
			// And a | ~a should be all ones.
			if MACAddrOr(tc.mac, got) != MACAddr(0xFFFFFFFFFFFF) {
				t.Errorf("expected %#x | ~%#x == 0xFFFFFFFFFFFF",
					uint64(tc.mac),
					uint64(tc.mac))
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
		{"AND with all ones", MACAddr(0x08002b010203), MACAddr(0xFFFFFFFFFFFF),
			MACAddr(0x08002b010203)},
		{"AND non-trivial", MACAddr(0x123456789abc), MACAddr(0x0f0f0f0f0f0f),
			MACAddr(uint64(0x123456789abc) & uint64(0x0f0f0f0f0f0f))},
		{"AND with zero", MACAddr(0x08002b010203), MACAddr(0), MACAddr(0)},
		{"AND with self", MACAddr(0x08002b010203), MACAddr(0x08002b010203),
			MACAddr(0x08002b010203)},
		{"AND with max val", MACAddr(0xFFFFFFFFFFFF), MACAddr(0xFFFFFFFFFFFF),
			MACAddr(0xFFFFFFFFFFFF)},
		{"AND with min val", MACAddr(0), MACAddr(0), MACAddr(0)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MACAddrAnd(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("MacAddrAnd(%x, %x) = %x; want %x",
					tc.a, tc.b, got, tc.want)
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
		{"OR with all ones", MACAddr(0x08002b010203),
			MACAddr(0xFFFFFFFFFFFF), MACAddr(0xFFFFFFFFFFFF)},
		{"OR non-trivial", MACAddr(0x123456789abc),
			MACAddr(0x0f0f0f0f0f0f), MACAddr(0x1f3f5f7f9fbf)},
		{"OR with zero", MACAddr(0x08002b010203),
			MACAddr(0), MACAddr(0x08002b010203)},
		{"OR with self", MACAddr(0x08002b010203),
			MACAddr(0x08002b010203), MACAddr(0x08002b010203)},
		{"OR with max value", MACAddr(0xFFFFFFFFFFFF),
			MACAddr(0xFFFFFFFFFFFF), MACAddr(0xFFFFFFFFFFFF)},
		{"OR with min value", MACAddr(0), MACAddr(0), MACAddr(0)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := MACAddrOr(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("MacAddrOr(%x, %x) = %x; want %x",
					tc.a, tc.b, got, tc.want)
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
