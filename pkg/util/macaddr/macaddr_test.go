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
