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
		{"fffff ff   fff", 0xffff0fffff0f, false},
		{"ff:ff:ff:ff: ff:f", 0xffffffffffff0f, false},
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
		{"  ff:ff:ff:ff: ff:f  ", 0xffffffffffff0f, false},

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
		{"0800:2b01:0203", 0, true},

		// These formats should fail (invalid dot format)
		{"0800.002b01.0203", 0, true},
		{"0800.1234558247574839922b01.0203", 0, true},
		{" fffff ff   fff ", 0xffff0fffff0f, false},

		// Edge cases found during code review
		{"", 0, true},                                    // Empty string
		{"   ", 0, true},                                 // Only spaces
		{"ffffffffffffffffffff", 0, true},                // Too long (20 chars)
		{"fffffffffffffff", 0, true},                     // 15 chars
		{"08:00:2b:01:02:", 0, true},                     // Trailing separator
		{":08:00:2b:01:02:03", 0, true},                  // Leading separator
		{"08::00:2b:01:02:03", 0, true},                  // Double separator
		{"08:00:2b:01:02:03:", 0, true},                  // Trailing separator (7 groups)
		{"ff\tff", 0, true},                              // Tab character
		{"ff\nff", 0, true},                              // Newline character
		{"ffffffffffffffffff", 0, true},                  // 18 chars
		{"08:00:2b:01:02:03:04:05:06:07:08:09", 0, true}, // Too many groups
		{"08.00.2b.01.0203", 0, true},                    // 5 groups with dots
		{"08.00.2b.01", 0, true},                         // 4 groups with dots
		{"g8:00:2b:01:02:03", 0, true},                   // Invalid hex

		// Test integer overflow edge cases
		{"12345678901234567890", 0, true},             // 20 digit number that could overflow uint32
		{"ffffffffffffffffffffffffffffffff", 0, true}, // 32 f's - way beyond any reasonable limit

		// Test group length validation for 2-group colon format
		{"08:010203", 0, true},                   // 2+6 chars (should fail)
		{"0800:010203", 0, true},                 // 4+6 chars (should fail)
		{"08000:10203", 0, true},                 // 5+5 chars (should fail)
		{"080000:010203", 0x080000010203, false}, // 6+6 chars (should work)

		// Test bare hex length validation
		{"0", 0, true},                          // 1 char (should fail)
		{"00", 0, true},                         // 2 chars (shouldfail)
		{"000000", 0, true},                     // 6 chars (should fail)
		{"00000000", 0, true},                   // 8 chars (should fail)
		{"0000000000", 0, true},                 // 10 chars (should fail)
		{"00000000000", 0x00000000000, false},   // 11 chars (should work)
		{"000000000000", 0x000000000000, false}, // 12 chars (should work)

		// Unicode character test cases
		{"08:00:2b:01:02:0ğ", 0, true}, // Turkish 'ğ' (U+011F)
		{"08:00:2b:01:02:0ç", 0, true}, // Turkish 'ç' (U+00E7)
		{"08:00:2b:01:02:0ı", 0, true}, // Turkish 'ı' (U+0131)
		{"08:00:2b:01:02:0ş", 0, true}, // Turkish 'ş' (U+015F)
		{"08:00:2b:01:02:0ü", 0, true}, // Turkish 'ü' (U+00FC)
		{"08:00:2b:01:02:0ö", 0, true}, // Turkish 'ö' (U+00F6)
		{"08:00:2b:01:02:0€", 0, true}, // Euro symbol (U+20AC)
		{"08:00:2b:01:02:0🚀", 0, true}, // Rocket emoji (U+1F680)
		{"08:00:2b:01:02:0α", 0, true}, // Greek alpha (U+03B1)
		{"08:00:2b:01:02:0中", 0, true}, // Chinese character (U+4E2D)
		{"08:00:2b:01:02:0Ж", 0, true}, // Cyrillic Zhe (U+0416)
		{"08:00:2b:01:02:0×", 0, true}, // Multiplication sign (U+00D7)
		{"08:00:2b:01:02:0Ω", 0, true}, // Greek Omega (U+03A9)
		{"08:00:2b:01:02:0‰", 0, true}, // Per mille sign (U+2030)
		{"08:00:2b:01:02:0™", 0, true}, // Trademark sign (U+2122)
		{"08:00:2b:01:02:0≤", 0, true}, // Less-than or equal (U+2264)

		// Multi-byte Unicode in different positions
		{"ğ8:00:2b:01:02:03", 0, true}, // Unicode at start
		{"08:ğ0:2b:01:02:03", 0, true}, // Unicode in middle
		{"08:00:2b:01:02:0ğ", 0, true}, // Unicode at end
		{"08ğ00:2b01:0203", 0, true},   // Unicode in dot notation
		{"08-00-2b-01-02-0ğ", 0, true}, // Unicode in dash notation

		// Non-ASCII but valid Latin-1 characters (should still fail as not hex)
		{"08:00:2b:01:02:0ý", 0, true}, // Latin small y with acute (U+00FD)
		{"08:00:2b:01:02:0þ", 0, true}, // Latin small thorn (U+00FE)
		{"08:00:2b:01:02:0ÿ", 0, true}, // Latin small y with diaeresis (U+00FF)

		// Control characters and special Unicode
		{"08:00:2b:01:02:0\x7F", 0, true},   // DEL character
		{"08:00:2b:01:02:0\x1F", 0, true},   // Unit separator
		{"08:00:2b:01:02:0\x00", 0, true},   // NULL character
		{"08:00:2b:01:02:0\uFEFF", 0, true}, // BOM (Byte Order Mark)
		{"08:00:2b:01:02:0\u200B", 0, true}, // Zero-width space
		{"08:00:2b:01:02:0\u00A0", 0, true}, // Non-breaking space

		// Invalid UTF-8 sequences (raw bytes)
		{"08:00:2b:01:02:0\xFF", 0, true},         // Invalid UTF-8 byte
		{"08:00:2b:01:02:0\xC0\x80", 0, true},     // Overlong encoding of NULL
		{"08:00:2b:01:02:0\xED\xA0\x80", 0, true}, // High surrogate (invalid UTF-8)

		// Comprehensive whitespace and tab character tests
		{"08:00:2b:01:02:03\t", 0x08002b010203, false}, // Tab at end (should work - trimmed)
		{"\t08:00:2b:01:02:03", 0x08002b010203, false}, // Tab at start (should work - trimmed)
		{"08:00:2b\t01:02:03", 0, true},                // Tab in middle (should fail)
		{"08:00:2b:01:02:03\n", 0x08002b010203, false}, // Newline at end (should work - trimmed)
		{"\n08:00:2b:01:02:03", 0x08002b010203, false}, // Newline at start (should work - trimmed)
		{"08:00:2b\n01:02:03", 0, true},                // Newline in middle (should fail)
		{"08:00:2b:01:02:03\r", 0x08002b010203, false}, // Carriage return at end (should work - trimmed)
		{"\r08:00:2b:01:02:03", 0x08002b010203, false}, // Carriage return at start (should work - trimmed)
		{"08:00:2b\r01:02:03", 0, true},                // Carriage return in middle (should fail)
		{"08:00:2b:01:02:03\v", 0x08002b010203, false}, // Vertical tab at end (should work - trimmed)
		{"\v08:00:2b:01:02:03", 0x08002b010203, false}, // Vertical tab at start (should work - trimmed)
		{"08:00:2b\v01:02:03", 0, true},                // Vertical tab in middle (should fail)
		{"08:00:2b:01:02:03\f", 0x08002b010203, false}, // Form feed at end (should work - trimmed)
		{"\f08:00:2b:01:02:03", 0x08002b010203, false}, // Form feed at start (should work - trimmed)
		{"08:00:2b\f01:02:03", 0, true},                // Form feed in middle (should fail)

		// Multiple whitespace characters
		{"  \t  08:00:2b:01:02:03  \t  ", 0x08002b010203, false}, // Mixed spaces and tabs at ends (should work)
		{"08:00:2b:01:02:03\t\t", 0x08002b010203, false},         // Multiple tabs at end (should work)
		{"\t\t08:00:2b:01:02:03", 0x08002b010203, false},         // Multiple tabs at start (should work)
		{"08:00:2b:01:02:03\n\r", 0x08002b010203, false},         // Mixed newlines at end (should work)
		{"\r\n08:00:2b:01:02:03", 0x08002b010203, false},         // Mixed newlines at start (should work)

		// Unicode whitespace characters (should fail)
		{"08:00:2b:01:02:03\u2000", 0, true}, // En quad
		{"08:00:2b:01:02:03\u2001", 0, true}, // Em quad
		{"08:00:2b:01:02:03\u2002", 0, true}, // En space
		{"08:00:2b:01:02:03\u2003", 0, true}, // Em space
		{"08:00:2b:01:02:03\u2004", 0, true}, // Three-per-em space
		{"08:00:2b:01:02:03\u2005", 0, true}, // Four-per-em space
		{"08:00:2b:01:02:03\u2006", 0, true}, // Six-per-em space
		{"08:00:2b:01:02:03\u2007", 0, true}, // Figure space
		{"08:00:2b:01:02:03\u2008", 0, true}, // Punctuation space
		{"08:00:2b:01:02:03\u2009", 0, true}, // Thin space
		{"08:00:2b:01:02:03\u200A", 0, true}, // Hair space
		{"08:00:2b:01:02:03\u202F", 0, true}, // Narrow no-break space
		{"08:00:2b:01:02:03\u205F", 0, true}, // Medium mathematical space
		{"08:00:2b:01:02:03\u3000", 0, true}, // Ideographic space

		// Whitespace with different MAC formats
		{"\t08002b010203\t", 0x08002b010203, false},      // Tabs with bare hex (should work)
		{"\n0800.2b01.0203\n", 0x08002b010203, false},    // Newlines with dot notation (should work)
		{"\r08-00-2b-01-02-03\r", 0x08002b010203, false}, // Carriage returns with dash notation (should work)
		{"\v08002b:010203\v", 0x08002b010203, false},     // Vertical tabs with colon notation (should work)

		// Mixed Unicode and whitespace
		{"08:00:2b:01:02:03\tğ", 0, true}, // Tab + Unicode
		{"08:00:2b:01:02:03\nα", 0, true}, // Newline + Unicode
		{"08:00:2b:01:02:03\r€", 0, true}, // Carriage return + Unicode
		{"ğ\t08:00:2b:01:02:03", 0, true}, // Unicode + Tab at start
		{"α\n08:00:2b:01:02:03", 0, true}, // Unicode + Newline at start
		{"€\r08:00:2b:01:02:03", 0, true}, // Unicode + Carriage return at start

		// Valid spaces (only regular ASCII space allowed at leading/trailing)
		{" 08:00:2b:01:02:03 ", 0x08002b010203, false},   // Regular spaces (should work)
		{"  08:00:2b:01:02:03  ", 0x08002b010203, false}, // Multiple regular spaces (should work)

		// Internal spaces with different separators
		{"08 00 2b 01 02 03", 0x08002b010203, false}, // Spaces with no separators (should work)
		{"08:00 2b:01 02:03", 0, true},               // Internal spaces with colons (should fail)
		{"08-00 2b-01 02-03", 0, true},               // Internal spaces with dashes (should fail)
		{"0800.2b01 0203", 0, true},                  // Internal spaces with dots (should fail)

		// Edge cases with spaces and Unicode spaces
		{"\u00A0 08:00:2b:01:02:03 \u00A0", 0, true}, // Non-breaking spaces
		{"\u200B08:00:2b:01:02:03\u200B", 0, true},   // Zero-width spaces
		{"\u2060 08:00:2b:01:02:03 \u2060", 0, true}, // Word joiners
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("TestCase: %d", i), func(t *testing.T) {
			got, err := parseMAC(tt.input)
			if (err != nil) != tt.wantErr {
				if tt.wantErr {
					t.Fatalf("parseMAC(%q) = %s, want error but got none", tt.input, got)
				} else {
					t.Fatalf("parseMAC(%q) = %s, want %d but got error: %s", tt.input, got, tt.want, err)
				}
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
