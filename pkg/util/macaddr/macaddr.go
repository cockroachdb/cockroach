// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package macaddr

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// MACAddr is the representation of a MAC address. The uint64 takes 8-bytes for
// both 6 and 8 byte Macaddr.
type MACAddr uint64

// bigger than we need, not too big to worry about overflow
const big = 0xFFFFFF

// ParseMAC parses Postgres style 6 byte MACAddr types. While Go's net.ParseMAC
// supports MAC adddresses upto 20 octets in length, Postgres does not. The
// following code is adapted from net.ParseMAC and modified to parse directly
// into a uint64 to avoid an extra allocation. It has also been adapted to
// support the following formats as specified by the Postgres documentation:
// '08:00:2b:01:02:03' - 17 characters
// '08-00-2b-01-02-03'
// '08002b:010203' - 13 characters
// '08002b-010203'
// '0800.2b01.0203' - 14 characters
// '0800-2b01-0203'
// '08002b010203' - 12 characters
func ParseMAC(s string) (MACAddr, error) {
	var mac uint64
	// Depending on the separator, the string can either be 12, 13, 14 or 17
	// characters long.
	switch len(s) {
	case 12:
		// No separator format: "08002b010203"
		// 6 total groups and we parse an octet at a time.
		for i := 0; i < 6; i++ {
			off := i * 2
			b, ok := xtoi2(s[off:off+2], 0)
			if !ok {
				return 0, pgerror.New(pgcode.InvalidTextRepresentation,
					"invalid MAC address")
			}
			mac = (mac << 8) | uint64(b)
		}
		return MACAddr(mac), nil
	case 13:
		// Single separator format: "08002b:010203" or "08002b-010203"
		if s[6] != ':' && s[6] != '-' {
			return 0, pgerror.New(pgcode.InvalidTextRepresentation,
				"invalid MAC address")
		}
		// Parse the left half (first 6 characters) representing the first
		// 3 octets.
		for i := 0; i < 3; i++ {
			// Calculate the offset for the current 2-digit hex substring.
			// For the left half, the offsets are: 0, 2, and 4.
			off := i * 2
			b, ok := xtoi2(s[off:off+2], 0)
			if !ok {
				return 0, pgerror.New(
					pgcode.InvalidTextRepresentation,
					"invalid MAC address",
				)
			}
			mac = (mac << 8) | uint64(b)
		}

		// Parse the right half (characters after the separator) representing
		// the last 3 octets.
		for i := 0; i < 3; i++ {
			// For the right half, the data starts at index 7.
			// Offsets are: 7, 9, and 11.
			off := 7 + i*2
			b, ok := xtoi2(s[off:off+2], 0)
			if !ok {
				return 0, pgerror.New(
					pgcode.InvalidTextRepresentation,
					"invalid MAC address",
				)
			}
			mac = (mac << 8) | uint64(b)
		}
		return MACAddr(mac), nil
	case 14:
		// Grouped 4-digit format: "0800.2b01.0203" or "0800-2b01-0203"
		// Do not allow mixed separators.
		sep := s[4]
		if (sep != '.' && sep != '-') || s[9] != sep {
			return 0, pgerror.New(pgcode.InvalidTextRepresentation,
				"invalid MAC address")
		}

		// Process 3 groups; each group has 4 hex digits representing
		// 2 octets with a seperator in between. Each iteration of the loop,
		// we call xtoi2 twice--once for the first 2 hex digits and once for
		// the next 2 hex digits—to produce a total of 2 bytes per group.
		// Over 3 groups, that yields the full 6-byte MAC address.
		for i := 0; i < 3; i++ {
			// Groups start at 0, 5, 10.
			// 4 hex digits + 1 separator, so the offset for group i is i * 5.
			off := i * 5
			// Parse the first two hex digits...
			b1, ok := xtoi2(s[off:off+2], 0)
			if !ok {
				return 0, pgerror.New(pgcode.InvalidTextRepresentation,
					"invalid MAC address")
			}
			// ...and then the next two.
			b2, ok := xtoi2(s[off+2:off+4], 0)
			if !ok {
				return 0, pgerror.New(pgcode.InvalidTextRepresentation,
					"invalid MAC address")
			}
			mac = (mac << 8) | uint64(b1)
			mac = (mac << 8) | uint64(b2)
		}
		return MACAddr(mac), nil
	case 17:
		// Standard 6 groups of 2 hex digits: "08:00:2b:01:02:03" or
		// "08-00-2b-01-02-03".
		sep := s[2]
		if sep != ':' && sep != '-' {
			return 0, pgerror.New(pgcode.InvalidTextRepresentation,
				"invalid MAC address")
		}

		// Ensure that all separator positions use the same separator.
		if s[5] != sep || s[8] != sep || s[11] != sep || s[14] != sep {
			return 0, pgerror.New(pgcode.InvalidTextRepresentation,
				"invalid MAC address")
		}

		for i := 0; i < 6; i++ {
			// 2 hex digits + 1 separator, so the offset for group i is i * 3.
			off := i * 3
			b, ok := xtoi2(s[off:off+2], 0)
			if !ok {
				return 0, pgerror.New(pgcode.InvalidTextRepresentation,
					"invalid MAC address")
			}
			mac = (mac << 8) | uint64(b)
		}
		return MACAddr(mac), nil
	default:
		return 0, pgerror.New(pgcode.InvalidTextRepresentation,
			"invalid MAC address")
	}
}

// Hexadecimal to integer.
// Returns number, characters consumed, success.
// TODO(dikshant): figure out how to reuse xtoi in util/ip.go
func xtoi(s string) (n int, i int, ok bool) {
	n = 0
	for i = 0; i < len(s); i++ {
		if '0' <= s[i] && s[i] <= '9' {
			n *= 16
			n += int(s[i] - '0')
		} else if 'a' <= s[i] && s[i] <= 'f' {
			n *= 16
			n += int(s[i]-'a') + 10
		} else if 'A' <= s[i] && s[i] <= 'F' {
			n *= 16
			n += int(s[i]-'A') + 10
		} else {
			break
		}
		if n >= big {
			return 0, i, false
		}
	}
	if i == 0 {
		return 0, i, false
	}
	return n, i, true
}

// xtoi2 converts the next two hex digits of s into a byte.
// If s is longer than 2 bytes then the third byte must be e.
// If the first two bytes of s are not hex digits or the third byte
// does not match e, false is returned.
func xtoi2(s string, e byte) (byte, bool) {
	if len(s) > 2 && s[2] != e {
		return 0, false
	}
	n, ei, ok := xtoi(s[:2])
	return byte(n), ok && ei == 2
}
