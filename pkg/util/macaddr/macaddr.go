// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package macaddr

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// hexMap: -1 = not hex, 0–15 = value
// we use this to lookup if a particular rune
// is a valid mac addr hex value in range of 0-15.
var hexMap [256]int8

func init() {
	for i := range hexMap {
		hexMap[i] = int8(-1)
	}
	for c := byte('0'); c <= '9'; c++ {
		hexMap[c] = int8(c - '0')
	}
	for c := byte('a'); c <= 'f'; c++ {
		hexMap[c] = int8(c - 'a' + 10)
	}
	for c := byte('A'); c <= 'F'; c++ {
		hexMap[c] = int8(c - 'A' + 10)
	}
}

// MACAddr is the representation of a MAC address. The uint64 takes 8-bytes for
// both 6 and 8 byte Macaddr.
type MACAddr uint64

var errInvalidMACAddr = pgerror.New(pgcode.InvalidTextRepresentation,
	"invalid MAC address")

// ParseMAC parses Postgres-style 6-byte MACAddr types.
// While Go's net.ParseMAC supports MAC addresses up to 20 octets,
// Postgres only supports 6-byte formats. The unused bits of a 6-byte
// MacAddr may be set to anything.
//
// It supports the following Postgres-documented formats:
//   - '08:00:2b:01:02:03'  (17 characters)
//   - '08-00-2b-01-02-03'
//   - '08002b:010203'      (13 characters)
//   - '08002b-010203'
//   - '0800.2b01.0203'     (14 characters)
//   - '0800-2b01-0203'
//   - '08002b010203'       (12 characters)
//
// It also handles additional Postgres-accepted formats:
//   - Leading/trailing whitespace: '  08-00-2b-01-02-13 '
//   - Shortened formats (padded or unpadded automatically):
//     '08002b01020'
//     '00-00-00-00'
//     '0:1:2:3:4:5'
//     '00-00-00-0'
//     '0-00-00-0'
//     '08:00:2b:01:02:03f'
//     '08:000:002b:0001:002:03f'
func ParseMAC(s string) (MACAddr, error) {
	p, err := parseMAC(s)
	if err != nil {
		return 0, errInvalidMACAddr
	}

	return p, nil
}

func parseMAC(s string) (MACAddr, error) {

	// Check for leading/trailing spaces.
	trimmed := strings.TrimSpace(s)
	leadingOrTrailing := len(s) != len(trimmed)
	s = trimmed

	// We'll collect each "group" of hex digits here.
	groups := make([]uint32, 0, 8)

	var (
		// The first separator we encounter: '.', ':' or '-' ).
		sep byte
		// Have we encountered any separator at all?
		sawSep bool
		// Accumulator for the current group's value.
		gv uint32
		// The length of a group.
		// This cannot exceed 12 since a MAC address
		// with no seperator e.g: ffffffffffff has max
		// length of 12.
		gl int
	)

	// 1) Accumulate hex groups.
	for i := range len(s) {
		c := s[i]

		// Handle spaces in the middle.
		if c == ' ' {
			// We can't allow spaces in the middle if there are leading and/or trailing spaces.
			if leadingOrTrailing {
				return 0, errInvalidChar
			}

			// Spaces in the middle are okay if no leading and/or trailing spaces.
			continue
		}

		if v := hexMap[c]; v >= 0 {
			// If we found a valid hex digit then it's part of our group.
			// Each digit is shifted left by 4 bits and OR'd with the new digit.
			// This naturally handles leading zeros - e.g. "008" becomes 8 because
			// shifting 0 left and OR'ing with 0 gives 0, then shifting 0 left and
			// OR'ing with 8 gives 8.
			// The total length of the group (including leading zeros) is tracked
			// for max group length validation.
			gv = (gv << 4) | uint32(v)
			gl++
			continue
		}

		// If it's not a valid hex nibble we may have encountered
		// a separator. So let's determine which type of separator it is.
		if c == '.' || c == ':' || c == '-' {
			if !sawSep {
				sep = c
				sawSep = true
			} else if c != sep {
				// We don't allow mixed separators.
				return 0, errInvalidChar
			}

			// Add each group to our collection of groups
			groups = append(groups, gv)

			// Reset so we can parse next group.
			gv, gl = 0, 0
			continue
		}

		// Anything else is invalid.
		return 0, errInvalidChar
	}

	// Up until now we collected each group after we encountered
	// a separator. But if it's a MAC address without any separators
	// or if it's the last group, we would still need to collect.
	if gl > 0 {
		groups = append(groups, gv)
	} else {
		// no hex at all
		return 0, errInvalidHexCount
	}

	// This is where the fun begins. Decide style, combine and pad.
	n := len(groups)

	switch {
	// Handle dash or colon grouping separator formats. These are:
	// 08-00-2b-01-02-13
	// 08:00:2b:01:02:3f
	// 08002b:010203
	// 08002b-010203
	// 0800-2b01-0203
	// 0800.2b01.0203
	case sep == ':':
		// Allow 2 or 6 groups for ':'.
		if !(n == 2 || n == 6) {
			return 0, errInvalidGroupCnt
		}
		var bytesPerGroup int
		if n == 2 {
			bytesPerGroup = 3
		} else {
			bytesPerGroup = 1
		}
		maxv := uint32(1<<(8*bytesPerGroup)) - 1
		var r MACAddr
		for _, gv := range groups {
			gv = gv & maxv
			r = (r << uint(8*bytesPerGroup)) | MACAddr(gv)
		}
		return r, nil
	case sep == '-':
		// Allow 2, 3, 4, or 6 groups for '-'.
		if !(n == 2 || n == 3 || n == 4 || n == 6) {
			return 0, errInvalidGroupCnt
		}
		var bytesPerGroup int
		if n == 2 {
			bytesPerGroup = 3
		} else if n == 3 {
			bytesPerGroup = 2
		} else {
			bytesPerGroup = 1
		}
		maxv := uint32(1<<(8*bytesPerGroup)) - 1
		var r MACAddr
		for _, gv := range groups {
			gv = gv & maxv
			r = (r << uint(8*bytesPerGroup)) | MACAddr(gv)
		}
		return r, nil
	case sep == '.':
		// Only 3 groups are valid for '.'
		if n != 3 {
			return 0, errInvalidGroupCnt
		}
		bytesPerGroup := 2
		maxv := uint32(1<<(8*bytesPerGroup)) - 1
		var r MACAddr
		for _, gv := range groups {
			gv = gv & maxv
			r = (r << uint(8*bytesPerGroup)) | MACAddr(gv)
		}
		return r, nil
	// Bare-hex (no separators) means everything is in a single group.
	default:
		if gl > 12 {
			return 0, errInvalidHexCount
		}
		return MACAddr(groups[0]), nil
	}
}

// parseError is a string-backed error type so all messages live in static data.
type parseError string

func (e parseError) Error() string { return string(e) }

const (
	errInvalidChar     = parseError("invalid character in MAC string")
	errInvalidGroupLen = parseError("invalid MAC group length")
	errGroupOverflow   = parseError("MAC group value overflow")
	errInvalidGroupCnt = parseError("invalid number of MAC groups")
	errInvalidHexCount = parseError("invalid number of hex digits in MAC")
)

// hibits returns the higher 32 bits of the MAC address.
func hibits(a MACAddr) uint32 {
	return uint32(uint64(a) >> 32)
}

// lobits returns the lower 32 bits of the MAC address.
func lobits(a MACAddr) uint32 {
	return uint32(uint64(a) & 0xFFFFFFFF)
}

// CompareMACs compares two MAC addresses using their high and low bits.
func (m MACAddr) Compare(addr MACAddr) int {
	if hibits(m) < hibits(addr) {
		return -1
	} else if hibits(m) > hibits(addr) {
		return 1
	} else if lobits(m) < lobits(addr) {
		return -1
	} else if lobits(m) > lobits(addr) {
		return 1
	} else {
		return 0
	}
}

// MACAddrNot performs bitwise NOT (invert) on a MAC address.
func MACAddrNot(addr MACAddr) MACAddr {
	return MACAddr(^uint64(addr) & 0xFFFFFFFFFFFF)
}

// MACAddrAnd performs bitwise AND between two MAC addresses.
func MACAddrAnd(addr1, addr2 MACAddr) MACAddr {
	return MACAddr(uint64(addr1) & uint64(addr2))
}

// MACAddrOr performs bitwise OR between two MAC addresses.
func MACAddrOr(addr1, addr2 MACAddr) MACAddr {
	return MACAddr(uint64(addr1) | uint64(addr2))
}

// String implements the stringer interface for MACAddr.
func (m MACAddr) String() string {
	return fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x",
		byte(m>>40),
		byte(m>>32),
		byte(m>>24),
		byte(m>>16),
		byte(m>>8),
		byte(m))
}
