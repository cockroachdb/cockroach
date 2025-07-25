// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package macaddr

import (
	"fmt"

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

// isWhitespace checks for ASCII whitespace characters like PostgreSQL
func isWhitespace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

func parseMAC(s string) (MACAddr, error) {
	// Find bounds without string slicing allocation - trim all ASCII whitespace
	start := 0
	end := len(s)
	for start < end && isWhitespace(s[start]) {
		start++
	}
	for end > start && isWhitespace(s[end-1]) {
		end--
	}
	leadingOrTrailing := start > 0 || end < len(s)

	// Zero-allocation parsing with direct indexing
	var groups [8]uint32
	var numGroups int
	var (
		sep               byte
		sawSep            bool
		gv                uint32
		gl                int
		groupLengths      [8]int
		trackGroupLengths bool
		sawInternalSpace  bool
	)

	// Parse directly using start/end indices to avoid slice allocation
	for i := start; i < end; i++ {
		c := s[i]

		// Hot path optimization - most characters are hex digits
		if v := hexMap[c]; v >= 0 {
			gv = (gv << 4) | uint32(v)
			gl++
			continue
		}

		// Optimized space handling
		if c == ' ' {
			sawInternalSpace = true
			if sawSep && sep != ':' {
				return 0, errInvalidChar
			}
			continue
		}

		// Separator handling - optimized for common patterns
		switch c {
		case ':', '-', '.':
			if !sawSep {
				sep = c
				sawSep = true
				if leadingOrTrailing && sawInternalSpace && c != ':' {
					return 0, errInvalidChar
				}
				trackGroupLengths = (c == ':' || c == '.')
			} else if c != sep {
				return 0, errInvalidChar
			}

			if numGroups >= 8 {
				return 0, errInvalidGroupCnt
			}
			groups[numGroups] = gv
			if trackGroupLengths {
				groupLengths[numGroups] = gl
			}
			numGroups++
			gv, gl = 0, 0
		default:
			return 0, errInvalidChar
		}
	}

	// Up until now we collected each group after we encountered
	// a separator. But if it's a MAC address without any separators
	// or if it's the last group, we would still need to collect.
	if gl > 0 {
		if numGroups >= 8 {
			return 0, errInvalidGroupCnt
		}
		groups[numGroups] = gv

		// Track group lengths for validation
		if trackGroupLengths {
			groupLengths[numGroups] = gl
		}
		numGroups++
	} else {
		// no hex at all
		return 0, errInvalidHexCount
	}

	// This is where the fun begins. Decide style, combine and pad.

	// Handle the most common cases first for better branch prediction
	if sep == 0 {
		// Bare-hex (no separators) means everything is in a single group.
		// PostgreSQL accepts 11-12 hex chars for contiguous bare hex, or any reasonable length with spaces
		if !sawInternalSpace && (gl < 11 || gl > 12) {
			return 0, errInvalidHexCount
		} else if gl > 12 {
			return 0, errInvalidHexCount
		}
		return MACAddr(groups[0]), nil
	}

	if sep == ':' {
		if !(numGroups == 2 || numGroups == 6) {
			return 0, errInvalidGroupCnt
		}

		if numGroups == 2 {
			// 2 groups, validate lengths and construct result
			if trackGroupLengths && (groupLengths[0] != 6 || groupLengths[1] != 6) {
				return 0, errInvalidGroupLen
			}
			return MACAddr(groups[0])<<24 | MACAddr(groups[1]&0xFFFFFF), nil
		}

		// 6 groups - let compiler optimize
		var r MACAddr
		for i := range 6 {
			r = (r << 8) | MACAddr(groups[i]&0xFF)
		}
		return r, nil
	}

	if sep == '-' {
		if !(numGroups == 2 || numGroups == 3 || numGroups == 4 || numGroups == 6) {
			return 0, errInvalidGroupCnt
		}

		switch numGroups {
		case 2:
			var r MACAddr
			for i := range 2 {
				r = (r << 24) | MACAddr(groups[i]&0xFFFFFF)
			}
			return r, nil
		case 3:
			var r MACAddr
			for i := range 3 {
				r = (r << 16) | MACAddr(groups[i]&0xFFFF)
			}
			return r, nil
		case 6:
			var r MACAddr
			for i := range 6 {
				r = (r << 8) | MACAddr(groups[i]&0xFF)
			}
			return r, nil
		default: // case 4
			var r MACAddr
			for i := range 4 {
				r = (r << 8) | MACAddr(groups[i]&0xFF)
			}
			return r, nil
		}
	}

	if sep == '.' {
		if numGroups != 3 {
			return 0, errInvalidGroupCnt
		}

		// Validate group lengths inline
		if trackGroupLengths && (groupLengths[0] > 4 || groupLengths[1] > 4 || groupLengths[2] > 4) {
			return 0, errInvalidGroupLen
		}

		var r MACAddr
		for i := range 3 {
			r = (r << 16) | MACAddr(groups[i]&0xFFFF)
		}
		return r, nil
	}

	// Should never reach here
	return 0, errInvalidChar
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
