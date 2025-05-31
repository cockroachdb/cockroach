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
// Postgres only supports 6-byte formats. This implementation is
// adapted from net.ParseMAC but modified to parse directly into a
// uint64 to avoid extra allocations.
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
	// We'll collect each "group" of hex digits here.
	groups := make([]uint32, 0, 8)

	// Trim all leading and trailing whitespace.
	s = strings.TrimSpace(s)

	var (
		// The first separator we encounter: '.', ':' or '-' ).
		sep byte
		// Have we encountered any separator at all?
		sawSep bool
		// Accumulator for the current group’s value.
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

		// Spaces in the middle are not allowed.
		if c <= ' ' {
			return 0, errInvalidChar
		}

		if v := hexMap[c]; v >= 0 {
			// If we found a valid hex digit then it's part of our group.
			gv = (gv << 4) | uint32(v)
			gl++
			// No group may exceed 12 nibbles because
			// 12 nibbles = 48 bits, which is the max width
			// of a 6 byte MAC address.
			if gl > 12 {
				return 0, errInvalidHexCount
			}
			continue
		}

		// If it's not a valid hex nibble we may have encountered
		// a separator. So let's determine which type of separator it is.
		if c == '.' || c == ':' || c == '-' {
			if !sawSep {
				sep = c
				sawSep = true
			} else if c != sep {
				// We don't allow mixed sperators.
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
	case (sep == '-' || sep == ':' || sep == '.'):

		// These formats can only have 2, 3, 4 or 6 groups in total.
		if n < 2 || n == 5 || n > 6 {
			return 0, errInvalidGroupCnt
		}

		// Determine how many bytes per group there are.
		var bytesPerGroup int
		if n == 2 {
			// Two group format: 08002b-010203 or 08002b:010203
			// has 3 bytes in each group which makes up
			// the 6 byte MAC address.
			bytesPerGroup = 3
		} else if n == 3 && (sep == '-' || sep == '.') {
			// Three group format: 0800.2b01.0203 or 0800-2b01-0203
			// has 2 bytes in each group which makes up
			// the 6 byte MAC address. Note 0800:2b01:0203 is not a valid
			// format.
			bytesPerGroup = 2
		} else {
			// 08-00-2b-01-02-13 or 08:00:2b:01:02:3f has 1 byte per group.
			bytesPerGroup = 1
		}

		// Byte-width mask for each group. It’s exactly the maximum
		// value you can represent in bytesPerGroup.
		maxv := uint32(1<<(8*bytesPerGroup)) - 1

		var r MACAddr
		for _, gv := range groups {
			// Make sure each group cannot overflow it's max bytes.
			if gv > maxv {
				return 0, errGroupOverflow
			}

			// Concatenate each parsed group into our MACAddr.
			r = (r << uint(8*bytesPerGroup)) | MACAddr(gv)
		}

		return r, nil

	// Bare-hex (no separators) means everything is in a single group.
	default:
		return MACAddr(groups[0]), nil
	}
}

// parseError is a string‐backed error type so all messages live in static data.
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

// MACAddrNot performs bitwise NOT on the MAC address.
func MACAddrNot(addr MACAddr) MACAddr {
	return MACAddr(^uint64(addr))
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
