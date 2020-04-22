// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package enum

import "bytes"

// Note that while maxToken is outside the range of a single
// byte, we never actually insert it in GenByteStringBetween.
// We only use it to perform computation using the value 256.
const minToken int = 0
const maxToken int = 256
const midToken = maxToken / 2
const shiftInterval = 5

// GenByteStringBetween generates a byte string that sorts
// between the two input strings. If prev is length 0, it is
// treated as negative infinity. If next is length 0, it is
// treated as positive infinity. Importantly, the input strings
// cannot end with minToken.
func GenByteStringBetween(prev []byte, next []byte) []byte {
	result := make([]byte, 0)
	if len(prev) == 0 && len(next) == 0 {
		// If both prev and next are unbounded, return the midpoint.
		return append(result, byte(midToken))
	}
	maxLen := len(prev)
	if len(next) > maxLen {
		maxLen = len(next)
	}

	// Construct the prefix of prev and next.
	pos := 0
	for ; pos < maxLen; pos++ {
		p, n := get(prev, pos, minToken), get(next, pos, maxToken)
		if p != n {
			break
		}
		result = append(result, byte(p))
	}

	// We've found an index where prev and next disagree. So, it's time to start
	// trying to construct a value between prev and next.
	p, n := get(prev, pos, minToken), get(next, pos, maxToken)
	mid := byteBetween(p, n)

	// If mid == p, then we know there is no more room between
	// prev and next at this index. So, we can append p to result
	// to ensure that it is less than next. To generate the rest
	// of the string, we try to find a string that fits between
	// the remainder of prev and posinf.
	if mid == p {
		result = append(result, byte(p))
		rest := GenByteStringBetween(slice(prev, pos+1), nil)
		return append(result, rest...)
	}

	// If mid != p, then there is room between prev and next at this index.
	// So, occupy that spot and return.
	result = append(result, byte(mid))
	return result
}

// Utility functions for GenByteStringBetween.

func get(arr []byte, idx int, def int) int {
	if idx >= len(arr) {
		return def
	}
	return int(arr[idx])
}

func slice(arr []byte, idx int) []byte {
	if idx > len(arr) {
		return []byte(nil)
	}
	return arr[idx:]
}

// byteBetween generates a byte value between hi and lo. Additionally,
// it optimizes for adding values at the beginning and end of the
// enum byte sequence by returning a value moved by a small constant
// rather than in the middle of the range when the upper and lower
// bounds are the min or max token.
func byteBetween(lo int, hi int) int {
	switch {
	case lo == minToken && hi == maxToken:
		return lo + (hi-lo)/2
	case lo == minToken && hi-lo > shiftInterval:
		return hi - shiftInterval
	case hi == maxToken && hi-lo > shiftInterval:
		return lo + shiftInterval
	default:
		return lo + (hi-lo)/2
	}
}

// enumBytesAreLess is a utility function for comparing byte values generated
// for enum's physical representation. It adjusts bytes.Compare to treat b=nil
// as positive infinity.
func enumBytesAreLess(a []byte, b []byte) bool {
	if len(b) == 0 {
		return true
	}
	return bytes.Compare(a, b) == -1
}
