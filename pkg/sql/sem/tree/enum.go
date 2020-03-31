// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import "bytes"

const minToken int = 0

// Note that while maxToken is outside the range of a single
// byte, we never actually insert it in GenByteStringBetween.
// We only use it to perform computation using the value 256.
const maxToken int = 256
const midToken = maxToken / 2

func getIdxWithDefault(arr []byte, idx int, def int) int {
	if idx >= len(arr) {
		return def
	}
	return int(arr[idx])
}

func maybeSlice(arr []byte, idx int) []byte {
	if idx > len(arr) {
		return []byte(nil)
	}
	return arr[idx:]
}

func midPoint(lo int, hi int) int {
	return lo + (hi-lo)/2
}

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
	for i := 0; i < maxLen; i++ {
		p, n := getIdxWithDefault(prev, i, minToken), getIdxWithDefault(next, i, maxToken)
		if p == n {
			// If the ranges are equal at this point, add the current
			// value and continue.
			result = append(result, byte(p))
		} else {
			mid := midPoint(p, n)
			if mid == p {
				// If mid == p, then we know there is no more room between
				// prev and next at this index. So, we can append p to result
				// to ensure that it is less than next. To generate the rest
				// of the string, we try to find a string that fits between
				// the remainder of prev and posinf.
				result = append(result, byte(p))
				rest := GenByteStringBetween(maybeSlice(prev, i+1), nil)
				return append(result, rest...)
			}
			// In this case, there is room between prev and next at this index.
			// So, occupy that spot and return.
			result = append(result, byte(mid))
			return result
		}
	}
	return result
}

func isLess(a []byte, b []byte) bool {
	if len(b) == 0 {
		return true
	}
	return bytes.Compare(a, b) == -1
}
