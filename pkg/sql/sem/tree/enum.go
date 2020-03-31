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

const minByte byte = 0
const maxByte byte = 4
const midByte byte = 2

func getIdxWithDefault(arr []byte, idx int, def byte) byte {
	if idx >= len(arr) {
		return def
	}
	return arr[idx]
}

func maybeSlice(arr []byte, idx int) []byte {
	if idx > len(arr) {
		return []byte(nil)
	}
	return arr[idx:]
}

func midPoint(lo byte, hi byte) byte {
	return lo + (hi-lo)/2
}

// GenByteStringInBetween generates a byte string that sorts
// between the two input strings. If prev is length 0, it is
// treated as negative infinity. If next is length 0, it is
// treated as positive infinity.
// TODO (rohany): I don't think this is really general purpose,
//  but I also don't know how to describe the restrictions on
//  the input strings other than they should be outputs of this
//  function.
func GenByteStringBetween(prev []byte, next []byte) []byte {
	result := make([]byte, 0)
	if len(prev) == 0 && len(next) == 0 {
		// If both prev and next are unbounded, return the midpoint.
		return append(result, midByte)
	}
	maxLen := len(prev)
	if len(next) > maxLen {
		maxLen = len(next)
	}
	for i := 0; i < maxLen; i++ {
		p, n := getIdxWithDefault(prev, i, minByte), getIdxWithDefault(next, i, maxByte)
		if p == n {
			// If the ranges are equal at this point, add the current
			// value and continue.
			result = append(result, p)
		} else {
			mid := midPoint(p, n)
			if mid == minByte {
				// In this case, we know that p == min and n != min. So, we know
				// that there isn't any room between prev and next at this index.
				// So, we can append minByte to result to guarantee that it is less
				// than next. To find the rest of the result we need to find a string
				// that fits between the rest of prev and posinf.
				result = append(result, minByte)
				rest := GenByteStringBetween(maybeSlice(prev, i+1), nil)
				return append(result, rest...)
			} else if mid == p {
				// In the case that the midpoint is the same as prev, we no longer
				// have room in between prev and next at this index. So, append
				// n to result. This guarantees that result is larger than prev.
				// To find the rest of the result, find a string that fits between
				// neginf and the rest of next.
				result = append(result, n)
				rest := GenByteStringBetween(nil, maybeSlice(next, i+1))
				return append(result, rest...)
			} else {
				// In this case, there is room between prev and next at this index.
				// So, occupy that spot and return.
				result = append(result, mid)
				return result
			}
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
