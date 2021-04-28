// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

// CombineUniqueInt64 combine two ordered int64 arrays and return the result without
// duplicates.
func CombineUniqueInt64(a []int64, b []int64) []int64 {
	res := make([]int64, 0, len(a))
	aIter, bIter := 0, 0
	for aIter < len(a) && bIter < len(b) {
		if a[aIter] == b[bIter] {
			res = append(res, a[aIter])
			aIter++
			bIter++
		} else if a[aIter] < b[bIter] {
			res = append(res, a[aIter])
			aIter++
		} else {
			res = append(res, b[bIter])
			bIter++
		}
	}
	if aIter < len(a) {
		res = append(res, a[aIter:]...)
	}
	if bIter < len(b) {
		res = append(res, b[bIter:]...)
	}
	return res
}

// EqualInt64Array compare if two int64 arrays are equal.
func EqualInt64Array(a, b []int64) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
