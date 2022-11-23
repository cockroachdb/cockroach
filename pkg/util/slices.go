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

// CombineUniqueInt64 combines two ordered int64 slices and returns
// the result without duplicates.
// This function is used for combine slices where one of the slices is small or
// has mostly the same elements as the other.
// If the two slices are large and don't have many duplications, this function should be avoided,
// because of the usage of `copy` that can increase CPU.
func CombineUniqueInt64(a []int64, b []int64) []int64 {
	// We want b to be the smaller slice, so there are fewer elements
	// to be added.
	if len(b) > len(a) {
		b, a = a, b
	}
	aIter, bIter := 0, 0
	for aIter < len(a) && bIter < len(b) {
		if a[aIter] == b[bIter] {
			aIter++
			bIter++
		} else if a[aIter] < b[bIter] {
			aIter++
		} else {
			a = append(a, 0)
			copy(a[aIter+1:], a[aIter:])
			a[aIter] = b[bIter]
			aIter++
			bIter++
		}
	}
	if bIter < len(b) {
		a = append(a, b[bIter:]...)
	}
	return a
}

// CombineUniqueString combines two ordered string slices and returns
// the result without duplicates.
// This function is used for combine slices where one of the slices is small or
// has mostly the same elements as the other.
// If the two slices are large and don't have many duplications, this function should be avoided,
// because of the usage of `copy` that can increase CPU.
func CombineUniqueString(a []string, b []string) []string {
	// We want b to be the smaller slice, so there are fewer elements
	// to be added.
	if len(b) > len(a) {
		b, a = a, b
	}
	aIter, bIter := 0, 0
	for aIter < len(a) && bIter < len(b) {
		if a[aIter] == b[bIter] {
			aIter++
			bIter++
		} else if a[aIter] < b[bIter] {
			aIter++
		} else {
			a = append(a, "")
			copy(a[aIter+1:], a[aIter:])
			a[aIter] = b[bIter]
			aIter++
			bIter++
		}
	}
	if bIter < len(b) {
		a = append(a, b[bIter:]...)
	}
	return a
}
