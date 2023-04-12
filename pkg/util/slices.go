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

import "golang.org/x/exp/constraints"

// CombineUnique combines two ordered slices and returns the result without
// duplicates.
// This function is used for combine slices where one of the slices is small or
// has mostly the same elements as the other.
// If the two slices are large and don't have many duplications, this function should be avoided,
// because of the usage of `copy` that can increase CPU.
func CombineUnique[T constraints.Ordered](a, b []T) []T {
	// We want b to be the smaller slice, so there are fewer elements to be added.
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
			var zero T
			a = append(a, zero)
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
