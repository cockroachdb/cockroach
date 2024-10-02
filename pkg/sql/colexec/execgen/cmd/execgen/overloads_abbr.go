// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import "github.com/cockroachdb/cockroach/pkg/sql/types"

// CanAbbreviate returns true if the canonical type family supports abbreviation
// to a uint64 that can be used for comparison fast paths.
//
// For each type for which CanAbbreviate() returns true, a corresponding method
// must be defined on the type called Abbreviated that returns a []uint64
// representing abbreviated values for each datum in the vector. Given datums a
// and b, the following properties must hold true for the respective abbreviated
// values abbrA and abbrB:
//
//   - abbrA > abbrB iff a > b
//   - abbrA < abbrB iff a < b
//   - If abbrA == abbrB, it is unknown if a is greater than, less than, or
//     equal to b. A full comparison of all bytes in each is required.
//
// For an example, see Bytes.Abbreviated.
//
// Some operations, especially sorting, spend a significant amount of time
// performing comparisons between two datums. Abbreviating values to a uint64
// helps optimize these comparisons for datums that are larger than the size of
// the system's native pointer (64-bits). If the abbreviated values of two
// datums are different, there is no need to compare the full value of the
// datums.
//
// This improves comparison performance for two reasons. First, comparing two
// uint64s is a single CPU instruction. Any datum larger than 64 bits requires
// multiple instructions for comparison. Second, CPU caches are more efficiently
// used because the abbreviated values of a vector are packed into a []uint64 of
// contiguous memory. This increases the chance that two datums can be compared
// by only fetching information from CPU caches rather than main memory.
func (b *argWidthOverloadBase) CanAbbreviate() bool {
	switch b.CanonicalTypeFamily {
	case types.BytesFamily:
		return true
	}
	return false
}

// Remove unused warnings.
var (
	_ = awob.CanAbbreviate
)
