// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package interval

// Interval represents an interval with bounds from Key() to UpperBound() where
// Key() is Inclusive and UpperBound() defines whether it is inclusive.
// Intervals with an UpperBound() which is smaller than Key() are invalid and
// may result in panics upon insertion. Note that an UpperBound() with a key
// equal to Key() but not inclusive is considered invalid.
type Interval[K any] interface {
	// Key returns the inclusive minimum value of the Interval.
	Key() K

	// UpperBound returns the maximum bound of the Interval. The inclusive
	// marker is only valid if the returned K is equal to Key().
	UpperBound() (_ K, inclusive bool)
}

// Comparators configures the btree for how it should compare intervals.
type Comparators[I Interval[K], K any] struct {

	// CmpK is used to compare interval keys. It must not be nil.
	CmpK Cmp[K]

	// CmpI is used to determine whether two intervals are the same. If the
	// same [Key, EndKey) interval should exist multiple times, then this
	// function must be used to break ties. If it is nil, then equality will
	// be defined as Key comparison followed by EndKey comparison.
	CmpI Cmp[I]
}

// Cmp is a comparison function for type T.
type Cmp[T any] func(T, T) int
