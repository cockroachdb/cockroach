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

import "github.com/cockroachdb/cockroach/pkg/util/btree/ordered"

// Config controls how the btree relates intervals.
type Config[Interval, Point any] interface {

	// Comparator is used to order intervals relative to each other.
	ordered.Comparator[Interval]

	// Key is used to extract the minimum point of the interval. It is inclusive.
	Key(Interval) Point

	// UpperBound is used to extract the maximum point of the interval. Intervals
	// in this package are defined as having an inclusive minimum Key and an
	// exclusive maximum UpperBound. If the interval represents exactly one
	// point, the inclusive flag should be returned along with the same value
	// which Key returns.
	UpperBound(Interval) (_ Point, inclusive bool)

	// ComparePoints is used to compare two points.
	ComparePoints(a, b Point) int
}

// upperBound is the maximum value of the interval.
type upperBound[K any] struct {
	K         K
	Inclusive bool
}

func contains[C Config[I, K], I, K any](b upperBound[K], k K) bool {
	c := (*new(C)).ComparePoints(k, b.K)
	return (c == 0 && b.Inclusive) || c < 0
}

func key[C Config[I, K], I, K any](item I) K {
	return (*new(C)).Key(item)
}

func makeUpperBound[K any](k K, inclusive bool) upperBound[K] {
	return upperBound[K]{K: k, Inclusive: inclusive}
}

func ub[C Config[I, K], I, K any](item I) upperBound[K] {
	return makeUpperBound[K]((*new(C)).UpperBound(item))
}

func eq[C Config[I, K], I, K any](a, b upperBound[K]) bool {
	return (*new(C)).ComparePoints(a.K, b.K) == 0 &&
		a.Inclusive == b.Inclusive
}

func less[C Config[I, K], I, K any](a, b upperBound[K]) bool {
	if c := (*new(C)).ComparePoints(a.K, b.K); c != 0 {
		return c < 0
	}
	return !a.Inclusive || b.Inclusive
}
