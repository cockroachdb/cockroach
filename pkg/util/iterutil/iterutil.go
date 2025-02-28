// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package iterutil

import (
	"iter"

	"github.com/cockroachdb/errors"
)

var errStopIteration = errors.New("stop iteration")

// StopIteration returns a sentinel error that indicates stopping the iteration.
//
// This error should not be propagated further, i.e., if a closure returns
// this error, the loop should break returning nil error. For example:
//
//	f := func(i int) error {
//		if i == 10 {
//			return iterutil.StopIteration()
//		}
//		return nil
//	}
//
//	for i := range slice {
//		if err := f(i); err != nil {
//			return iterutil.Map(err)
//		}
//		// continue when nil error
//	}
func StopIteration() error { return errStopIteration }

// Map the nil if it is StopIteration, or keep the error otherwise
func Map(err error) error {
	if errors.Is(err, errStopIteration) {
		return nil
	}
	return err
}

// Enumerate takes an iter.Seq and returns an iter.Seq2 where the first value
// is an int counter.
func Enumerate[E any](seq iter.Seq[E]) iter.Seq2[int, E] {
	return func(yield func(int, E) bool) {
		var i int
		for v := range seq {
			if !yield(i, v) {
				return
			}
			i++
		}
	}
}

// Keys takes an iter.Seq2 and returns an iter.Seq that iterates over the
// first value in each pair of values.
func Keys[K, V any](seq iter.Seq2[K, V]) iter.Seq[K] {
	return func(yield func(K) bool) {
		for k := range seq {
			if !yield(k) {
				return
			}
		}
	}
}

// MinFunc returns the minimum element in seq, using cmp to compare elements.
// If seq has no values, the zero value is returned.
func MinFunc[E any](seq iter.Seq[E], cmp func(E, E) int) E {
	var m E
	for i, v := range Enumerate(seq) {
		if i == 0 || cmp(v, m) < 0 {
			m = v
		}
	}
	return m
}
