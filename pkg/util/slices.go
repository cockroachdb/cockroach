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

// CombineUnique merges two ordered slices. If both slices have unique elements
// then so does the resulting slice. More generally, each element is present
// max(timesInA, timesInB) times.
//
// Takes ownership of both slices, and uses the longer one to store the result.
//
// This function is used to combine slices where one of the slices is small or
// has mostly the same elements as the other. If the two slices are large and
// don't have many duplicates, this function should be avoided, because of the
// usage of `copy` that can increase CPU.
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

// Filter appends elements from collection that satisfy predicate to out and
// returns it. Accepting out as a parameter allows callers to fine tune
// allocation performance.
//
//	// As a special case, if out == nil, a new slice the size of collection will be
//	// allocated and returned. The following two statements are equivalent.
//	even := Filter(numbers, isEven, nil)
//	even := Filter(numbers, isEven, make([]int, 0, len(numbers))
//
//	// Force append to resize if any matching elements are found.
//	_ := Filter(numbers, isEven, []int{})
//
//	// Zero allocations, reuses the original slice.
//	numbers = Filter(numbers, isEven, numbers[:0])
//
//	// Exactly one allocation if we know/guessed the number of elements that
//	// satisfy the predicate (e.g. we did a "first pass" over it).
//	even := Filter(numbers, isEven, make([]int, 0, 10))
//
//	// We optimistically halve the slice size, but allocate 1 extra time if
//	// it doesn't work out.
//	even := Filter(numbers, isEven, make([]int, 0, len(numbers)/2)
func Filter[T any](collection []T, predicate func(T) bool, out []T) []T {
	// Special/Ergonomic case. If out is nil, optimistically guess that we'll not
	// be filtering many values.
	if out == nil {
		out = make([]T, 0, len(collection))
	}

	for i := range collection {
		if predicate(collection[i]) {
			out = append(out, collection[i])
		}
	}

	return out
}

// Map returns a new slice containing the results of fn for each element within
// collection. Usage:
//
//	Map([]int{1, 2, 3}, func(i int) int {
//		return i
//	})
func Map[T, K any](collection []T, fn func(T) K) []K {
	out := make([]K, len(collection))
	for i, el := range collection {
		out[i] = fn(el)
	}
	return out
}

// MapFrom returns a map populated with keys and values returned by fn.
// Usage:
//
//	// Construct a set.
//	MapFrom(numbers, func(i int) (int, struct{}) {
//		return i, struct{}{}
//	})
//
//	// Construct a map of numbers to their square.
//	MapFrom(numbers, func(i int) (int, int) {
//		return i, i * i
//	})
func MapFrom[T any, K comparable, V any](collection []T, fn func(T) (K, V)) map[K]V {
	out := make(map[K]V, len(collection))
	for _, el := range collection {
		key, value := fn(el)
		out[key] = value
	}
	return out
}
