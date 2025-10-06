// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"cmp"
	"slices"
)

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
func CombineUnique[T cmp.Ordered](a, b []T) []T {
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

// Filter returns a new slice that only contains elements from collection that
// satisfy predicate.
//
//	// Filter in place
//	numbers = Filter(numbers, isEven)
//	// Filter into a new slice
//	odds := Filter(numbers, isEven)
func Filter[T any](collection []T, predicate func(T) bool) []T {
	i := 0
	out := make([]T, len(collection))
	for j := range collection {
		if predicate(collection[j]) {
			out[i] = collection[j]
			i++
		}
	}
	return slices.Clip(out[:i])
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

// InsertUnique inserts an element into an ordered slice if the element is not
// already present while maintaining the ordering property. Possibly updated
// slice is returned.
func InsertUnique[T cmp.Ordered](s []T, v T) []T {
	idx, found := slices.BinarySearch(s, v)
	if found {
		return s
	}
	return slices.Insert(s, idx, v)
}
