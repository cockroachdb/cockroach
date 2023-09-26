package schemachange

import "golang.org/x/exp/slices"

// TODO(chrisseto): Drop this file once is https://github.com/cockroachdb/cockroach/pull/111276 merged.

// Filter removes all elements from collection that do not satisfy predicate.
// NOTE: Collection is modified in place. To create a new slice, utilize
// slices.Clone. Usage:
//
//	// Filter in place
//	numbers = Filter(numbers, isEven)
//	// Filter into a new slice
//	odds := Filter(slices.Clone(numbers), isEven)
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

// MapBy returns a map populated with keys and values returned by fn.
// Usage:
//
//	// Construct a set.
//	MapBy(numbers, func(i int) (int, struct{}) {
//		return i, struct{}{}
//	})
//
//	// Construct a map of numbers to their square.
//	MapBy(numbers, func(i int) (int, int) {
//		return i, i * i
//	})
func MapBy[T any, K comparable, V any](collection []T, fn func(T) (K, V)) map[K]V {
	out := make(map[K]V, len(collection))
	for _, el := range collection {
		key, value := fn(el)
		out[key] = value
	}
	return out
}
