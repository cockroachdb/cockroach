// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulk

// Iterator is an interface to iterate a collection of objects of type T.
type Iterator[T any] interface {
	// Valid must be called after any call to Next(). It returns (true, nil) if
	// the iterator points to a valid value, and (false, nil) if the iterator has
	// moved past the last value. It returns (false, err) if there is an error in
	// the iterator.
	Valid() (bool, error)

	// Value returns the current value. The returned value is only valid until the
	// next call to Next(). is only valid until the
	Value() T

	// Next advances the iterator to the next value.
	Next()

	// Close closes the iterator.
	Close()
}

// CollectToSlice iterates over all objects in iterator and collects them into a
// slice.
func CollectToSlice[T any](iterator Iterator[T]) ([]T, error) {
	var values []T
	for ; ; iterator.Next() {
		if ok, err := iterator.Valid(); err != nil {
			return nil, err
		} else if !ok {
			break
		}

		values = append(values, iterator.Value())
	}
	return values, nil
}
