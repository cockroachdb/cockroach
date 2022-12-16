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

// SetConfig is used to make new Sets.
type SetConfig[I Interval[K], K any] MapConfig[I, K, struct{}]

// NewSetConfig constructs a new SetConfig for a given set of comparators.
func NewSetConfig[I Interval[K], K any](c Comparators[I, K]) *SetConfig[I, K] {
	return (*SetConfig[I, K])(NewMapConfig[struct{}](c))
}

// MakeSet constructs a Set.
func (c *SetConfig[I, K]) MakeSet() Set[I, K] {
	return Set[I, K]{Map: (*MapConfig[I, K, struct{}])(c).MakeMap()}
}

// Set is an ordered set with items of type T which additionally offers the
// methods of an order-statistic tree on its iterator.
type Set[I Interval[T], T any] struct {
	Map[I, T, struct{}]
}

// Clone clones the Set, lazily. It does so in constant time.
func (t *Set[I, T]) Clone() Set[I, T] {
	return Set[I, T]{Map: t.Map.Clone()}
}

// Upsert inserts or updates the provided item. It returns
// the overwritten item if a previous value existed for the key.
func (t *Set[I, T]) Upsert(item I) (replaced I, overwrote bool) {
	return withoutVal(t.m.Upsert(item, struct{}{}))
}

// Delete removes an item equal to the passed in item from the tree.
func (t *Set[I, T]) Delete(item I) (removed I, didRemove bool) {
	return withoutVal(t.m.Delete(item))
}

// Get returns the value associated with the requested key, if it exists.
func (t *Set[I, T]) Get(item I) (_ I, ok bool) {
	return withoutVal(t.m.Get(item))
}

func withoutVal[K any](k K, _ struct{}, ok bool) (K, bool) {
	return k, ok
}
