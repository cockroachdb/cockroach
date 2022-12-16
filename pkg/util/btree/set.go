// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package btree

// SetConfig is used to construct a Set.
type SetConfig[T any] MapConfig[T, struct{}]

// NewSetConfig constructs a SetConfig.
func NewSetConfig[T any](cmp func(T, T) int) *SetConfig[T] {
	return (*SetConfig[T])(NewMapConfig[struct{}](cmp))
}

// MakeSet constructs a new Set.
func (c *SetConfig[T]) MakeSet() Set[T] {
	return Set[T]{Map: (*MapConfig[T, struct{}])(c).MakeMap()}
}

// Set is an ordered set with items of type T which additionally offers the
// methods of an order-statistic tree on its iterator.
type Set[T any] struct {
	Map[T, struct{}]
}

// Clone clones the Set, lazily. It does so in constant time.
func (t *Set[T]) Clone() Set[T] {
	return Set[T]{Map: t.Map.Clone()}
}

// Upsert inserts or updates the provided item. It returns
// the overwritten item if a previous value existed for the key.
func (t *Set[T]) Upsert(item T) (replaced T, overwrote bool) {
	return withoutVal(t.m.Upsert(item, struct{}{}))
}

// Delete removes an item equal to the passed in item from the tree.
func (t *Set[T]) Delete(item T) (removed T, didRemove bool) {
	return withoutVal(t.m.Delete(item))
}

// Get returns the value associated with the requested key, if it exists.
func (t *Set[T]) Get(item T) (_ T, ok bool) {
	return withoutVal(t.m.Get(item))
}

func withoutVal[K any](k K, _ struct{}, ok bool) (K, bool) {
	return k, ok
}
