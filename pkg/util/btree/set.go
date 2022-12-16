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

import "github.com/cockroachdb/cockroach/pkg/util/btree/ordered"

// Set is an ordered set with comparable items of type T.
// The zero value is usable so long as the config's zero value is usable.
type Set[C Config[T], T any] struct{ Map[C, T, struct{}] }

// MakeSet constructs a set with an arbitrary config. It only needs to be used
// if the config requires initialization.
func MakeSet[T any, C Config[T]](c C) Set[C, T] {
	return Set[C, T]{Map: MakeMap[T, struct{}](c)}
}

// MakeSetWithFunc constructs a set with a custom comparator function.
func MakeSetWithFunc[K any](cmp ordered.Func[K]) Set[WithFunc[K], K] {
	return MakeSet[K](WithFunc[K]{Func: cmp})
}

// Clone clones the Set, lazily. It does so in constant time.
func (t *Set[C, T]) Clone() Set[C, T] {
	return Set[C, T]{Map: t.Map.Clone()}
}

// Upsert inserts or updates the provided item. It returns the overwritten
// item if a previous value existed for the key.
func (t *Set[C, T]) Upsert(item T) (replaced T, overwrote bool) {
	return withoutVal(t.m.Upsert(item, struct{}{}))
}

// Delete removes an item equal to the passed in item from the tree.
func (t *Set[C, T]) Delete(item T) (removed T, didRemove bool) {
	return withoutVal(t.m.Delete(item))
}

// Get returns the value associated with the requested key, if it exists.
func (t *Set[C, T]) Get(item T) (_ T, ok bool) {
	return withoutVal(t.m.Get(item))
}

func withoutVal[K any](k K, _ struct{}, ok bool) (K, bool) {
	return k, ok
}
