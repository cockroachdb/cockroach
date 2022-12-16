// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package orderstat

import "github.com/cockroachdb/cockroach/pkg/util/btree/internal/abstract"

// MapConfig is used to construct a Map.
type MapConfig[K, V any] abstract.Config[K, V, aug, updater[K, V]]

// NewMapConfig constructs a MapConfig.
func NewMapConfig[V, K any](cmp func(K, K) int) *MapConfig[K, V] {
	return (*MapConfig[K, V])(
		abstract.NewConfig[K, V, aug, updater[K, V]](
			cmp, updater[K, V]{},
		),
	)
}

// MakeMap constructs a new Map.
func (c *MapConfig[K, V]) MakeMap() Map[K, V] {
	return Map[K, V]{
		m: (*abstract.Config[K, V, aug, updater[K, V]])(c).MakeMap(),
	}
}

// Map is a ordered map from K to V.
type Map[K, V any] struct {
	m abstract.Map[K, V, aug, updater[K, V]]
}

// Reset removes all items from the Map. In doing so, it allows memory
// held by the AugBTree to be recycled. Failure to call this method before
// letting a AugBTree be GCed is safe in that it won't cause a memory leak,
// but it will prevent AugBTree nodes from being efficiently re-used.
func (m *Map[K, V]) Reset() { m.m.Reset() }

// Clone clones the Map, lazily. It does so in constant time.
func (m *Map[K, V]) Clone() Map[K, V] {
	return Map[K, V]{m: m.m.Clone()}
}

// Len returns the number of items currently in the tree.
func (m *Map[K, V]) Len() int { return m.m.Len() }

// Upsert adds the given item to the tree. If an item in the tree already equals
// the given one, it is replaced with the new item.
func (m *Map[K, V]) Upsert(key K, value V) (removedK K, removedV V, didRemove bool) {
	return m.m.Upsert(key, value)
}

// Delete removes an item equal to the passed in item from the tree.
func (m *Map[K, V]) Delete(key K) (removedK K, v V, didRemove bool) {
	return m.m.Delete(key)
}

// Get returns the value associated with the requested key, if it exists.
func (m *Map[K, V]) Get(key K) (k K, v V, ok bool) { return m.m.Get(key) }

// Contains returns true if the map contains the requested key.
func (m *Map[K, V]) Contains(key K) (ok bool) { return m.m.Contains(key) }

// Iterator constructs a new Iterator for the Map.
func (t *Map[K, V]) MakeIter() Iterator[K, V] {
	var it Iterator[K, V]
	t.m.InitIterator(&it.it)
	return it
}

// String returns a string description of the tree. The format is
// similar to the https://en.wikipedia.org/wiki/Newick_format.
func (m *Map[K, V]) String() string { return m.m.String() }
