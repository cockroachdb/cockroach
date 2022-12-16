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

import "github.com/cockroachdb/cockroach/pkg/util/btree/internal/aug"

// Map is an ordered map from K to V with the specified Config.
// The zero value is usable so long as the config's zero value is usable.
type Map[C Config[K], K, V any] struct {
	m aug.BTree[C, K, V, noAug, noop[C, K, V]]
}

// MakeMap constructs a map with a custom config.
// It only needs to be used if the config needs initialization.
func MakeMap[K, V any, C Config[K]](cmp C) Map[C, K, V] {
	return Map[C, K, V]{
		m: aug.MakeMap[C, K, V, noAug, noop[C, K, V]](cmp),
	}
}

// MakeMapWithFunc constructs a map with a custom function comparator.
func MakeMapWithFunc[K, V any](cmp func(a, b K) int) Map[WithFunc[K], K, V] {
	return MakeMap[K, V](WithFunc[K]{Func: cmp})
}

// Reset removes all items from the Map. In doing so, it allows memory
// held by the AugBTree to be recycled. Failure to call this method before
// letting a AugBTree be GCed is safe in that it won't cause a memory leak,
// but it will prevent AugBTree nodes from being efficiently re-used.
func (m *Map[C, K, V]) Reset() { m.m.Reset() }

// Compare can be used to compare keys with the underlying comparator.
func (m *Map[C, K, V]) Compare(a, b K) int {
	return m.m.Compare(a, b)
}

// Clone clones the Map, lazily. It does so in constant time.
func (m *Map[C, K, V]) Clone() Map[C, K, V] {
	return Map[C, K, V]{m: m.m.Clone()}
}

// Len returns the number of items currently in the tree.
func (m *Map[C, K, V]) Len() int { return m.m.Len() }

// Upsert adds the given item to the tree. If an item in the tree already
// equals the given one, it is replaced with the new item.
func (m *Map[C, K, V]) Upsert(key K, value V) (removedK K, removedV V, didRemove bool) {
	return m.m.Upsert(key, value)
}

// Delete removes an item equal to the passed in item from the tree.
func (m *Map[C, K, V]) Delete(key K) (removedK K, v V, didRemove bool) {
	return m.m.Delete(key)
}

// Get returns the value associated with the requested key, if it exists.
func (m *Map[C, K, V]) Get(key K) (k K, v V, ok bool) { return m.m.Get(key) }

// Contains returns true if the map contains the requested key.
func (m *Map[C, K, V]) Contains(key K) (ok bool) { return m.m.Contains(key) }

// MakeIter constructs a new Iterator for the Map.
func (m *Map[C, K, V]) MakeIter() Iterator[C, K, V] {
	var it Iterator[C, K, V]
	m.m.InitIterator(&it.it)
	return it
}

// String returns a string description of the tree. The format is
// similar to the https://en.wikipedia.org/wiki/Newick_format.
func (m *Map[C, K, V]) String() string { return m.m.String() }
