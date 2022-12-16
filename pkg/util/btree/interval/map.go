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

import "github.com/cockroachdb/cockroach/pkg/util/btree/internal/abstract"

// MapConfig is used to make new Maps.
type MapConfig[I Interval[K], K, V any] abstract.Config[I, V, aug[K], updater[I, K, V]]

// NewMapConfig constructs a new MapConfig for a given set of comparators
// for the provided types. Because the interval and key types can be inferred
// from the argument, only the value needs to be explicitly stated.
func NewMapConfig[V any, I Interval[K], K any](c Comparators[I, K]) *MapConfig[I, K, V] {
	if c.CmpI == nil {
		cmpK := c.CmpK
		c.CmpI = func(a, b I) int {
			if cmp := cmpK(a.Key(), b.Key()); cmp != 0 {
				return cmp
			}
			endA, _ := a.UpperBound()
			endB, _ := b.UpperBound()
			return cmpK(endA, endB)
		}
	}
	return (*MapConfig[I, K, V])(abstract.NewConfig[I, V, aug[K], updater[I, K, V]](
		c.CmpI,
		updater[I, K, V]{cmp: c.CmpK},
	))
}

// MakeMap constructs a Map.
func (c *MapConfig[I, K, V]) MakeMap() Map[I, K, V] {
	return Map[I, K, V]{
		m: (*abstract.Config[I, V, aug[K], updater[I, K, V]])(c).MakeMap(),
	}
}

// Map is a ordered map from I to V where I is an interval. Its iterator
// provides efficient overlap queries.
type Map[I Interval[K], K, V any] struct {
	m abstract.Map[I, V, aug[K], updater[I, K, V]]
}

// Reset removes all items from the Map. In doing so, it allows memory
// held by the AugBTree to be recycled. Failure to call this method before
// letting a AugBTree be GCed is safe in that it won't cause a memory leak,
// but it will prevent AugBTree nodes from being efficiently re-used.
func (m *Map[I, K, V]) Reset() { m.m.Reset() }

// Clone clones the Map, lazily. It does so in constant time.
func (m *Map[I, K, V]) Clone() Map[I, K, V] {
	return Map[I, K, V]{m: m.m.Clone()}
}

// Len returns the number of items currently in the tree.
func (m *Map[I, K, V]) Len() int { return m.m.Len() }

// Upsert adds the given item to the tree. If an item in the tree already equals
// the given one, it is replaced with the new item.
func (m *Map[I, K, V]) Upsert(key I, value V) (removedK I, removedV V, didRemove bool) {
	return m.m.Upsert(key, value)
}

// Delete removes an item equal to the passed in item from the tree.
func (m *Map[I, K, V]) Delete(key I) (removedK I, v V, didRemove bool) {
	return m.m.Delete(key)
}

// Get returns the value associated with the requested key, if it exists.
func (m *Map[I, K, V]) Get(key I) (k I, v V, ok bool) { return m.m.Get(key) }

// Contains returns true if the map contains the requested key.
func (m *Map[I, K, V]) Contains(key I) (ok bool) { return m.m.Contains(key) }

// Iterator constructs a new Iterator for the Map.
func (m *Map[I, K, V]) MakeIter() Iterator[I, K, V] {
	var it Iterator[I, K, V]
	m.m.InitIterator(&it.it)
	return it
}

// String returns a string description of the tree. The format is
// similar to the https://en.wikipedia.org/wiki/Newick_format.
func (m *Map[I, K, V]) String() string { return m.m.String() }
