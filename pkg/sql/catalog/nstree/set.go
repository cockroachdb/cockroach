// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nstree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/google/btree"
)

// Set is a set of namespace keys. Safe for use without initialization.
// Calling Clear will return memory to a sync.Pool.
type Set struct {
	t *btree.BTree
}

// Add will add the relevant namespace key to the set.
func (s *Set) Add(components catalog.NameKey) {
	s.maybeInitialize()
	item := makeByNameItem(components).get()
	item.v = item // the value needs to be non-nil
	upsert(s.t, item)
}

// Contains will test whether the relevant namespace key was added.
func (s *Set) Contains(components catalog.NameKey) bool {
	if !s.initialized() {
		return false
	}
	return get(s.t, makeByNameItem(components).get()) != nil
}

// Clear will clear the set, returning any held memory to the sync.Pool.
func (s *Set) Clear() {
	if !s.initialized() {
		return
	}
	clear(s.t)
	btreeSyncPool.Put(s.t)
	*s = Set{}
}

// Empty returns true if the set has no entries.
func (s *Set) Empty() bool {
	return !s.initialized() || s.t.Len() == 0
}

func (s *Set) maybeInitialize() {
	if s.initialized() {
		return
	}
	*s = Set{
		t: btreeSyncPool.Get().(*btree.BTree),
	}
}

func (s Set) initialized() bool {
	return s != (Set{})
}
