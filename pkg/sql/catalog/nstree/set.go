// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nstree

import "github.com/cockroachdb/cockroach/pkg/sql/catalog"

// Set is a set of namespace keys. Safe for use without initialization.
// Calling Clear will return memory to a sync.Pool.
type Set struct {
	m byNameMap
}

// Add will add the relevant namespace key to the set.
func (s *Set) Add(components catalog.NameKey) {
	s.maybeInitialize()
	s.m.t.ReplaceOrInsert(makeByNameKey(components), nil)
}

// Contains will test whether the relevant namespace key was added.
func (s *Set) Contains(components catalog.NameKey) bool {
	if !s.initialized() {
		return false
	}
	return s.m.t.Has(makeByNameKey(components))
}

// Clear will clear the set, returning any held memory to the sync.Pool.
func (s *Set) Clear() {
	if !s.initialized() {
		return
	}
	s.m.clear()
	*s = Set{}
}

// Empty returns true if the set has no entries.
func (s *Set) Empty() bool {
	return !s.initialized() || s.m.t.Len() == 0
}

func (s *Set) maybeInitialize() {
	if s.initialized() {
		return
	}
	*s = Set{
		m: makeByNameMap(),
	}
}

func (s Set) initialized() bool {
	return s != (Set{})
}
