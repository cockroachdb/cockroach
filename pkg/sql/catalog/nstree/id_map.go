// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nstree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// IDMap is a lookup structure for descriptors. It is used to provide
// indexed access to a set of entries by ID.
// The entries' properties are indexed; they must not change or else the
// index will be corrupted. Safe for use without initialization. Calling
// Clear will return memory to a sync.Pool.
type IDMap struct {
	byIDMap
}

// EntryIterator is used to iterate namespace entries.
// If an error is returned, iteration is stopped and will be propagated
// up the stack. If the error is iterutil.StopIteration, iteration will
// stop but no error will be returned.
type EntryIterator func(entry catalog.NameEntry) error

// Upsert adds the entry to the tree. If any entry exists in the
// tree with the same ID, it will be removed.
func (dt *IDMap) Upsert(d catalog.NameEntry) {
	dt.maybeInitialize()
	dt.upsert(d)
}

// Remove removes the entry with the given ID from the tree and
// returns it if it exists.
func (dt *IDMap) Remove(id descpb.ID) catalog.NameEntry {
	dt.maybeInitialize()
	return dt.delete(id)
}

// Get gets an entry from the tree by id.
func (dt *IDMap) Get(id descpb.ID) catalog.NameEntry {
	if !dt.initialized() {
		return nil
	}
	return dt.get(id)
}

// Clear removes all entries, returning any held memory to the sync.Pool.
func (dt *IDMap) Clear() {
	if !dt.initialized() {
		return
	}
	dt.clear()
	*dt = IDMap{}
}

// Iterate iterates the descriptors by ID, ascending.
func (dt *IDMap) Iterate(f EntryIterator) error {
	if !dt.initialized() {
		return nil
	}
	return dt.ascend(f)
}

// Len returns the number of descriptors in the tree.
func (dt *IDMap) Len() int {
	if !dt.initialized() {
		return 0
	}
	return dt.len()
}

func (dt *IDMap) maybeInitialize() {
	if dt.initialized() {
		return
	}
	*dt = IDMap{byIDMap: makeByIDMap()}
}
