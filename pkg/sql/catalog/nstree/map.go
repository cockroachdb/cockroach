// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nstree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/google/btree"
)

// Map is a lookup structure for descriptors. It is used to provide
// indexed access to a set of entries either by name or by ID. The
// entries' properties are indexed; they must not change or else the
// index will be corrupted.
type Map struct {
	byID   byIDMap
	byName byNameMap
}

// EntryIterator is used to iterate namespace entries.
// If an error is returned, iteration is stopped and will be propagated
// up the stack. If the error is iterutil.StopIteration, iteration will
// stop but no error will be returned.
type EntryIterator func(entry catalog.NameEntry) error

// MakeMap makes a new Map.
func MakeMap() Map {
	const (
		degree       = 8 // arbitrary
		initialNodes = 2 // one per tree
	)
	freeList := btree.NewFreeList(initialNodes)
	return Map{
		byName: byNameMap{t: btree.NewWithFreeList(degree, freeList)},
		byID:   byIDMap{t: btree.NewWithFreeList(degree, freeList)},
	}
}

// Upsert adds the descriptor to the tree. If any descriptor exists in the
// tree with the same name or id, it will be removed.
func (dt *Map) Upsert(d catalog.NameEntry) {
	if replaced := dt.byName.upsert(d); replaced != nil {
		dt.byID.delete(replaced.GetID())
	}
	if replaced := dt.byID.upsert(d); replaced != nil {
		dt.byName.delete(replaced)
	}
}

// Remove removes the descriptor with the given ID from the tree and
// returns it if it exists.
func (dt *Map) Remove(id descpb.ID) catalog.NameEntry {
	if d := dt.byID.delete(id); d != nil {
		dt.byName.delete(d)
		return d
	}
	return nil
}

// GetByID gets a descriptor from the tree by id.
func (dt *Map) GetByID(id descpb.ID) catalog.NameEntry {
	return dt.byID.get(id)
}

// GetByName gets a descriptor from the tree by name.
func (dt *Map) GetByName(parentID, parentSchemaID descpb.ID, name string) catalog.NameEntry {
	return dt.byName.getByName(parentID, parentSchemaID, name)
}

// Clear removes all entries.
func (dt *Map) Clear() {
	dt.byID.clear()
	dt.byName.clear()
}

// IterateByID iterates the descriptors by ID, ascending.
func (dt *Map) IterateByID(f EntryIterator) error {
	return dt.byID.ascend(f)
}

// Len returns the number of descriptors in the tree.
func (dt *Map) Len() int {
	return dt.byID.len()
}
