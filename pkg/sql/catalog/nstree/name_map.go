// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nstree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// NameMap is a lookup structure for descriptors. It is used to provide
// indexed access to a set of entries either by name or by ID. The
// entries' properties are indexed; they must not change or else the
// index will be corrupted. Safe for use without initialization. Calling
// Clear will return memory to a sync.Pool.
type NameMap struct {
	byID   byIDMap
	byName byNameMap
	// nameSkipped record the ids of items upsert by skipping the name map.
	nameSkipped map[descpb.ID]struct{}
}

// Upsert adds the descriptor to the tree. If any descriptor exists in the
// tree with the same name or id, it will be removed.
func (dt *NameMap) Upsert(d catalog.NameEntry, skipNameMap bool) {
	dt.maybeInitialize()

	if skipNameMap {
		dt.byID.upsert(d)
		dt.nameSkipped[d.GetID()] = struct{}{}
	} else {
		if replaced := dt.byName.upsert(d); replaced != nil {
			dt.byID.delete(replaced.GetID())
		}
		if replaced := dt.byID.upsert(d); replaced != nil {
			dt.byName.delete(replaced)
		}
		delete(dt.nameSkipped, d.GetID())
	}
}

// Remove removes the descriptor with the given ID from the tree and
// returns it if it exists.
func (dt *NameMap) Remove(id descpb.ID) catalog.NameEntry {
	dt.maybeInitialize()
	if d := dt.byID.delete(id); d != nil {
		if _, ok := dt.nameSkipped[id]; !ok {
			dt.byName.delete(d)
		}
		return d
	}
	return nil
}

// GetByID gets a descriptor from the tree by id.
func (dt *NameMap) GetByID(id descpb.ID) catalog.NameEntry {
	if !dt.initialized() {
		return nil
	}
	return dt.byID.get(id)
}

// GetByName gets a descriptor from the tree by name.
func (dt *NameMap) GetByName(parentID, parentSchemaID descpb.ID, name string) catalog.NameEntry {
	if !dt.initialized() {
		return nil
	}
	return dt.byName.getByName(parentID, parentSchemaID, name)
}

// Clear removes all entries, returning any held memory to the sync.Pool.
func (dt *NameMap) Clear() {
	if !dt.initialized() {
		return
	}
	dt.byID.clear()
	dt.byName.clear()
	*dt = NameMap{}
}

// IterateByID iterates the descriptors by ID, ascending.
func (dt *NameMap) IterateByID(f EntryIterator) error {
	if !dt.initialized() {
		return nil
	}
	return dt.byID.ascend(f)
}

// iterateByName iterates the descriptors by name, ascending.
// This method is only used by data driven test internally.
// Use IterateByID instead to get all descriptors.
func (dt *NameMap) iterateByName(f EntryIterator) error {
	if !dt.initialized() {
		return nil
	}
	return dt.byName.ascend(f)
}

// IterateDatabasesByName iterates the database descriptors by name, ascending.
func (dt *NameMap) IterateDatabasesByName(f EntryIterator) error {
	if !dt.initialized() {
		return nil
	}
	return dt.byName.ascendDatabases(f)
}

// IterateSchemasForDatabaseByName iterates the schema descriptors for the
// database by name, ascending.
func (dt *NameMap) IterateSchemasForDatabaseByName(dbID descpb.ID, f EntryIterator) error {
	if !dt.initialized() {
		return nil
	}
	return dt.byName.ascendSchemasForDatabase(dbID, f)
}

// Len returns the number of descriptors in the tree.
func (dt *NameMap) Len() int {
	if !dt.initialized() {
		return 0
	}
	return dt.byID.len()
}

func (dt NameMap) initialized() bool {
	return dt.byID.initialized() && dt.byName.initialized() && dt.nameSkipped != nil
}

func (dt *NameMap) maybeInitialize() {
	if dt.initialized() {
		return
	}
	*dt = NameMap{
		byName:      makeByNameMap(),
		byID:        makeByIDMap(),
		nameSkipped: make(map[descpb.ID]struct{}),
	}
}
