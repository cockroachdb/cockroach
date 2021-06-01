// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package descriptortree provides a data structure for storing and retrieving
// descriptors.
package descriptortree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/google/btree"
)

// Tree is a lookup structure for descriptors. It is used to provide
// indexed access to a set of descriptors either by name or by ID. The
// descriptor's properties are index; these names must not change or else the
// index will be corrupted.
type Tree struct {
	byID   byID
	byName byName
}

// Iterator is used to iterate descriptors.
// If an error is returned, iteration is stopped and will be propagated
// up the stack. If the error is iterutil.StopIteration, iteration will
// stop but no error will be returned.
type Iterator func(descriptor catalog.Descriptor) error

// Make makes a new Tree.
func Make() Tree {
	const (
		degree       = 8 // arbitrary
		initialNodes = 2 // one per tree
	)
	freeList := btree.NewFreeList(initialNodes)
	return Tree{
		byName: byName{t: btree.NewWithFreeList(degree, freeList)},
		byID:   byID{t: btree.NewWithFreeList(degree, freeList)},
	}
}

// Upsert adds the descriptor to the tree. If any descriptor exists in the
// tree with the same name or id, it will be removed.
func (dt *Tree) Upsert(d catalog.Descriptor) {
	if replaced := dt.byName.upsert(d); replaced != nil {
		dt.byID.delete(replaced.GetID())
	}
	if replaced := dt.byID.upsert(d); replaced != nil {
		dt.byName.delete(replaced)
	}
}

// Remove removes the descriptor with the given ID from the tree and
// returns it if it exists.
func (dt *Tree) Remove(id descpb.ID) (catalog.Descriptor, bool) {
	d := dt.byID.delete(id)
	if d == nil {
		return nil, false
	}
	dt.byName.delete(d)
	return d, true
}

// GetByID gets a descriptor from the tree by id.
func (dt *Tree) GetByID(id descpb.ID) (catalog.Descriptor, bool) {
	return dt.byID.get(id)
}

// GetByName gets a descriptor from the tree by name.
func (dt *Tree) GetByName(
	parentID, parentSchemaID descpb.ID, name string,
) (catalog.Descriptor, bool) {
	return dt.byName.getByName(parentID, parentSchemaID, name)
}

// Clear removes all entries.
func (dt *Tree) Clear() {
	dt.byID.clear()
	dt.byName.clear()
}

// IterateByID iterates the descriptors by ID, ascending.
func (dt *Tree) IterateByID(f Iterator) error {
	return dt.byID.ascend(f)
}

// Len returns the number of descriptors in the tree.
func (dt *Tree) Len() int {
	return dt.byID.len()
}

type item interface {
	btree.Item
	descriptor() catalog.Descriptor
	put()
}

func upsert(t *btree.BTree, toUpsert item) catalog.Descriptor {
	if overwritten := t.ReplaceOrInsert(toUpsert); overwritten != nil {
		overwrittenItem := overwritten.(item)
		defer overwrittenItem.put()
		return overwrittenItem.descriptor()
	}
	return nil
}

func get(t *btree.BTree, k item) (catalog.Descriptor, bool) {
	defer k.put()
	if got := t.Get(k); got != nil {
		return got.(item).descriptor(), true
	}
	return nil, false
}

func delete(t *btree.BTree, k item) catalog.Descriptor {
	defer k.put()
	if deleted, ok := t.Delete(k).(item); ok {
		defer deleted.put()
		return deleted.descriptor()
	}
	return nil
}

func clear(t *btree.BTree) {
	for t.Len() > 0 {
		t.DeleteMin().(item).put()
	}
}

func ascend(t *btree.BTree, f Iterator) (err error) {
	t.Ascend(func(i btree.Item) bool {
		err = f(i.(item).descriptor())
		return err == nil
	})
	if iterutil.Done(err) {
		err = nil
	}
	return err
}
