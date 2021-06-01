// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/google/btree"
)

// descriptorTree is a lookup structure for descriptors. It is used to provide
// indexed access to a set of descriptors either by name or by ID. The
// descriptor's properties are index; these names must not change or else the
// index will be corrupted.
type descriptorTree struct {
	byID   *btree.BTree
	byName *btree.BTree
}

func makeDescriptorTree() descriptorTree {
	const (
		degree       = 8 // arbitrary
		initialNodes = 2 // one per tree
	)
	freeList := btree.NewFreeList(initialNodes)
	return descriptorTree{
		byName: btree.NewWithFreeList(degree, freeList),
		byID:   btree.NewWithFreeList(degree, freeList),
	}
}

func (dt *descriptorTree) add(d catalog.Descriptor) {
	if prev := dt.byName.ReplaceOrInsert(getByNameItem(makeByNameItem(d))); prev != nil {
		byName := prev.(*byNameItem)
		defer putByNameItem(byName)
		byID := getByIDItem(makeByIDItem(byName.d))
		defer putByIDItem(byID)
		if deleted := dt.byID.Delete(byID); deleted != nil {
			putByIDItem(deleted.(*byIDItem))
		}
	}
	if prev := dt.byID.ReplaceOrInsert(getByIDItem(makeByIDItem(d))); prev != nil {
		byId := prev.(*byIDItem)
		defer putByIDItem(byId)
		byName := getByNameItem(makeByNameItem(byId.d))
		defer putByNameItem(byName)
		if deleted := dt.byName.Delete(byName); deleted != nil {
			putByNameItem(deleted.(*byNameItem))
		}
	}
}

func (dt *descriptorTree) remove(id descpb.ID) (existed bool) {
	k := getByIDItem(byIDItem{id: id})
	defer putByIDItem(k)
	got := dt.byID.Delete(k)
	if got == nil {
		return false
	}
	item := got.(*byIDItem)
	d := item.d
	putByIDItem(item)
	{
		k := getByNameItem(makeByNameItem(d))
		defer putByNameItem(k)
		// TODO(ajwerner): This would be very bad. It would imply that the name
		// has changed since this descriptor was added.
		got := dt.byName.Delete(k)
		if got != nil {
			putByNameItem(got.(*byNameItem))
		}
	}
	return true
}

func (dt *descriptorTree) getByID(id descpb.ID) (catalog.Descriptor, bool) {
	k := getByIDItem(byIDItem{id: id})
	defer putByIDItem(k)
	got := dt.byID.Get(k)
	if got == nil {
		return nil, false
	}
	return got.(*byIDItem).d, true
}

func (dt *descriptorTree) getByName(
	parentID, parentSchemaID descpb.ID, name string,
) (catalog.Descriptor, bool) {
	k := getByNameItem(byNameItem{
		parentID:       parentID,
		parentSchemaID: parentSchemaID,
		name:           name,
	})
	defer putByNameItem(k)
	got := dt.byName.Get(k)
	if got == nil {
		return nil, false
	}
	return got.(*byNameItem).d, true
}

func (dt *descriptorTree) len() int {
	return dt.byName.Len()
}

func (dt *descriptorTree) clear() {
	{
		dt.byName.Ascend(func(i btree.Item) bool {
			putByNameItem(i.(*byNameItem))
			return true
		})
		dt.byName.Clear(true /* addNodesToFreeList */)
	}
	{
		dt.byID.Ascend(func(i btree.Item) bool {
			putByIDItem(i.(*byIDItem))
			return true
		})
		dt.byID.Clear(true /* addNodesToFreeList */)
	}
}

// iterateFunc is used to iterate descriptors.
// If an error is returned, iteration is stopped and will be propagated
// up the stack. If the error is iterutil.StopIteration, iteration will
// stop but no error will be returned.
type iterateFunc func(descriptor catalog.Descriptor) error

func (dt *descriptorTree) iterateByID(f iterateFunc) error {
	var err error
	dt.byID.Ascend(func(i btree.Item) bool {
		err = f(i.(*byIDItem).d)
		return err == nil
	})
	if iterutil.Done(err) {
		err = nil
	}
	return err
}

type byIDItem struct {
	id descpb.ID
	d  catalog.Descriptor
}

func makeByIDItem(d catalog.Descriptor) byIDItem {
	return byIDItem{id: d.GetID(), d: d}
}

var _ btree.Item = (*byIDItem)(nil)

func (b *byIDItem) Less(thanItem btree.Item) bool {
	than := thanItem.(*byIDItem)
	return b.id < than.id
}

var byIDItemPool = sync.Pool{
	New: func() interface{} { return new(byIDItem) },
}

func getByIDItem(item byIDItem) *byIDItem {
	b := byIDItemPool.Get().(*byIDItem)
	*b = item
	return b
}

func putByIDItem(b *byIDItem) {
	*b = byIDItem{}
	byIDItemPool.Put(b)
}

type byNameItem struct {
	parentID, parentSchemaID descpb.ID
	name                     string
	d                        catalog.Descriptor
}

func makeByNameItem(d catalog.Descriptor) byNameItem {
	return byNameItem{
		parentID:       d.GetParentID(),
		parentSchemaID: d.GetParentSchemaID(),
		name:           d.GetName(),
		d:              d,
	}
}

var _ btree.Item = (*byNameItem)(nil)

func (b *byNameItem) Less(thanItem btree.Item) bool {
	than := thanItem.(*byNameItem)
	if b.parentID != than.parentID {
		return b.parentID < than.parentID
	}
	if b.parentSchemaID != than.parentSchemaID {
		return b.parentSchemaID < than.parentSchemaID
	}
	return b.name < than.name
}

var byNameItemPool = sync.Pool{
	New: func() interface{} { return new(byNameItem) },
}

func getByNameItem(item byNameItem) *byNameItem {
	b := byNameItemPool.Get().(*byNameItem)
	*b = item
	return b
}

func putByNameItem(b *byNameItem) {
	*b = byNameItem{}
	byNameItemPool.Put(b)
}
