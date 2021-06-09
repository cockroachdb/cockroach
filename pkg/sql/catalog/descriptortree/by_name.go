// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descriptortree

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/google/btree"
)

type byName struct {
	t *btree.BTree
}

func (t byName) upsert(d catalog.Descriptor) (replaced catalog.Descriptor) {
	return upsert(t.t, makeByNameItem(d).get())
}

func (t byName) getByName(
	parentID, parentSchemaID descpb.ID, name string,
) (catalog.Descriptor, bool) {
	return get(t.t, byNameItem{
		parentID:       parentID,
		parentSchemaID: parentSchemaID,
		name:           name,
	}.get())
}

func (t byName) delete(d catalog.Descriptor) {
	delete(t.t, makeByNameItem(d).get())
}

func (t byName) clear() {
	clear(t.t)
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

func (b *byNameItem) descriptor() catalog.Descriptor {
	return b.d
}

var byNameItemPool = sync.Pool{
	New: func() interface{} { return new(byNameItem) },
}

func (b byNameItem) get() *byNameItem {
	item := byNameItemPool.Get().(*byNameItem)
	*item = b
	return item
}

func (b *byNameItem) put() {
	*b = byNameItem{}
	byNameItemPool.Put(b)
}
