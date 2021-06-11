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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/google/btree"
)

type byID struct {
	t *btree.BTree
}

func (t byID) upsert(d catalog.NameEntry) (replaced catalog.NameEntry) {
	return upsert(t.t, makeByIDItem(d).get())
}

func (t byID) get(id descpb.ID) (catalog.NameEntry, bool) {
	return get(t.t, byIDItem{id: id}.get())
}

func (t byID) delete(id descpb.ID) (removed catalog.NameEntry) {
	return delete(t.t, byIDItem{id: id}.get())
}

func (t byID) clear() {
	clear(t.t)
}

func (t byID) ascend(f Iterator) error {
	return ascend(t.t, f)
}

func (t byID) len() int {
	return t.t.Len()
}

type byIDItem struct {
	id descpb.ID
	d  catalog.NameEntry
}

func makeByIDItem(d catalog.NameEntry) byIDItem {
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

func (b byIDItem) get() *byIDItem {
	alloc := byIDItemPool.Get().(*byIDItem)
	*alloc = b
	return alloc
}

func (b *byIDItem) descriptor() catalog.NameEntry {
	return b.d
}

func (b *byIDItem) put() {
	*b = byIDItem{}
	byIDItemPool.Put(b)
}
