// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nstree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/google/btree"
)

type byIDMap struct {
	t *btree.BTree
}

func (t byIDMap) upsert(d catalog.NameEntry) (replaced catalog.NameEntry) {
	replaced, _ = upsert(t.t, makeByIDItem(d).get()).(catalog.NameEntry)
	return replaced
}

func (t byIDMap) get(id descpb.ID) catalog.NameEntry {
	got, _ := get(t.t, byIDItem{id: id}.get()).(catalog.NameEntry)
	return got
}

func (t byIDMap) delete(id descpb.ID) (removed catalog.NameEntry) {
	removed, _ = remove(t.t, byIDItem{id: id}.get()).(catalog.NameEntry)
	return removed
}

func (t byIDMap) clear() {
	clear(t.t)
	btreeSyncPool.Put(t.t)
}

func (t byIDMap) ascend(f EntryIterator) error {
	return ascend(t.t, func(k interface{}) error {
		return f(k.(catalog.NameEntry))
	})
}

func (t byIDMap) initialized() bool {
	return t != byIDMap{}
}

func makeByIDMap() byIDMap {
	return byIDMap{t: btreeSyncPool.Get().(*btree.BTree)}
}
