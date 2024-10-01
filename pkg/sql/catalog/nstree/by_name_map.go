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

type byNameMap struct {
	t *btree.BTree
}

func (t byNameMap) upsert(d catalog.NameEntry) (replaced catalog.NameEntry) {
	replaced, _ = upsert(t.t, makeByNameItem(d).get()).(catalog.NameEntry)
	return replaced
}

func (t byNameMap) getByName(parentID, parentSchemaID descpb.ID, name string) catalog.NameEntry {
	got, _ := get(t.t, byNameItem{
		parentID:       parentID,
		parentSchemaID: parentSchemaID,
		name:           name,
	}.get()).(catalog.NameEntry)
	return got
}

func (t byNameMap) delete(d catalog.NameKey) (removed catalog.NameEntry) {
	removed, _ = remove(t.t, makeByNameItem(d).get()).(catalog.NameEntry)
	return removed
}

func (t byNameMap) clear() {
	clear(t.t)
	btreeSyncPool.Put(t.t)
}

func (t byNameMap) ascend(f EntryIterator) error {
	return ascend(t.t, func(k interface{}) error {
		return f(k.(catalog.NameEntry))
	})
}

func (t byNameMap) ascendDatabases(f EntryIterator) error {
	min, max := byNameItem{}.get(), byNameItem{parentSchemaID: 1}.get()
	defer min.put()
	defer max.put()
	return ascendRange(t.t, min, max, func(k interface{}) error {
		return f(k.(catalog.NameEntry))
	})
}

func (t byNameMap) ascendSchemasForDatabase(dbID descpb.ID, f EntryIterator) error {
	min := byNameItem{
		parentID: dbID,
	}.get()
	max := byNameItem{
		parentID:       dbID,
		parentSchemaID: 1,
	}.get()
	defer min.put()
	defer max.put()
	return ascendRange(t.t, min, max, func(k interface{}) error {
		return f(k.(catalog.NameEntry))
	})
}

func (t byNameMap) initialized() bool {
	return t != byNameMap{}
}

func makeByNameMap() byNameMap {
	return byNameMap{t: btreeSyncPool.Get().(*btree.BTree)}
}
