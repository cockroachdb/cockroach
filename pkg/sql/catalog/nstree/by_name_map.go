// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nstree

import (
	"cmp"
	"sync"

	"github.com/RaduBerinde/btreemap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
)

type byNameMap struct {
	t *btreemap.BTreeMap[byNameKey, catalog.NameEntry]
}

var byNameMapPool = sync.Pool{
	New: func() interface{} {
		return btreemap.New[byNameKey, catalog.NameEntry](degree, byNameKeyCmp)
	},
}

const degree = 8

type byNameKey struct {
	parentID       descpb.ID
	parentSchemaID descpb.ID
	name           string
}

func makeByNameKey(d catalog.NameKey) byNameKey {
	return byNameKey{
		parentID:       d.GetParentID(),
		parentSchemaID: d.GetParentSchemaID(),
		name:           d.GetName(),
	}
}

func byNameKeyCmp(a, b byNameKey) int {
	if c := cmp.Compare(a.parentID, b.parentID); c != 0 {
		return c
	}
	if c := cmp.Compare(a.parentSchemaID, b.parentSchemaID); c != 0 {
		return c
	}
	return cmp.Compare(a.name, b.name)
}

func (t byNameMap) upsert(d catalog.NameEntry) (replaced catalog.NameEntry) {
	_, replaced, _ = t.t.ReplaceOrInsert(makeByNameKey(d), d)
	return replaced
}

func (t byNameMap) getByName(parentID, parentSchemaID descpb.ID, name string) catalog.NameEntry {
	_, got, _ := t.t.Get(byNameKey{
		parentID:       parentID,
		parentSchemaID: parentSchemaID,
		name:           name,
	})
	return got
}

func (t byNameMap) delete(d catalog.NameKey) (removed catalog.NameEntry) {
	_, removed, _ = t.t.Delete(makeByNameKey(d))
	return removed
}

func (t byNameMap) clear() {
	t.t.Clear(true /* addNodesToFreeList */)
	byNameMapPool.Put(t.t)
}

func (t byNameMap) ascend(f EntryIterator) error {
	for _, e := range t.t.Ascend(btreemap.Min[byNameKey](), btreemap.Max[byNameKey]()) {
		if err := f(e); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

func (t byNameMap) ascendDatabases(f EntryIterator) error {
	stop := btreemap.LT(byNameKey{parentSchemaID: 1})
	for _, e := range t.t.Ascend(btreemap.Min[byNameKey](), stop) {
		if err := f(e); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

func (t byNameMap) ascendSchemasForDatabase(dbID descpb.ID, f EntryIterator) error {
	start := btreemap.GE(byNameKey{parentID: dbID})
	stop := btreemap.LT(byNameKey{parentID: dbID, parentSchemaID: 1})
	for _, e := range t.t.Ascend(start, stop) {
		if err := f(e); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

func (t byNameMap) initialized() bool {
	return t != byNameMap{}
}

func makeByNameMap() byNameMap {
	return byNameMap{t: byNameMapPool.Get().(*btreemap.BTreeMap[byNameKey, catalog.NameEntry])}
}
