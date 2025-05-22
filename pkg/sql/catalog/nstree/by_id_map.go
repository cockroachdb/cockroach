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

type byIDMap struct {
	t *btreemap.BTreeMap[descpb.ID, catalog.NameEntry]
}

var byIDMapPool = sync.Pool{
	New: func() interface{} {
		return btreemap.New[descpb.ID, catalog.NameEntry](degree, cmp.Compare[descpb.ID])
	},
}

func (t byIDMap) upsert(d catalog.NameEntry) (replaced catalog.NameEntry) {
	_, replaced, _ = t.t.ReplaceOrInsert(d.GetID(), d)
	return replaced
}

func (t byIDMap) get(id descpb.ID) catalog.NameEntry {
	_, result, _ := t.t.Get(id)
	return result
}

func (t byIDMap) delete(id descpb.ID) (removed catalog.NameEntry) {
	_, removed, _ = t.t.Delete(id)
	return removed
}

func (t byIDMap) clear() {
	t.t.Clear(true /* addNodesToFreeList */)
	byIDMapPool.Put(t.t)
}

func (t byIDMap) len() int {
	return t.t.Len()
}

func (t byIDMap) ascend(f EntryIterator) error {
	for _, e := range t.t.Ascend(btreemap.Min[descpb.ID](), btreemap.Max[descpb.ID]()) {
		if err := f(e); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

func (t byIDMap) initialized() bool {
	return t != byIDMap{}
}

func makeByIDMap() byIDMap {
	return byIDMap{t: byIDMapPool.Get().(*btreemap.BTreeMap[descpb.ID, catalog.NameEntry])}
}
