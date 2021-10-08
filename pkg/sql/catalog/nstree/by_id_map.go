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
	removed, _ = delete(t.t, byIDItem{id: id}.get()).(catalog.NameEntry)
	return removed
}

func (t byIDMap) clear() {
	clear(t.t)
}

func (t byIDMap) ascend(f EntryIterator) error {
	return ascend(t.t, func(k interface{}) error {
		return f(k.(catalog.NameEntry))
	})
}
