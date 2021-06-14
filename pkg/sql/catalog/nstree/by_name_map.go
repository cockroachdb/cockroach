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

func (t byNameMap) delete(d catalog.NameEntry) {
	delete(t.t, makeByNameItem(d).get())
}

func (t byNameMap) clear() {
	clear(t.t)
}
