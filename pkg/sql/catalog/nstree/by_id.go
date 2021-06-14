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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/google/btree"
)

func (t byIDMap) len() int {
	return t.t.Len()
}

type byIDItem struct {
	id descpb.ID
	v  interface{}
}

func makeByIDItem(d interface{ GetID() descpb.ID }) byIDItem {
	return byIDItem{id: d.GetID(), v: d}
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

func (b *byIDItem) value() interface{} {
	return b.v
}

func (b *byIDItem) put() {
	*b = byIDItem{}
	byIDItemPool.Put(b)
}
