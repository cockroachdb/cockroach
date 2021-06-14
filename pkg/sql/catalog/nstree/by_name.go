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

type byNameItem struct {
	parentID, parentSchemaID descpb.ID
	name                     string
	v                        interface{}
}

func makeByNameItem(d catalog.NameKey) byNameItem {
	return byNameItem{
		parentID:       d.GetParentID(),
		parentSchemaID: d.GetParentSchemaID(),
		name:           d.GetName(),
		v:              d,
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

func (b *byNameItem) value() interface{} {
	return b.v
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
