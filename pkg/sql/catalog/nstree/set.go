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
	"github.com/google/btree"
)

// Set is a set of namespace keys.
type Set struct {
	t *btree.BTree
}

// MakeSet makes a Set of namespace keys.
func MakeSet() Set {
	const (
		degree       = 8 // arbitrary
		initialNodes = 1 // one per tree
	)
	freeList := btree.NewFreeList(initialNodes)
	return Set{
		t: btree.NewWithFreeList(degree, freeList),
	}
}

// Add will add the relevant namespace key to the set.
func (s *Set) Add(components catalog.NameKey) {
	item := makeByNameItem(components).get()
	item.v = item // the value needs to be non-nil
	upsert(s.t, item)
}

// Contains will test whether the relevant namespace key was added.
func (s *Set) Contains(components catalog.NameKey) bool {
	return get(s.t, makeByNameItem(components).get()) != nil
}

// Clear will clear the set.
func (s *Set) Clear() {
	clear(s.t)
}
