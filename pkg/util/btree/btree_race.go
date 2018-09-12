// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// +build race

package btree

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	googlebtree "github.com/google/btree"
)

// BTree is a wrapper around googlebtree.BTree that checks for incorrect usage.
type BTree struct {
	*googlebtree.BTree
}

// New creates a new B-tree with the given degree.
func New(degree int) *BTree {
	return &BTree{googlebtree.New(degree)}
}

// Delete removes an item equal to the passed in item from the tree, returning
// it. If no such item exists, it returns nil.
func (b *BTree) Delete(item Item) Item {
	// In #30110 it was observed that duplicate items were getting silently
	// inserted into the B-tree. Duplicates should be impossible, because the only
	// way to insert an item into the B-tree is via the ReplaceOrInsert method. It
	// is, however, possible to get around this by inserting item I1 with key K1,
	// inserting another item I2 with key K2, and then *mutating* I1's key to K2.
	// The fact that we regularly mutate items' keys while they are in the B-tree
	// is horrible but requires a rather large refactor to fix (#30125).
	//
	// For now, the best we can do is detect this situation when we go to remove
	// an item. If Get(item) returns anything but nil after Delete(item), we know
	// we have a duplicate.
	i1 := b.BTree.Delete(item)
	if i2 := b.Get(item); i2 != nil {
		log.Fatalf(context.TODO(), "duplicate items in BTree at same key: %s and %s", i1, i2)
	}
	return i1
}

// Item is googlebtree.Item.
type Item = googlebtree.Item
