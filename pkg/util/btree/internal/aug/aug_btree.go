// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package aug

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/btree/ordered"
)

// TODO(ajwerner): It'd be amazing to find a way to make this not a single
// compile-time constant.
const (
	degree     = 16
	MaxEntries = 2*degree - 1
	MinEntries = degree - 1
)

// BTree is an implementation of an augmented B-Tree.
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type BTree[
	C ordered.Comparator[K],
	K, V,
	A any, U Updater[C, K, V, A],
] struct {
	Root *Node[C, K, V, A]
	config[C, K, V, A, U]
	length int
}

// Compare can be used to compare keys the same was as the map.
func (t *BTree[C, K, V, A, U]) Compare(a, b K) int {
	return t.cmp.Compare(a, b)
}

// MakeMap is used to construct a map with a custom comparator.
// Note that the zero value of a BTree is usable if the zero value of the
// Comparator is usable.
func MakeMap[
	C ordered.Comparator[K],
	K, V, A any,
	U Updater[C, K, V, A],
](c C) BTree[C, K, V, A, U] {
	m := BTree[C, K, V, A, U]{}
	m.cmp = c
	return m
}

// Reset removes all items from the AugBTree. In doing so, it allows memory
// held by the AugBTree to be recycled. Failure to call this method before
// letting a AugBTree be GCed is safe in that it won't cause a memory leak,
// but it will prevent AugBTree nodes from being efficiently re-used.
func (t *BTree[C, K, V, A, U]) Reset() {
	if t.Root != nil {
		t.decRef(t.Root, true /* recursive */)
		t.Root = nil
	}
	t.length = 0
}

// Clone clones the AugBTree, lazily. It does so in constant time.
func (t *BTree[C, K, V, A, U]) Clone() BTree[C, K, V, A, U] {
	if t.Root != nil {
		// Incrementing the reference Count on the Root Node is sufficient to
		// ensure that no Node in the cloned tree can be mutated by an actor
		// holding a reference to the original tree and vice versa. This
		// property is upheld because the Root Node in the receiver AugBTree and
		// the returned AugBTree will both necessarily have a reference Count of at
		// least 2 when this method returns. All tree mutations recursively
		// acquire mutable Node references (see mut) as they traverse down the
		// tree. The act of acquiring a mutable Node reference performs a clone
		// if a Node's reference Count is greater than one. Cloning a Node (see
		// clone) increases the reference Count on each of its Children,
		// ensuring that they have a reference Count of at least 2. This, in
		// turn, ensures that any of the child nodes that are modified will also
		// be copied-on-write, recursively ensuring the immutability property
		// over the entire tree.
		t.Root.incRef()
	}
	return *t
}

// Len returns the number of items currently in the tree.
func (t *BTree[C, K, V, A, U]) Len() int {
	return t.length
}

// Upsert adds the given item to the tree. If an item in the tree already equals
// the given one, it is replaced with the new item.
func (t *BTree[C, K, V, A, U]) Upsert(item K, value V) (replacedK K, replacedV V, replaced bool) {
	if t.Root == nil {
		if t.nodePool == nil {
			t.nodePool = getNodePool[C, K, V, A]()
		}
		t.Root = t.getLeafNode()
	} else if t.Root.Count >= MaxEntries {
		splitLaK, splitLaV, splitNode := t.split(t.mut(&t.Root), MaxEntries/2)
		newRoot := t.getInteriorNode()
		newRoot.Count = 1
		newRoot.Keys[0] = splitLaK
		newRoot.Values[0] = splitLaV
		newRoot.Children[0] = t.Root
		newRoot.Children[1] = splitNode
		var u U
		u.UpdateDefault(newRoot)
		t.Root = newRoot
	}
	replacedK, replacedV, replaced, _ = t.insert(t.mut(&t.Root), item, value)
	if !replaced {
		t.length++
	}
	return replacedK, replacedV, replaced
}

// Delete removes an item equal to the passed in item from the tree.
func (t *BTree[C, K, V, A, U]) Delete(k K) (removedK K, v V, found bool) {
	if t.Root == nil || t.Root.Count == 0 {
		return removedK, v, false
	}
	if removedK, v, found, _ = t.remove(t.mut(&t.Root), k); found {
		t.length--
	}
	if t.Root.Count == 0 {
		old := t.Root
		if t.Root.IsLeaf() {
			t.Root = nil
		} else {
			t.Root = t.Root.Children[0]
		}
		t.decRef(old, false /* recursive */)
	}
	return removedK, v, found
}

// Get returns the value associated with the requested key, if it exists.
func (t *BTree[C, K, V, A, U]) Get(k K) (got K, v V, ok bool) {
	if it := t.makeIter(); it.SeekGE(k) {
		return it.Cur(), it.Value(), true
	}
	return got, v, false
}

// Contains returns true if the map contains the requested key.
func (t *BTree[C, K, V, A, U]) Contains(k K) bool {
	it := t.makeIter()
	return it.SeekGE(k)
}

// Iterator returns a new Iterator object. It is not safe to continue using an
// Iterator after modifications are made to the tree. If modifications are made,
// create a new Iterator.
func (t *BTree[C, K, V, A, U]) makeIter() Iterator[C, K, V, A] {
	var it Iterator[C, K, V, A]
	t.InitIterator(&it)
	return it
}

func (t *BTree[C, K, V, A, U]) InitIterator(it *Iterator[C, K, V, A]) {
	it.root, it.Pos, it.Node = t.Root, -1, t.Root
}

// String returns a string description of the tree. The format is
// similar to the https://en.wikipedia.org/wiki/Newick_format.
func (t *BTree[C, K, V, A, U]) String() string {
	if t.length == 0 {
		return ";"
	}
	var b strings.Builder
	t.Root.writeString(&b)
	return b.String()
}
