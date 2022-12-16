// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package abstract

import "strings"

// TODO(ajwerner): It'd be amazing to find a way to make this not a single
// compile-time constant.
const (
	Degree     = 16
	MaxEntries = 2*Degree - 1
	MinEntries = Degree - 1
)

// Map is an implementation of an augmented B-Tree.
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type Map[K, V, A any, U Updater[K, V, A]] struct {
	root *Node[K, V, A]
	*Config[K, V, A, U]
	length int
}

// MakeMap constructs a new Map.
func (c *Config[K, V, A, U]) MakeMap() Map[K, V, A, U] {
	return Map[K, V, A, U]{Config: c}
}

// Reset removes all items from the AugBTree. In doing so, it allows memory
// held by the AugBTree to be recycled. Failure to call this method before
// letting a AugBTree be GCed is safe in that it won't cause a memory leak,
// but it will prevent AugBTree nodes from being efficiently re-used.
func (t *Map[K, V, A, U]) Reset() {
	if t.root != nil {
		t.root.decRef(t.np, true /* recursive */)
		t.root = nil
	}
	t.length = 0
}

// Clone clones the AugBTree, lazily. It does so in constant time.
func (t *Map[K, V, A, U]) Clone() Map[K, V, A, U] {
	if t.root != nil {
		// Incrementing the reference Count on the root Node is sufficient to
		// ensure that no Node in the cloned tree can be mutated by an actor
		// holding a reference to the original tree and vice versa. This
		// property is upheld because the root Node in the receiver AugBTree and
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
		t.root.incRef()
	}
	return *t
}

// Len returns the number of items currently in the tree.
func (t *Map[K, V, A, U]) Len() int {
	return t.length
}

// Upsert adds the given item to the tree. If an item in the tree already equals
// the given one, it is replaced with the new item.
func (t *Map[K, V, A, U]) Upsert(item K, value V) (replacedK K, replacedV V, replaced bool) {
	if t.root == nil {
		t.root = t.np.getLeafNode()
	} else if t.root.Count >= MaxEntries {
		splitLaK, splitLaV, splitNode := split(
			mut(t.np, &t.root), t.Config, MaxEntries/2,
		)
		newRoot := t.np.getInteriorNode()
		newRoot.Count = 1
		newRoot.Keys[0] = splitLaK
		newRoot.Values[0] = splitLaV
		newRoot.Children[0] = t.root
		newRoot.Children[1] = splitNode
		t.Updater.UpdateDefault(newRoot)
		t.root = newRoot
	}
	replacedK, replacedV, replaced, _ = insert(
		mut(t.np, &t.root), t.Config, item, value,
	)
	if !replaced {
		t.length++
	}
	return replacedK, replacedV, replaced
}

// Delete removes an item equal to the passed in item from the tree.
func (t *Map[K, V, A, U]) Delete(k K) (removedK K, v V, found bool) {
	if t.root == nil || t.root.Count == 0 {
		return removedK, v, false
	}
	if removedK, v, found, _ = remove(
		mut(t.np, &t.root), t.Config, k,
	); found {
		t.length--
	}
	if t.root.Count == 0 {
		old := t.root
		if t.root.IsLeaf() {
			t.root = nil
		} else {
			t.root = t.root.Children[0]
		}
		old.decRef(t.np, false /* recursive */)
	}
	return removedK, v, found
}

// Get returns the value associated with the requested key, if it exists.
func (t *Map[K, V, A, U]) Get(k K) (got K, v V, ok bool) {
	it := t.makeIter()
	it.SeekGTE(k)
	if it.Valid() && it.Cmp(it.Cur(), k) == 0 {
		return it.Cur(), it.Value(), true
	}
	return got, v, false
}

// Contains returns true if the map contains the requested key.
func (t *Map[K, V, A, U]) Contains(k K) bool {
	it := t.makeIter()
	it.SeekGTE(k)
	return it.Valid() && it.Cmp(it.Cur(), k) == 0
}

// Iterator returns a new Iterator object. It is not safe to continue using an
// Iterator after modifications are made to the tree. If modifications are made,
// create a new Iterator.
func (t *Map[K, V, A, U]) makeIter() Iterator[K, V, A, U] {
	var it Iterator[K, V, A, U]
	t.InitIterator(&it)
	return it
}

func (t *Map[K, V, A, U]) InitIterator(it *Iterator[K, V, A, U]) {
	it.Config, it.root, it.Pos, it.Node = t.Config, t.root, -1, t.root
}

// String returns a string description of the tree. The format is
// similar to the https://en.wikipedia.org/wiki/Newick_format.
func (t *Map[K, V, A, U]) String() string {
	if t.length == 0 {
		return ";"
	}
	var b strings.Builder
	t.root.writeString(&b)
	return b.String()
}

// Height returns the height of the tree.
func (t *Map[K, V, A, U]) Height() int {
	if t.root == nil {
		return 0
	}
	h := 1
	n := t.root
	for !n.IsLeaf() {
		n = n.Children[0]
		h++
	}
	return h
}
