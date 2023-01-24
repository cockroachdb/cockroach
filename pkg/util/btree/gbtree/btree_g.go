// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package gbtree contains an adapter type to give btree.Set the same interface
// as github.com/google/btree.BTreeG.
package gbtree

import (
	"github.com/cockroachdb/cockroach/pkg/util/btree"
	"github.com/cockroachdb/cockroach/pkg/util/btree/ordered"
)

// ItemIteratorG is used to iterate items in the tree.
type ItemIteratorG[T any] func(item T) bool

// BTreeG wraps btree.Set to behave like google/btree.BTreeG.
type BTreeG[T any] struct {
	s btree.Set[btree.WithFunc[T], T]
}

// New constructs a new BTreeG.
func New[T any](fn ordered.Func[T]) *BTreeG[T] {
	return &BTreeG[T]{s: btree.MakeSetWithFunc(fn)}
}

// Ascend calls the iterator for every value in the tree within the range
// [first, last], until iterator returns false.
func (a *BTreeG[T]) Ascend(iterator ItemIteratorG[T]) {
	it := a.s.MakeIter()
	it.First()
	for ; it.Valid(); it.Next() {
		if !iterator(it.Cur()) {
			return
		}
	}
}

// AscendGreaterOrEqual calls the iterator for every value in the tree within
// the range [pivot, last], until iterator returns false.
func (a *BTreeG[T]) AscendGreaterOrEqual(pivot T, iterator ItemIteratorG[T]) {
	it := a.s.MakeIter()
	it.SeekGE(pivot)
	for ; it.Valid(); it.Next() {
		if !iterator(it.Cur()) {
			return
		}
	}
}

// AscendLessThan calls the iterator for every value in the tree within the
// range [first, pivot), until iterator returns false.
func (a *BTreeG[T]) AscendLessThan(pivot T, iterator ItemIteratorG[T]) {
	it := a.s.MakeIter()
	it.First()
	for ; it.Valid(); it.Next() {
		if a.s.Compare(it.Cur(), pivot) >= 0 || !iterator(it.Cur()) {
			return
		}
	}
}

// AscendRange calls the iterator for every value in the tree within the range
// [greaterOrEqual, lessThan), until iterator returns false.
func (a *BTreeG[T]) AscendRange(greaterOrEqual, lessThan T, iterator ItemIteratorG[T]) {
	it := a.s.MakeIter()
	it.SeekGE(greaterOrEqual)
	for ; it.Valid(); it.Next() {
		if a.s.Compare(it.Cur(), lessThan) >= 0 || !iterator(it.Cur()) {
			return
		}
	}
}

// Clear removes all items from the btree. The boolean parameter is ignored.
//
// This can be much faster than calling Delete on all elements, because that
// requires finding/removing each element in the tree and updating the tree
// accordingly. It also is somewhat faster than creating a new tree to replace
// the old one, because nodes from the old tree are reclaimed into the freelist
// for use by the new one, instead of being lost to the garbage collector.
func (a *BTreeG[T]) Clear(_ bool) { a.s.Reset() }

// Clone clones the btree, lazily. Clone should not be called concurrently, but
// the original tree (t) and the new tree (t2) can be used concurrently once
// the Clone call completes.
//
// The internal tree structure of b is marked read-only and shared between t
// and t2. Writes to both t and t2 use copy-on-write logic, creating new nodes
// whenever one of b's original nodes would have been modified. Read operations
// should have no performance degredation. Write operations for both t and t2
// will initially experience minor slow-downs caused by additional allocs and
// copies due to the aforementioned copy-on-write logic, but should converge to
// the original performance characteristics of the original tree.
func (a *BTreeG[T]) Clone() (t2 *BTreeG[T]) {
	return &BTreeG[T]{s: a.s.Clone()}
}

// Delete removes an item equal to the passed in item from the tree, returning
// it. If no such item exists, returns (zeroValue, false).
func (a *BTreeG[T]) Delete(item T) (T, bool) {
	return a.s.Delete(item)
}

// DeleteMax removes the largest item in the tree and returns it. If no such
// item exists, returns (zeroValue, false).
func (a *BTreeG[T]) DeleteMax() (T, bool) {
	it := a.s.MakeIter()
	it.Last()
	if !it.Valid() {
		var zero T
		return zero, false
	}
	return a.s.Delete(it.Cur())
}

// DeleteMin removes the smallest item in the tree and returns it. If no such
// item exists, returns (zeroValue, false).
func (a *BTreeG[T]) DeleteMin() (T, bool) {
	it := a.s.MakeIter()
	it.First()
	if !it.Valid() {
		var zero T
		return zero, false
	}
	return a.s.Delete(it.Cur())
}

// Descend calls the iterator for every value in the tree within the range
// [last, first], until iterator returns false.
func (a *BTreeG[T]) Descend(iterator ItemIteratorG[T]) {
	it := a.s.MakeIter()
	it.Last()
	for ; it.Valid(); it.Prev() {
		if !iterator(it.Cur()) {
			return
		}
	}
}

// DescendGreaterThan calls the iterator for every value in the tree within the
// range [last, pivot), until iterator returns false.
func (a *BTreeG[T]) DescendGreaterThan(pivot T, iterator ItemIteratorG[T]) {
	it := a.s.MakeIter()
	it.Last()
	for ; it.Valid(); it.Prev() {
		if a.s.Compare(it.Cur(), pivot) <= 0 || !iterator(it.Cur()) {
			return
		}
	}
}

// DescendLessOrEqual calls the iterator for every value in the tree within the
// range [pivot, first], until iterator returns false.
func (a *BTreeG[T]) DescendLessOrEqual(pivot T, iterator ItemIteratorG[T]) {
	it := a.s.MakeIter()
	it.SeekLE(pivot)
	for ; it.Valid(); it.Prev() {
		if !iterator(it.Cur()) {
			return
		}
	}
}

// DescendRange calls the iterator for every value in the tree within the range
// [lessOrEqual, greaterThan), until iterator returns false.
func (a *BTreeG[T]) DescendRange(lessOrEqual, greaterThan T, iterator ItemIteratorG[T]) {
	it := a.s.MakeIter()
	it.SeekLE(lessOrEqual)
	for ; it.Valid(); it.Prev() {
		if a.s.Compare(it.Cur(), greaterThan) <= 0 || !iterator(it.Cur()) {
			return
		}
	}
}

// Get looks for the key item in the tree, returning it. It returns
// (zeroValue, false) if unable to find that item.
func (a *BTreeG[T]) Get(key T) (T, bool) {
	return a.s.Get(key)
}

// Has returns true if the given key is in the tree.
func (a *BTreeG[T]) Has(key T) bool {
	return a.s.Contains(key)
}

// Len returns the number of items currently in the tree.
func (a *BTreeG[T]) Len() int {
	return a.s.Len()
}

// Max returns the largest item in the tree, or (zeroValue, false) if the
// tree is empty.
func (a *BTreeG[T]) Max() (T, bool) {
	it := a.s.MakeIter()
	it.Last()
	if it.Valid() {
		return it.Cur(), true
	}
	var zero T
	return zero, false
}

// Min returns the smallest item in the tree, or (zeroValue, false) if the
// tree is empty.
func (a *BTreeG[T]) Min() (T, bool) {
	it := a.s.MakeIter()
	it.First()
	if it.Valid() {
		return it.Cur(), true
	}
	var zero T
	return zero, false
}

// ReplaceOrInsert adds the given item to the tree. If an item in the tree
// already equals the given one, it is removed from the tree and returned,
// and the second return value is true. Otherwise, (zeroValue, false).
func (a *BTreeG[T]) ReplaceOrInsert(item T) (T, bool) {
	return a.s.Upsert(item)
}
