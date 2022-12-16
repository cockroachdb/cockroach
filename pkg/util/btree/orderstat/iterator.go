// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package orderstat

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/btree/internal/abstract"
)

// Iterator allows iteration through the collection. It offers all the usual
// iterator methods, plus it offers Rank() and SeekNth() which allow efficient
// rank operations.
//
// The tree must not be modified during iteration. If concurrent modifications
// are required, take a Clone of the tree before constructing an Iterator.
type Iterator[K, V any] struct {
	it abstract.Iterator[K, V, aug, updater[K, V]]
}

// First seeks to the first key in the tree.
func (it *Iterator[K, V]) First() { it.it.First() }

// Last seeks to the last key in the tree.
func (it *Iterator[K, V]) Last() { it.it.Last() }

// Next positions the Iterator to the key immediately following
// its current position.
func (it *Iterator[K, V]) Next() { it.it.Next() }

// Prev positions the Iterator to the key immediately preceding
// its current position.
func (it *Iterator[K, V]) Prev() { it.it.Prev() }

// SeekGT seeks to the first key greater than the provided key.
func (it *Iterator[K, V]) SeekGT(v K) { it.it.SeekGT(v) }

// SeekGTE seeks to the first key greater-than or equal to the provided
// key.
func (it *Iterator[K, V]) SeekGTE(v K) { it.it.SeekGTE(v) }

// SeekLT seeks to the first key less than the provided key.
func (it *Iterator[K, V]) SeekLT(v K) { it.it.SeekLT(v) }

// SeekLTE seeks to the first key less than or equal to the provided key.
func (it *Iterator[K, V]) SeekLTE(v K) { it.it.SeekLTE(v) }

// Cur returns the key at the Iterator's current position. It is illegal
// to call Key if the Iterator is not valid.
func (it *Iterator[K, V]) Cur() K { return it.it.Cur() }

// Value returns the value at the Iterator's current position. It is illegal
// to call Value if the Iterator is not valid.
func (it *Iterator[K, V]) Value() V { return it.it.Value() }

// Valid returns true if the iterator is positioned on some element of the tree.
func (it *Iterator[K, V]) Valid() bool { return it.it.Valid() }

// Reset marks the iterator as invalid and clears any state.
func (it *Iterator[K, V]) Reset() { it.it.Reset() }

// Rank returns the rank of the current iterator position. If the iterator
// is not valid, -1 is returned.
func (it *Iterator[K, V]) Rank() int {
	if !it.Valid() {
		return -1
	}
	// If this is the root, then we want to figure out how many children are
	// below the current point.

	// Otherwise, we need to go up to the current parent, calculate everything
	// less and then drop back down to the current node and add everything less.
	var before int
	if it.it.Depth() > 0 {
		it.it.Ascend()
		for i, parentPos := int16(0), it.it.Pos; i < parentPos; i++ {
			before += it.it.Node.Children[i].Aug.children
		}
		before += int(it.it.Pos)
		it.it.Descend(it.it.Node, it.it.Pos)
	}
	if !it.it.IsLeaf() {
		for i, pos := int16(0), it.it.Pos; i <= pos; i++ {
			before += it.it.Node.Children[i].Aug.children
		}
	}
	before += int(it.it.Pos)
	return before
}

// SeekNth seeks the iterator to the nth item in the collection (0-indexed).
func (it *Iterator[K, V]) SeekNth(nth int) {
	it.Reset()
	// Reset has bizarre semantics in that it initializes the iterator to
	// an invalid position (-1) at the root of the tree. IncrementPos moves it
	// to the first child and item of the
	it.it.Pos++
	n := 0
	for n <= nth {
		if it.it.IsLeaf() {
			// If we're in the leaf, then, by construction, we can find
			// the relevant position and seek to it in constant time.
			//
			// TODO(ajwerner): Add more invariant checking.
			it.it.Pos = int16(nth - n)
			return
		}
		a := it.it.Node.Children[it.it.Pos].Aug
		if n+a.children > nth {
			it.it.Descend(it.it.Node, it.it.Pos)
			continue
		}

		n += a.children
		switch {
		case n < nth:
			// Consume the current value, move on to the next one.
			n++
			it.it.Pos++
		case n == nth:
			return // found it
		default:
			onErrorf("invariant violated")
		}
	}
}

var onErrorf = func(format string, args ...interface{}) {
	panic(fmt.Errorf(format, args...))
}
