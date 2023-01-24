// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package interval

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/util/btree/internal/aug"
)

// Iterator is used to search and traverse the tree.
//
// The tree must not be modified during iteration. If concurrent modifications
// are required, take a Clone of the tree before constructing an Iterator.
type Iterator[C Config[I, K], I, K, V any] struct {
	it aug.Iterator[C, I, V, upperBound[K]]
	o  overlapScan[C, I, K, V]
}

// First seeks to the first key in the tree.
func (it *Iterator[C, I, K, V]) First() { it.it.First() }

// Last seeks to the last key in the tree.
func (it *Iterator[C, I, K, V]) Last() { it.it.Last() }

// Next positions the Iterator to the key immediately following
// its current position.
func (it *Iterator[C, I, K, V]) Next() { it.it.Next() }

// Prev positions the Iterator to the key immediately preceding
// its current position.
func (it *Iterator[C, I, K, V]) Prev() { it.it.Prev() }

// SeekGT seeks to the first key greater than the provided key.
func (it *Iterator[C, I, K, V]) SeekGT(v I) { it.it.SeekGT(v) }

// SeekGE seeks to the first key greater-than or equal to the provided
// key.
func (it *Iterator[C, I, K, V]) SeekGE(v I) (eq bool) { return it.it.SeekGE(v) }

// SeekLT seeks to the first key less than the provided key.
func (it *Iterator[C, I, K, V]) SeekLT(v I) { it.it.SeekLT(v) }

// SeekLE seeks to the first key less than or equal to the provided key.
func (it *Iterator[C, I, K, V]) SeekLE(v I) (eq bool) { return it.it.SeekLE(v) }

// Cur returns the key at the Iterator's current position. It is illegal
// to call Key if the Iterator is not valid.
func (it *Iterator[C, I, K, V]) Cur() I { return it.it.Cur() }

// Value returns the value at the Iterator's current position. It is illegal
// to call Value if the Iterator is not valid.
func (it *Iterator[C, I, K, V]) Value() V { return it.it.Value() }

// Valid returns true if the iterator is positioned on some element of the tree.
func (it *Iterator[C, I, K, V]) Valid() bool { return it.it.Valid() }

// Reset marks the iterator as invalid and clears any state.
func (it *Iterator[C, I, K, V]) Reset() { it.o.reset(); it.it.Reset() }

// FirstOverlap seeks to the first latch in the abstract that overlaps with the
// provided search latch.
func (it *Iterator[C, I, K, V]) FirstOverlap(bounds I) {
	it.Reset()
	if it.it.Node == nil {
		return
	}
	it.it.Pos++
	it.o = overlapScan[C, I, K, V]{}
	it.constrainMinSearchBounds(bounds)
	it.constrainMaxSearchBounds(bounds)
	it.findNextOverlap(bounds)
}

// NextOverlap positions the iterator to the latch immediately following
// its current position that overlaps with the search latch.
func (it *Iterator[C, I, K, V]) NextOverlap(item I) {
	if it.it.Node == nil {
		return
	}
	it.it.Pos++
	it.findNextOverlap(item)
}

// An overlap scan is a scan over all latches that overlap with the provided
// latch in order of the overlapping latches' start keys. The goal of the scan
// is to minimize the number of key comparisons performed in total. The
// algorithm operates based on the following two invariants maintained by
// augmented interval abstract:
//  1. all latches are sorted in the abstract based on their start key.
//  2. all abstract nodes maintain the upper bound end key of all latches
//     in their subtree.
//
// The scan algorithm starts in "unconstrained minimum" and "unconstrained
// maximum" states. To enter a "constrained minimum" state, the scan must reach
// latches in the tree with start keys above the search range's start key.
// Because latches in the tree are sorted by start key, once the scan enters the
// "constrained minimum" state it will remain there. To enter a "constrained
// maximum" state, the scan must determine the first child abstract node in a given
// subtree that can have latches with start keys above the search range's end
// key. The scan then remains in the "constrained maximum" state until it
// traverse into this child node, at which point it moves to the "unconstrained
// maximum" state again.
//
// The scan algorithm works like a standard abstract forward scan with the
// following augmentations:
//  1. before traversing the tree, the scan performs a binary search on the
//     root node's items to determine a "soft" lower-bound constraint position
//     and a "hard" upper-bound constraint position in the root's children.
//  2. when traversing into a child node in the lower or upper bound constraint
//     position, the constraint is refined by searching the child's items.
//  3. the initial traversal down the tree follows the left-most children
//     whose upper bound end keys are equal to or greater than the start key
//     of the search range. The children followed will be equal to or less
//     than the soft lower bound constraint.
//  4. once the initial traversal completes and the scan is in the left-most
//     abstract node whose upper bound overlaps the search range, key comparisons
//     must be performed with each latch in the tree. This is necessary because
//     any of these latches may have end keys that cause them to overlap with the
//     search range.
//  5. once the scan reaches the lower bound constraint position (the first latch
//     with a start key equal to or greater than the search range's start key),
//     it can begin scanning without performing key comparisons. This is allowed
//     because all latches from this point forward will have end keys that are
//     greater than the search range's start key.
//  6. once the scan reaches the upper bound constraint position, it terminates.
//     It does so because the latch at this position is the first latch with a
//     start key larger than the search range's end key.
type overlapScan[C Config[I, K], I, K, V any] struct {
	// The "soft" lower-bound constraint.
	constrMinN       *aug.Node[C, I, V, upperBound[K]]
	constrMinPos     int16
	constrMinReached bool

	// The "hard" upper-bound constraint.
	constrMaxN   *aug.Node[C, I, V, upperBound[K]]
	constrMaxPos int16
}

func (o *overlapScan[C, I, K, V]) reset() {
	*o = overlapScan[C, I, K, V]{}
}

func (it *Iterator[C, I, K, V]) constrainMinSearchBounds(item I) {
	var cmp C
	k := cmp.Key(item)
	n := it.it.Node
	j := sort.Search(int(n.Count), func(j int) bool {
		return cmp.ComparePoints(k, cmp.Key(n.Keys[j])) <= 0
	})
	it.o.constrMinN = n
	it.o.constrMinPos = int16(j)
}

func (it *Iterator[C, I, K, V]) constrainMaxSearchBounds(item I) {
	up := ub[C, I, K](item)
	n := it.it.Node
	j := sort.Search(int(n.Count), func(j int) bool {
		return !contains[C, I](up, key[C, I, K](n.Keys[j]))
	})
	it.o.constrMaxN = n
	it.o.constrMaxPos = int16(j)
}

func (it *Iterator[C, I, K, V]) findNextOverlap(item I) {
	for {
		if it.it.Pos > it.it.Node.Count {
			// Iterate up tree.
			it.it.Ascend()
		} else if it.it.Node.Children != nil {
			// Iterate down tree.
			if it.o.constrMinReached ||
				contains[C, I](
					it.it.Node.Children[it.it.Pos].Aug,
					key[C, I, K](item),
				) {

				par, pos := it.it.Node, it.it.Pos
				it.it.Descend(par, pos)

				// Refine the constraint bounds, if necessary.
				if par == it.o.constrMinN && pos == it.o.constrMinPos {
					it.constrainMinSearchBounds(item)
				}
				if par == it.o.constrMaxN && pos == it.o.constrMaxPos {
					it.constrainMaxSearchBounds(item)
				}
				continue
			}
		}

		// Check search bounds.
		n, pos := it.it.Node, it.it.Pos
		if n == it.o.constrMaxN && pos == it.o.constrMaxPos {
			// Invalid. Past possible overlaps.
			it.Reset()
			return
		}
		if n == it.o.constrMinN && pos == it.o.constrMinPos {
			// The scan reached the soft lower-bound constraint.
			it.o.constrMinReached = true
		}

		// Iterate across node.
		if pos < n.Count {
			// Check for overlapping latch.
			if it.o.constrMinReached {
				// Fast-path to avoid span comparison. it.o.constrMinReached
				// tells us that all latches have end keys above our search
				// span's start key.
				return
			}
			b := ub[C, I, K](it.it.Cur())
			if contains[C, I](b, key[C, I, K](item)) {
				return
			}
		}
		it.it.Pos++
	}
}
