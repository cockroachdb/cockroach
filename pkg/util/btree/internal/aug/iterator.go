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

import "github.com/cockroachdb/cockroach/pkg/util/btree/ordered"

// Iterator is responsible for search and traversal within a AugBTree.
type Iterator[C ordered.Comparator[K], K, V, A any] struct {
	root *Node[C, K, V, A]
	iterFrame[C, K, V, A]
	s iterStack[C, K, V, A]
}

// Reset marks the iterator as invalid and clears any state.
func (i *Iterator[C, K, V, A]) Reset() {
	i.Node = i.root
	i.Pos = -1
	i.s.reset()
}

// SeekGT seeks to the first key greater than the provided key.
func (i *Iterator[C, K, V, A]) SeekGT(key K) {
	if found := i.SeekGE(key); found {
		i.Next()
	}
}

// SeekGE seeks to the first key greater-than or equal to the provided
// key.
func (i *Iterator[C, K, V, A]) SeekGE(key K) (eq bool) {
	i.Reset()
	if i.Node == nil {
		return false
	}
	for {
		pos, found := i.Node.find(key)
		i.Pos = int16(pos)
		if found {
			return found
		}
		if i.Node.Children == nil {
			if i.Pos == i.Node.Count {
				i.Next()
			}
			return false
		}
		i.Descend(i.Node, i.Pos)
	}
}

// SeekLT seeks to the first key less than the provided key.
func (i *Iterator[C, K, V, A]) SeekLT(key K) {
	if found := i.SeekLE(key); found {
		i.Prev()
	}
}

// SeekLE seeks to the first key less than or equal to the provided key.
func (i *Iterator[C, K, V, A]) SeekLE(key K) (eq bool) {
	i.Reset()
	if i.Node == nil {
		return false
	}
	for {
		pos, found := i.Node.find(key)
		i.Pos = int16(pos)
		if !found && i.Node.Children != nil {
			i.Descend(i.Node, i.Pos)
			continue
		}
		if !found {
			i.Prev()
		}
		return found
	}
}

// First seeks to the first key in the tree.
func (i *Iterator[C, K, V, A]) First() {
	i.Reset()
	if i.Node == nil {
		return
	}
	for i.Node.Children != nil {
		i.Descend(i.Node, 0)
	}
	i.Pos = 0
}

// Last seeks to the last key in the tree.
func (i *Iterator[C, K, V, A]) Last() {
	i.Reset()
	if i.Node == nil {
		return
	}
	for i.Node.Children != nil {
		i.Descend(i.Node, i.Node.Count)
	}
	i.Pos = i.Node.Count - 1
}

// Next positions the Iterator to the key immediately following
// its current position.
func (i *Iterator[C, K, V, A]) Next() {
	if i.Node == nil {
		return
	}
	if i.Node.Children == nil {
		i.Pos++
		if i.Pos < i.Node.Count {
			return
		}
		for i.s.len() > 0 && i.Pos >= i.Node.Count {
			i.Ascend()
		}
		return
	}

	i.Descend(i.Node, i.Pos+1)
	for i.Node.Children != nil {
		i.Descend(i.Node, 0)
	}
	i.Pos = 0
}

// Prev positions the Iterator to the key immediately preceding
// its current position.
func (i *Iterator[C, K, V, A]) Prev() {
	if i.Node == nil {
		return
	}
	if i.Node.Children == nil {
		i.Pos--
		if i.Pos >= 0 {
			return
		}
		for i.s.len() > 0 && i.Pos < 0 {
			i.Ascend()
			i.Pos--
		}
		return
	}

	i.Descend(i.Node, i.Pos)
	for i.Node.Children != nil {
		i.Descend(i.Node, i.Node.Count)
	}
	i.Pos = i.Node.Count - 1
}

// Cur returns the key at the Iterator's current position. It is illegal
// to call Key if the Iterator is not valid.
func (i *Iterator[C, K, V, A]) Cur() K {
	return i.Node.Keys[i.Pos]
}

// Value returns the value at the Iterator's current position. It is illegal
// to call Value if the Iterator is not valid.
func (i *Iterator[C, K, V, A]) Value() V {
	return i.Node.Values[i.Pos]
}

// Valid returns true if the iterator is positioned on some element of the tree.
func (i *Iterator[C, K, V, A]) Valid() bool {
	return i.Pos >= 0 && i.Pos < i.Node.Count
}

// IsLeaf returns true if the current Node is a leaf.
func (i *Iterator[C, K, V, A]) IsLeaf() bool {
	return i.Node.IsLeaf()
}

// Depth returns the number of nodes above the current Node in the stack.
// It is illegal to call Ascend if this function returns 0.
func (i *Iterator[C, K, V, A]) Depth() int {
	return i.s.len()
}

// Descend pushes the current position into the iterators stack and
// descends into the child Node currently pointed to by the iterator.
// It is illegal to call if there is no such child. The position in the
// new Node will be 0.
func (i *Iterator[C, K, V, A]) Descend(n *Node[C, K, V, A], pos int16) {
	i.s.push(iterFrame[C, K, V, A]{Node: n, Pos: pos})
	i.iterFrame = iterFrame[C, K, V, A]{Node: n.Children[pos], Pos: 0}
}

// Ascend ascends up to the current Node's parent and resets the position
// to the one previously set for this parent Node.
func (i *Iterator[C, K, V, A]) Ascend() {
	i.iterFrame = i.s.pop()
}

func (i *Iterator[C, K, V, A]) makeFrame(n *Node[C, K, V, A], pos int16) iterFrame[C, K, V, A] {
	return iterFrame[C, K, V, A]{Node: n, Pos: pos}
}
