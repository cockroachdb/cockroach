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

// Iterator is responsible for search and traversal within a AugBTree.
type Iterator[K, V, A any, U Updater[K, V, A]] struct {
	root *Node[K, V, A]
	*Config[K, V, A, U]
	iterFrame[K, V, A]
	s iterStack[K, V, A]
}

// Reset marks the iterator as invalid and clears any state.
func (i *Iterator[K, V, A, U]) Reset() {
	i.Node = i.root
	i.Pos = -1
	i.s.reset()
}

// SeekGT seeks to the first key greater than the provided key.
func (i *Iterator[K, V, A, U]) SeekGT(key K) {
	i.Reset()
	if i.Node == nil {
		return
	}
	for {
		pos, found := i.Node.find(i.Cmp, key)
		i.Pos = int16(pos)
		if found {
			i.Next()
			return
		}
		if i.Node.IsLeaf() {
			if i.Pos == i.Node.Count {
				i.Next()
			}
			return
		}
		i.Descend(i.Node, i.Pos)
	}
}

// SeekGTE seeks to the first key greater-than or equal to the provided
// key.
func (i *Iterator[K, V, A, U]) SeekGTE(key K) {
	i.Reset()
	if i.Node == nil {
		return
	}
	for {
		pos, found := i.Node.find(i.Cmp, key)
		i.Pos = int16(pos)
		if found {
			return
		}
		if i.Node.IsLeaf() {
			if i.Pos == i.Node.Count {
				i.Next()
			}
			return
		}
		i.Descend(i.Node, i.Pos)
	}
}

// SeekLT seeks to the first key less than the provided key.
func (i *Iterator[K, V, A, U]) SeekLT(key K) {
	i.Reset()
	if i.Node == nil {
		return
	}
	for {
		pos, found := i.Node.find(i.Cmp, key)
		i.Pos = int16(pos)
		if found || i.Node.IsLeaf() {
			i.Prev()
			return
		}
		i.Descend(i.Node, i.Pos)
	}
}

// SeekLTE seeks to the first key less than or equal to the provided key.
func (i *Iterator[K, V, A, U]) SeekLTE(key K) {
	i.Reset()
	if i.Node == nil {
		return
	}
	for {
		pos, found := i.Node.find(i.Cmp, key)
		i.Pos = int16(pos)
		if found || i.Node.IsLeaf() {
			return
		}
		i.Descend(i.Node, i.Pos)
	}
}

// First seeks to the first key in the tree.
func (i *Iterator[K, V, A, U]) First() {
	i.Reset()
	if i.Node == nil {
		return
	}
	for !i.Node.IsLeaf() {
		i.Descend(i.Node, 0)
	}
	i.Pos = 0
}

// Last seeks to the last key in the tree.
func (i *Iterator[K, V, A, U]) Last() {
	i.Reset()
	if i.Node == nil {
		return
	}
	for !i.Node.IsLeaf() {
		i.Descend(i.Node, i.Node.Count)
	}
	i.Pos = i.Node.Count - 1
}

// Next positions the Iterator to the key immediately following
// its current position.
func (i *Iterator[K, V, A, U]) Next() {
	if i.Node == nil {
		return
	}
	if i.Node.IsLeaf() {
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
	for !i.Node.IsLeaf() {
		i.Descend(i.Node, 0)
	}
	i.Pos = 0
}

// Prev positions the Iterator to the key immediately preceding
// its current position.
func (i *Iterator[K, V, A, U]) Prev() {
	if i.Node == nil {
		return
	}
	if i.Node.IsLeaf() {
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
	for !i.Node.IsLeaf() {
		i.Descend(i.Node, i.Node.Count)
	}
	i.Pos = i.Node.Count - 1
}

// Cur returns the key at the Iterator's current position. It is illegal
// to call Key if the Iterator is not valid.
func (i *Iterator[K, V, A, U]) Cur() K {
	return i.Node.Keys[i.Pos]
}

// Value returns the value at the Iterator's current position. It is illegal
// to call Value if the Iterator is not valid.
func (i *Iterator[K, V, A, U]) Value() V {
	return i.Node.Values[i.Pos]
}

// Valid returns true if the iterator is positioned on some element of the tree.
func (i *Iterator[K, V, A, I]) Valid() bool {
	return i.Pos >= 0 && i.Pos < i.Node.Count
}

// IsLeaf returns true if the current Node is a leaf.
func (i *Iterator[K, V, A, U]) IsLeaf() bool {
	return i.Node.IsLeaf()
}

// Depth returns the number of nodes above the current Node in the stack.
// It is illegal to call Ascend if this function returns 0.
func (i *Iterator[K, V, A, U]) Depth() int {
	return i.s.len()
}

// Descend pushes the current position into the iterators stack and
// descends into the child Node currently pointed to by the iterator.
// It is illegal to call if there is no such child. The position in the
// new Node will be 0.
func (i *Iterator[K, V, A, U]) Descend(n *Node[K, V, A], pos int16) {
	i.s.push(iterFrame[K, V, A]{Node: n, Pos: pos})
	i.Node = n.Children[pos]
	i.Pos = 0
}

// Ascend ascends up to the current Node's parent and resets the position
// to the one previously set for this parent Node.
func (i *Iterator[K, V, A, U]) Ascend() {
	i.iterFrame = i.s.pop()
}

func (i *Iterator[K, V, A, U]) makeFrame(n *Node[K, V, A], pos int16) iterFrame[K, V, A] {
	return iterFrame[K, V, A]{
		Node: n,
		Pos:  pos,
	}
}
