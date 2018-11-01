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

package cmdq

import (
	"sort"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TODO(nvanbenschoten):
// 1. Make the structure an augmented interval tree
// 2. Add synchronized node and leafNode freelists
// 3. Introduce immutability and a copy-on-write policy:
// 4. Describe pedigree, changes, etc. of this implementation

const (
	degree  = 16
	maxCmds = 2*degree - 1
	minCmds = degree - 1
)

// TODO(nvanbenschoten): remove.
type cmd struct {
	id   int64
	span roachpb.Span
}

// cmp returns a value indicating the sort order relationship between a and b.
// The comparison is performed lexicographically on (a.span.Key, a.id) and
// (b.span.Key, b.id) tuples where span.Key is more significant that id.
//
// Given c = cmp(a, b):
//
//  c == -1  if (a.span.Key, a.id) < (b.span.Key, b.id);
//  c ==  0  if (a.span.Key, a.id) == (b.span.Key, b.id); and
//  c ==  1  if (a.span.Key, a.id) > (b.span.Key, b.id).
//
func cmp(a, b *cmd) int {
	k := a.span.Key.Compare(b.span.Key)
	if k != 0 {
		return k
	}
	if a.id < b.id {
		return -1
	} else if a.id > b.id {
		return 1
	} else {
		return 0
	}
}

type leafNode struct {
	parent *node
	pos    int16
	count  int16
	leaf   bool
	cmds   [maxCmds]*cmd
}

func newLeafNode() *node {
	return (*node)(unsafe.Pointer(&leafNode{leaf: true}))
}

type node struct {
	leafNode
	children [maxCmds + 1]*node
}

func (n *node) updatePos(start, end int) {
	for i := start; i < end; i++ {
		n.children[i].pos = int16(i)
	}
}

func (n *node) insertAt(index int, c *cmd, nd *node) {
	if index < int(n.count) {
		copy(n.cmds[index+1:n.count+1], n.cmds[index:n.count])
		if !n.leaf {
			copy(n.children[index+2:n.count+2], n.children[index+1:n.count+1])
			n.updatePos(index+2, int(n.count+2))
		}
	}
	n.cmds[index] = c
	if !n.leaf {
		n.children[index+1] = nd
		nd.parent = n
		nd.pos = int16(index + 1)
	}
	n.count++
}

func (n *node) pushBack(c *cmd, nd *node) {
	n.cmds[n.count] = c
	if !n.leaf {
		n.children[n.count+1] = nd
		nd.parent = n
		nd.pos = n.count + 1
	}
	n.count++
}

func (n *node) pushFront(c *cmd, nd *node) {
	if !n.leaf {
		copy(n.children[1:n.count+2], n.children[:n.count+1])
		n.updatePos(1, int(n.count+2))
		n.children[0] = nd
		nd.parent = n
		nd.pos = 0
	}
	copy(n.cmds[1:n.count+1], n.cmds[:n.count])
	n.cmds[0] = c
	n.count++
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (n *node) removeAt(index int) (*cmd, *node) {
	var child *node
	if !n.leaf {
		child = n.children[index+1]
		copy(n.children[index+1:n.count], n.children[index+2:n.count+1])
		n.updatePos(index+1, int(n.count))
		n.children[n.count] = nil
	}
	n.count--
	out := n.cmds[index]
	copy(n.cmds[index:n.count], n.cmds[index+1:n.count+1])
	n.cmds[n.count] = nil
	return out, child
}

// popBack removes and returns the last element in the list.
func (n *node) popBack() (*cmd, *node) {
	n.count--
	out := n.cmds[n.count]
	n.cmds[n.count] = nil
	if n.leaf {
		return out, nil
	}
	child := n.children[n.count+1]
	n.children[n.count+1] = nil
	return out, child
}

// popFront removes and returns the first element in the list.
func (n *node) popFront() (*cmd, *node) {
	n.count--
	var child *node
	if !n.leaf {
		child = n.children[0]
		copy(n.children[:n.count+1], n.children[1:n.count+2])
		n.updatePos(0, int(n.count+1))
		n.children[n.count+1] = nil
	}
	out := n.cmds[0]
	copy(n.cmds[:n.count], n.cmds[1:n.count+1])
	n.cmds[n.count] = nil
	return out, child
}

// find returns the index where the given cmd should be inserted into this
// list. 'found' is true if the cmd already exists in the list at the given
// index.
func (n *node) find(c *cmd) (index int, found bool) {
	i := sort.Search(int(n.count), func(i int) bool {
		return cmp(c, n.cmds[i]) < 0
	})
	if i > 0 && cmp(n.cmds[i-1], c) == 0 {
		return i - 1, true
	}
	return i, false
}

// split splits the given node at the given index. The current node shrinks,
// and this function returns the cmd that existed at that index and a new node
// containing all cmds/children after it.
func (n *node) split(i int) (*cmd, *node) {
	out := n.cmds[i]
	var next *node
	if n.leaf {
		next = newLeafNode()
	} else {
		next = &node{}
	}
	next.count = n.count - int16(i+1)
	copy(next.cmds[:], n.cmds[i+1:n.count])
	for j := int16(i); j < n.count; j++ {
		n.cmds[j] = nil
	}
	if !n.leaf {
		copy(next.children[:], n.children[i+1:n.count+1])
		for j := int16(i + 1); j < n.count; j++ {
			n.children[j] = nil
		}
		for j := int16(0); j <= next.count; j++ {
			next.children[j].parent = next
			next.children[j].pos = j
		}
	}
	n.count = int16(i)
	return out, next
}

// insert inserts a cmd into the subtree rooted at this node, making sure
// no nodes in the subtree exceed maxCmds cmds. Returns true if a cmd was
// inserted and false if an existing cmd was replaced.
func (n *node) insert(c *cmd) bool {
	i, found := n.find(c)
	if found {
		n.cmds[i] = c
		return false
	}
	if n.leaf {
		n.insertAt(i, c, nil)
		return true
	}
	if n.children[i].count >= maxCmds {
		splitcmd, splitNode := n.children[i].split(maxCmds / 2)
		n.insertAt(i, splitcmd, splitNode)

		switch cmp := cmp(c, n.cmds[i]); {
		case cmp < 0:
			// no change, we want first split node
		case cmp > 0:
			i++ // we want second split node
		default:
			n.cmds[i] = c
			return false
		}
	}
	return n.children[i].insert(c)
}

func (n *node) removeMax() *cmd {
	if n.leaf {
		n.count--
		out := n.cmds[n.count]
		n.cmds[n.count] = nil
		return out
	}
	child := n.children[n.count]
	if child.count <= minCmds {
		n.rebalanceOrMerge(int(n.count))
		return n.removeMax()
	}
	return child.removeMax()
}

// remove removes a cmd from the subtree rooted at this node.
func (n *node) remove(c *cmd) (*cmd, bool) {
	i, found := n.find(c)
	if n.leaf {
		if found {
			cmd, _ := n.removeAt(i)
			return cmd, true
		}
		return nil, false
	}
	child := n.children[i]
	if child.count <= minCmds {
		n.rebalanceOrMerge(i)
		return n.remove(c)
	}
	if found {
		// Replace the cmd being removed with the max cmd in our left child.
		out := n.cmds[i]
		n.cmds[i] = child.removeMax()
		return out, true
	}
	return child.remove(c)
}

func (n *node) rebalanceOrMerge(i int) {
	switch {
	case i > 0 && n.children[i-1].count > minCmds:
		// Rebalance from left sibling.
		left := n.children[i-1]
		child := n.children[i]
		cmd, grandChild := left.popBack()
		child.pushFront(n.cmds[i-1], grandChild)
		n.cmds[i-1] = cmd

	case i < int(n.count) && n.children[i+1].count > minCmds:
		// Rebalance from right sibling.
		right := n.children[i+1]
		child := n.children[i]
		cmd, grandChild := right.popFront()
		child.pushBack(n.cmds[i], grandChild)
		n.cmds[i] = cmd

	default:
		// Merge with either the left or right sibling.
		if i >= int(n.count) {
			i = int(n.count - 1)
		}
		child := n.children[i]
		mergeCmd, mergeChild := n.removeAt(i)
		child.cmds[child.count] = mergeCmd
		copy(child.cmds[child.count+1:], mergeChild.cmds[:mergeChild.count])
		if !child.leaf {
			copy(child.children[child.count+1:], mergeChild.children[:mergeChild.count+1])
			for i := int16(0); i <= mergeChild.count; i++ {
				mergeChild.children[i].parent = child
			}
			child.updatePos(int(child.count+1), int(child.count+mergeChild.count+2))
		}
		child.count += mergeChild.count + 1
	}
}

// btree is an implementation of a B-Tree.
//
// btree stores keys in an ordered structure, allowing easy insertion,
// removal, and iteration.
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type btree struct {
	root   *node
	length int
}

// Reset removes all cmds from the btree.
func (t *btree) Reset() {
	t.root = nil
	t.length = 0
}

// Silent unused warning.
var _ = (*btree).Reset

// Delete removes a cmd equal to the passed in cmd from the tree.
func (t *btree) Delete(c *cmd) {
	if t.root == nil || t.root.count == 0 {
		return
	}
	if _, found := t.root.remove(c); found {
		t.length--
	}
	if t.root.count == 0 && !t.root.leaf {
		t.root = t.root.children[0]
		t.root.parent = nil
	}
}

// Set adds the given cmd to the tree. If a cmd in the tree already equals
// the given one, it is replaced with the new cmd.
func (t *btree) Set(c *cmd) {
	if t.root == nil {
		t.root = newLeafNode()
	} else if t.root.count >= maxCmds {
		splitcmd, splitNode := t.root.split(maxCmds / 2)
		newRoot := &node{}
		newRoot.count = 1
		newRoot.cmds[0] = splitcmd
		newRoot.children[0] = t.root
		newRoot.children[1] = splitNode
		t.root.parent = newRoot
		t.root.pos = 0
		splitNode.parent = newRoot
		splitNode.pos = 1
		t.root = newRoot
	}
	if t.root.insert(c) {
		t.length++
	}
}

// MakeIter returns a new iterator object. Note that it is safe for an
// iterator to be copied by value.
func (t *btree) MakeIter() iterator {
	return iterator{t: t, pos: -1}
}

// Height returns the height of the tree.
func (t *btree) Height() int {
	if t.root == nil {
		return 0
	}
	h := 1
	n := t.root
	for !n.leaf {
		n = n.children[0]
		h++
	}
	return h
}

// Len returns the number of cmds currently in the tree.
func (t *btree) Len() int {
	return t.length
}

// iterator ...
type iterator struct {
	t   *btree
	n   *node
	pos int16
}

// SeekGE ...
func (i *iterator) SeekGE(c *cmd) {
	i.n = i.t.root
	if i.n == nil {
		return
	}
	for {
		pos, found := i.n.find(c)
		i.pos = int16(pos)
		if found {
			return
		}
		if i.n.leaf {
			if i.pos == i.n.count {
				i.Next()
			}
			return
		}
		i.n = i.n.children[i.pos]
	}
}

// SeekLT ...
func (i *iterator) SeekLT(c *cmd) {
	i.n = i.t.root
	if i.n == nil {
		return
	}
	for {
		pos, found := i.n.find(c)
		i.pos = int16(pos)
		if found || i.n.leaf {
			i.Prev()
			return
		}
		i.n = i.n.children[i.pos]
	}
}

// First ...
func (i *iterator) First() {
	i.n = i.t.root
	if i.n == nil {
		return
	}
	for !i.n.leaf {
		i.n = i.n.children[0]
	}
	i.pos = 0
}

// Last ...
func (i *iterator) Last() {
	i.n = i.t.root
	if i.n == nil {
		return
	}
	for !i.n.leaf {
		i.n = i.n.children[i.n.count]
	}
	i.pos = i.n.count - 1
}

// Next ...
func (i *iterator) Next() {
	if i.n == nil {
		return
	}

	if i.n.leaf {
		i.pos++
		if i.pos < i.n.count {
			return
		}
		for i.n.parent != nil && i.pos >= i.n.count {
			i.pos = i.n.pos
			i.n = i.n.parent
		}
		return
	}

	i.n = i.n.children[i.pos+1]
	for !i.n.leaf {
		i.n = i.n.children[0]
	}
	i.pos = 0
}

// Prev ...
func (i *iterator) Prev() {
	if i.n == nil {
		return
	}

	if i.n.leaf {
		i.pos--
		if i.pos >= 0 {
			return
		}
		for i.n.parent != nil && i.pos < 0 {
			i.pos = i.n.pos - 1
			i.n = i.n.parent
		}
		return
	}

	i.n = i.n.children[i.pos]
	for !i.n.leaf {
		i.n = i.n.children[i.n.count]
	}
	i.pos = i.n.count - 1
}

// Valid ...
func (i *iterator) Valid() bool {
	return i.pos >= 0 && i.pos < i.n.count
}

// Cmd ...
func (i *iterator) Cmd() *cmd {
	return i.n.cmds[i.pos]
}
