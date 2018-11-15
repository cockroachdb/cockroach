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
	"bytes"
	"sort"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TODO(nvanbenschoten):
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

// cmp returns a value indicating the sort order relationship between
// a and b. The comparison is performed lexicographically on
//  (a.span.Key, a.span.EndKey, a.id)
// and
//  (b.span.Key, b.span.EndKey, b.id)
// tuples.
//
// Given c = cmp(a, b):
//
//  c == -1  if (a.span.Key, a.span.EndKey, a.id) <  (b.span.Key, b.span.EndKey, b.id)
//  c ==  0  if (a.span.Key, a.span.EndKey, a.id) == (b.span.Key, b.span.EndKey, b.id)
//  c ==  1  if (a.span.Key, a.span.EndKey, a.id) >  (b.span.Key, b.span.EndKey, b.id)
//
func cmp(a, b *cmd) int {
	c := bytes.Compare(a.span.Key, b.span.Key)
	if c != 0 {
		return c
	}
	c = bytes.Compare(a.span.EndKey, b.span.EndKey)
	if c != 0 {
		return c
	}
	if a.id < b.id {
		return -1
	} else if a.id > b.id {
		return 1
	} else {
		return 0
	}
}

// keyBound represents the upper-bound of a key range.
type keyBound struct {
	key roachpb.Key
	inc bool
}

func (b keyBound) compare(o keyBound) int {
	c := bytes.Compare(b.key, o.key)
	if c != 0 {
		return c
	}
	if b.inc == o.inc {
		return 0
	}
	if b.inc {
		return 1
	}
	return -1
}

func (b keyBound) contains(a *cmd) bool {
	c := bytes.Compare(a.span.Key, b.key)
	if c == 0 {
		return b.inc
	}
	return c < 0
}

func upperBound(c *cmd) keyBound {
	if len(c.span.EndKey) != 0 {
		return keyBound{key: c.span.EndKey}
	}
	return keyBound{key: c.span.Key, inc: true}
}

type leafNode struct {
	max   keyBound
	count int16
	leaf  bool
	cmds  [maxCmds]*cmd
}

func newLeafNode() *node {
	return (*node)(unsafe.Pointer(&leafNode{leaf: true}))
}

type node struct {
	leafNode
	children [maxCmds + 1]*node
}

func (n *node) insertAt(index int, c *cmd, nd *node) {
	if index < int(n.count) {
		copy(n.cmds[index+1:n.count+1], n.cmds[index:n.count])
		if !n.leaf {
			copy(n.children[index+2:n.count+2], n.children[index+1:n.count+1])
		}
	}
	n.cmds[index] = c
	if !n.leaf {
		n.children[index+1] = nd
	}
	n.count++
}

func (n *node) pushBack(c *cmd, nd *node) {
	n.cmds[n.count] = c
	if !n.leaf {
		n.children[n.count+1] = nd
	}
	n.count++
}

func (n *node) pushFront(c *cmd, nd *node) {
	if !n.leaf {
		copy(n.children[1:n.count+2], n.children[:n.count+1])
		n.children[0] = nd
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
	// Logic copied from sort.Search. Inlining this gave
	// an 11% speedup on BenchmarkBTreeDeleteInsert.
	i, j := 0, int(n.count)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		v := cmp(c, n.cmds[h])
		if v == 0 {
			return h, true
		} else if v > 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, false
}

// split splits the given node at the given index. The current node shrinks,
// and this function returns the cmd that existed at that index and a new node
// containing all cmds/children after it.
//
// Before:
//
//          +-----------+
//          |   x y z   |
//          +--/-/-\-\--+
//
// After:
//
//          +-----------+
//          |     y     |
//          +----/-\----+
//              /   \
//             v     v
// +-----------+     +-----------+
// |         x |     | z         |
// +-----------+     +-----------+
//
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
		for j := int16(i + 1); j <= n.count; j++ {
			n.children[j] = nil
		}
	}
	n.count = int16(i)

	next.max = next.findUpperBound()
	if n.max.compare(next.max) != 0 && n.max.compare(upperBound(out)) != 0 {
		// If upper bound wasn't from new node or cmd
		// at index i, it must still be from old node.
	} else {
		n.max = n.findUpperBound()
	}
	return out, next
}

// insert inserts a cmd into the subtree rooted at this node, making sure no
// nodes in the subtree exceed maxCmds cmds. Returns true if an existing cmd was
// replaced and false if a command was inserted. Also returns whether the node's
// upper bound changes.
func (n *node) insert(c *cmd) (replaced, newBound bool) {
	i, found := n.find(c)
	if found {
		n.cmds[i] = c
		return true, false
	}
	if n.leaf {
		n.insertAt(i, c, nil)
		return false, n.adjustUpperBoundOnInsertion(c, nil)
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
			return true, false
		}
	}
	replaced, newBound = n.children[i].insert(c)
	if newBound {
		newBound = n.adjustUpperBoundOnInsertion(c, nil)
	}
	return replaced, newBound
}

// removeMax removes and returns the maximum cmd from the subtree rooted at
// this node.
func (n *node) removeMax() *cmd {
	if n.leaf {
		n.count--
		out := n.cmds[n.count]
		n.cmds[n.count] = nil
		n.adjustUpperBoundOnRemoval(out, nil)
		return out
	}
	child := n.children[n.count]
	if child.count <= minCmds {
		n.rebalanceOrMerge(int(n.count))
		return n.removeMax()
	}
	return child.removeMax()
}

// remove removes a cmd from the subtree rooted at this node. Returns
// the cmd that was removed or nil if no matching command was found.
// Also returns whether the node's upper bound changes.
func (n *node) remove(c *cmd) (out *cmd, newBound bool) {
	i, found := n.find(c)
	if n.leaf {
		if found {
			out, _ = n.removeAt(i)
			return out, n.adjustUpperBoundOnRemoval(out, nil)
		}
		return nil, false
	}
	child := n.children[i]
	if child.count <= minCmds {
		// Child not large enough to remove from.
		n.rebalanceOrMerge(i)
		return n.remove(c)
	}
	if found {
		// Replace the cmd being removed with the max cmd in our left child.
		out = n.cmds[i]
		n.cmds[i] = child.removeMax()
		return out, n.adjustUpperBoundOnRemoval(out, nil)
	}
	// Cmd is not in this node and child is large enough to remove from.
	out, newBound = child.remove(c)
	if newBound {
		newBound = n.adjustUpperBoundOnRemoval(out, nil)
	}
	return out, newBound
}

// rebalanceOrMerge grows child 'i' to ensure it has sufficient room to remove
// a cmd from it while keeping it at or above minCmds.
func (n *node) rebalanceOrMerge(i int) {
	switch {
	case i > 0 && n.children[i-1].count > minCmds:
		// Rebalance from left sibling.
		//
		//          +-----------+
		//          |     y     |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |         x |     |           |
		// +----------\+     +-----------+
		//             \
		//              v
		//              a
		//
		// After:
		//
		//          +-----------+
		//          |     x     |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |           |     | y         |
		// +-----------+     +/----------+
		//                   /
		//                  v
		//                  a
		//
		left := n.children[i-1]
		child := n.children[i]
		xCmd, grandChild := left.popBack()
		yCmd := n.cmds[i-1]
		child.pushFront(yCmd, grandChild)
		n.cmds[i-1] = xCmd

		left.adjustUpperBoundOnRemoval(xCmd, grandChild)
		child.adjustUpperBoundOnInsertion(yCmd, grandChild)

	case i < int(n.count) && n.children[i+1].count > minCmds:
		// Rebalance from right sibling.
		//
		//          +-----------+
		//          |     y     |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |           |     | x         |
		// +-----------+     +/----------+
		//                   /
		//                  v
		//                  a
		//
		// After:
		//
		//          +-----------+
		//          |     x     |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |         y |     |           |
		// +----------\+     +-----------+
		//             \
		//              v
		//              a
		//
		right := n.children[i+1]
		child := n.children[i]
		xCmd, grandChild := right.popFront()
		yCmd := n.cmds[i]
		child.pushBack(yCmd, grandChild)
		n.cmds[i] = xCmd

		right.adjustUpperBoundOnRemoval(xCmd, grandChild)
		child.adjustUpperBoundOnInsertion(yCmd, grandChild)

	default:
		// Merge with either the left or right sibling.
		//
		//          +-----------+
		//          |   u y v   |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |         x |     | z         |
		// +-----------+     +-----------+
		//
		// After:
		//
		//          +-----------+
		//          |    u v    |
		//          +-----|-----+
		//                |
		//                v
		//          +-----------+
		//          |   x y z   |
		//          +-----------+
		//
		if i >= int(n.count) {
			i = int(n.count - 1)
		}
		child := n.children[i]
		mergeCmd, mergeChild := n.removeAt(i)
		child.cmds[child.count] = mergeCmd
		copy(child.cmds[child.count+1:], mergeChild.cmds[:mergeChild.count])
		if !child.leaf {
			copy(child.children[child.count+1:], mergeChild.children[:mergeChild.count+1])
		}
		child.count += mergeChild.count + 1

		child.adjustUpperBoundOnInsertion(mergeCmd, mergeChild)
	}
}

// findUpperBound returns the largest end key node range, assuming that its
// children have correct upper bounds already set.
func (n *node) findUpperBound() keyBound {
	var max keyBound
	for i := int16(0); i < n.count; i++ {
		up := upperBound(n.cmds[i])
		if max.compare(up) < 0 {
			max = up
		}
	}
	if !n.leaf {
		for i := int16(0); i <= n.count; i++ {
			up := n.children[i].max
			if max.compare(up) < 0 {
				max = up
			}
		}
	}
	return max
}

// adjustUpperBoundOnInsertion adjusts the upper key bound for this node
// given a cmd and an optional child node that was inserted. Returns true
// is the upper bound was changed and false if not.
func (n *node) adjustUpperBoundOnInsertion(c *cmd, child *node) bool {
	up := upperBound(c)
	if child != nil {
		if up.compare(child.max) < 0 {
			up = child.max
		}
	}
	if n.max.compare(up) < 0 {
		n.max = up
		return true
	}
	return false
}

// adjustUpperBoundOnRemoval adjusts the upper key bound for this node
// given a cmd and an optional child node that were removed. Returns true
// is the upper bound was changed and false if not.
func (n *node) adjustUpperBoundOnRemoval(c *cmd, child *node) bool {
	up := upperBound(c)
	if child != nil {
		if up.compare(child.max) < 0 {
			up = child.max
		}
	}
	if n.max.compare(up) == 0 {
		n.max = n.findUpperBound()
		return true
	}
	return false
}

// btree is an implementation of an augmented interval B-Tree.
//
// btree stores cmds in an ordered structure, allowing easy insertion,
// removal, and iteration. It represents intervals and permits an interval
// search operation following the approach laid out in CLRS, Chapter 14.
// The B-Tree stores cmds in order based on their start key and each B-Tree
// node maintains the upper-bound end key of all cmds in its subtree.
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
	if out, _ := t.root.remove(c); out != nil {
		t.length--
	}
	if t.root.count == 0 && !t.root.leaf {
		t.root = t.root.children[0]
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
		newRoot.max = newRoot.findUpperBound()
		t.root = newRoot
	}
	if replaced, _ := t.root.insert(c); !replaced {
		t.length++
	}
}

// MakeIter returns a new iterator object. It is not safe to continue using an
// iterator after modifications are made to the tree. If modifications are made,
// create a new iterator.
func (t *btree) MakeIter() iterator {
	return iterator{r: t.root, pos: -1}
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

// String returns a string description of the tree. The format is
// similar to the https://en.wikipedia.org/wiki/Newick_format.
func (t *btree) String() string {
	if t.length == 0 {
		return ";"
	}
	var b strings.Builder
	t.root.writeString(&b)
	return b.String()
}

func (n *node) writeString(b *strings.Builder) {
	if n.leaf {
		for i := int16(0); i < n.count; i++ {
			if i != 0 {
				b.WriteString(",")
			}
			b.WriteString(n.cmds[i].span.String())
		}
		return
	}
	for i := int16(0); i <= n.count; i++ {
		b.WriteString("(")
		n.children[i].writeString(b)
		b.WriteString(")")
		if i < n.count {
			b.WriteString(n.cmds[i].span.String())
		}
	}
}

// iterStack represents a stack of (node, pos) tuples, which captures
// iteration state as an iterator descends a btree.
type iterStack struct {
	a    iterStackArr
	aLen int16 // -1 when using s
	s    []iterFrame
}

// Used to avoid allocations for stacks below a certain size.
type iterStackArr [3]iterFrame

type iterFrame struct {
	n   *node
	pos int16
}

func (is *iterStack) push(f iterFrame) {
	if is.aLen == -1 {
		is.s = append(is.s, f)
	} else if int(is.aLen) == len(is.a) {
		is.s = make([]iterFrame, int(is.aLen)+1, 2*int(is.aLen))
		copy(is.s, is.a[:])
		is.s[int(is.aLen)] = f
		is.aLen = -1
	} else {
		is.a[is.aLen] = f
		is.aLen++
	}
}

func (is *iterStack) pop() iterFrame {
	if is.aLen == -1 {
		f := is.s[len(is.s)-1]
		is.s = is.s[:len(is.s)-1]
		return f
	}
	is.aLen--
	return is.a[is.aLen]
}

func (is *iterStack) len() int {
	if is.aLen == -1 {
		return len(is.s)
	}
	return int(is.aLen)
}

func (is *iterStack) reset() {
	if is.aLen == -1 {
		is.s = is.s[:0]
	} else {
		is.aLen = 0
	}
}

// iterator is responsible for search and traversal within a btree.
type iterator struct {
	r   *node
	n   *node
	pos int16
	s   iterStack
	o   overlapScan
}

func (i *iterator) reset() {
	i.n = i.r
	i.pos = -1
	i.s.reset()
	i.o = overlapScan{}
}

func (i *iterator) descend(n *node, pos int16) {
	i.s.push(iterFrame{n: n, pos: pos})
	i.n = n.children[pos]
	i.pos = 0
}

// ascend ascends up to the current node's parent and resets the position
// to the one previously set for this parent node.
func (i *iterator) ascend() {
	f := i.s.pop()
	i.n = f.n
	i.pos = f.pos
}

// SeekGE seeks to the first cmd greater-than or equal to the provided cmd.
func (i *iterator) SeekGE(c *cmd) {
	i.reset()
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
		i.descend(i.n, i.pos)
	}
}

// SeekLT seeks to the first cmd less-than the provided cmd.
func (i *iterator) SeekLT(c *cmd) {
	i.reset()
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
		i.descend(i.n, i.pos)
	}
}

// First seeks to the first cmd in the btree.
func (i *iterator) First() {
	i.reset()
	if i.n == nil {
		return
	}
	for !i.n.leaf {
		i.descend(i.n, 0)
	}
	i.pos = 0
}

// Last seeks to the last cmd in the btree.
func (i *iterator) Last() {
	i.reset()
	if i.n == nil {
		return
	}
	for !i.n.leaf {
		i.descend(i.n, i.n.count)
	}
	i.pos = i.n.count - 1
}

// Next positions the iterator to the cmd immediately following
// its current position.
func (i *iterator) Next() {
	if i.n == nil {
		return
	}

	if i.n.leaf {
		i.pos++
		if i.pos < i.n.count {
			return
		}
		for i.s.len() > 0 && i.pos >= i.n.count {
			i.ascend()
		}
		return
	}

	i.descend(i.n, i.pos+1)
	for !i.n.leaf {
		i.descend(i.n, 0)
	}
	i.pos = 0
}

// Prev positions the iterator to the cmd immediately preceding
// its current position.
func (i *iterator) Prev() {
	if i.n == nil {
		return
	}

	if i.n.leaf {
		i.pos--
		if i.pos >= 0 {
			return
		}
		for i.s.len() > 0 && i.pos < 0 {
			i.ascend()
			i.pos--
		}
		return
	}

	i.descend(i.n, i.pos)
	for !i.n.leaf {
		i.descend(i.n, i.n.count)
	}
	i.pos = i.n.count - 1
}

// Valid returns whether the iterator is positioned at a valid position.
func (i *iterator) Valid() bool {
	return i.pos >= 0 && i.pos < i.n.count
}

// Cmd returns the cmd at the iterator's current position. It is illegal
// to call Cmd if the iterator is not valid.
func (i *iterator) Cmd() *cmd {
	return i.n.cmds[i.pos]
}

// An overlap scan is a scan over all cmds that overlap with the provided cmd
// in order of the overlapping cmds' start keys. The goal of the scan is to
// minimize the number of key comparisons performed in total. The algorithm
// operates based on the following two invariants maintained by augmented
// interval btree:
// 1. all cmds are sorted in the btree based on their start key.
// 2. all btree nodes maintain the upper bound end key of all cmds
//    in their subtree.
//
// The scan algorithm starts in "unconstrained minimum" and "unconstrained
// maximum" states. To enter a "constrained minimum" state, the scan must reach
// cmds in the tree with start keys above the search range's start key. Because
// cmds in the tree are sorted by start key, once the scan enters the
// "constrained minimum" state it will remain there. To enter a "constrained
// maximum" state, the scan must determine the first child btree node in a given
// subtree that can have cmds with start keys above the search range's end key.
// The scan then remains in the "constrained maximum" state until it traverse
// into this child node, at which point it moves to the "unconstrained maximum"
// state again.
//
// The scan algorithm works like a standard btree forward scan with the
// following augmentations:
// 1. before tranversing the tree, the scan performs a binary search on the
//    root node's items to determine a "soft" lower-bound constraint position
//    and a "hard" upper-bound constraint position in the root's children.
// 2. when tranversing into a child node in the lower or upper bound constraint
//    position, the constraint is refined by searching the child's items.
// 3. the initial traversal down the tree follows the left-most children
//    whose upper bound end keys are equal to or greater than the start key
//    of the search range. The children followed will be equal to or less
//    than the soft lower bound constraint.
// 4. once the initial tranversal completes and the scan is in the left-most
//    btree node whose upper bound overlaps the search range, key comparisons
//    must be performed with each cmd in the tree. This is necessary because
//    any of these cmds may have end keys that cause them to overlap with the
//    search range.
// 5. once the scan reaches the lower bound constraint position (the first cmd
//    with a start key equal to or greater than the search range's start key),
//    it can begin scaning without performing key comparisons. This is allowed
//    because all commands from this point forward will have end keys that are
//    greater than the search range's start key.
// 6. once the scan reaches the upper bound constraint position, it terminates.
//    It does so because the cmd at this position is the first cmd with a start
//    key larger than the search range's end key.
type overlapScan struct {
	c *cmd // search cmd

	// The "soft" lower-bound constraint.
	constrMinN       *node
	constrMinPos     int16
	constrMinReached bool

	// The "hard" upper-bound constraint.
	constrMaxN   *node
	constrMaxPos int16
}

// FirstOverlap seeks to the first cmd in the btree that overlaps with the
// provided search cmd.
func (i *iterator) FirstOverlap(c *cmd) {
	i.reset()
	if i.n == nil {
		return
	}
	i.pos = 0
	i.o = overlapScan{c: c}
	i.constrainMinSearchBounds()
	i.constrainMaxSearchBounds()
	i.findNextOverlap()
}

// NextOverlap positions the iterator to the cmd immediately following
// its current position that overlaps with the search cmd.
func (i *iterator) NextOverlap() {
	if i.n == nil {
		return
	}
	if i.o.c == nil {
		// Invalid. Mixed overlap scan with non-overlap scan.
		i.pos = i.n.count
		return
	}
	i.pos++
	i.findNextOverlap()
}

func (i *iterator) constrainMinSearchBounds() {
	k := i.o.c.span.Key
	j := sort.Search(int(i.n.count), func(j int) bool {
		return bytes.Compare(k, i.n.cmds[j].span.Key) <= 0
	})
	i.o.constrMinN = i.n
	i.o.constrMinPos = int16(j)
}

func (i *iterator) constrainMaxSearchBounds() {
	up := upperBound(i.o.c)
	j := sort.Search(int(i.n.count), func(j int) bool {
		return !up.contains(i.n.cmds[j])
	})
	i.o.constrMaxN = i.n
	i.o.constrMaxPos = int16(j)
}

func (i *iterator) findNextOverlap() {
	for {
		if i.pos > i.n.count {
			// Iterate up tree.
			i.ascend()
		} else if !i.n.leaf {
			// Iterate down tree.
			if i.o.constrMinReached || i.n.children[i.pos].max.contains(i.o.c) {
				par := i.n
				pos := i.pos
				i.descend(par, pos)

				// Refine the constraint bounds, if necessary.
				if par == i.o.constrMinN && pos == i.o.constrMinPos {
					i.constrainMinSearchBounds()
				}
				if par == i.o.constrMaxN && pos == i.o.constrMaxPos {
					i.constrainMaxSearchBounds()
				}
				continue
			}
		}

		// Check search bounds.
		if i.n == i.o.constrMaxN && i.pos == i.o.constrMaxPos {
			// Invalid. Past possible overlaps.
			i.pos = i.n.count
			return
		}
		if i.n == i.o.constrMinN && i.pos == i.o.constrMinPos {
			// The scan reached the soft lower-bound constraint.
			i.o.constrMinReached = true
		}

		// Iterate across node.
		if i.pos < i.n.count {
			// Check for overlapping cmd.
			if i.o.constrMinReached {
				// Fast-path to avoid span comparison. i.o.constrMinReached
				// tells us that all cmds have end keys above our search
				// span's start key.
				return
			}
			if upperBound(i.n.cmds[i.pos]).contains(i.o.c) {
				return
			}
		}
		i.pos++
	}
}
