// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanlatch

import (
	"bytes"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

const (
	degree     = 16
	maxLatches = 2*degree - 1
	minLatches = degree - 1
)

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
func cmp(a, b *latch) int {
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

func (b keyBound) contains(a *latch) bool {
	c := bytes.Compare(a.span.Key, b.key)
	if c == 0 {
		return b.inc
	}
	return c < 0
}

func upperBound(c *latch) keyBound {
	if len(c.span.EndKey) != 0 {
		return keyBound{key: c.span.EndKey}
	}
	return keyBound{key: c.span.Key, inc: true}
}

type leafNode struct {
	ref     int32
	count   int16
	leaf    bool
	max     keyBound
	latches [maxLatches]*latch
}

type node struct {
	leafNode
	children [maxLatches + 1]*node
}

func leafToNode(ln *leafNode) *node {
	return (*node)(unsafe.Pointer(ln))
}

func nodeToLeaf(n *node) *leafNode {
	return (*leafNode)(unsafe.Pointer(n))
}

var leafPool = sync.Pool{
	New: func() interface{} {
		return new(leafNode)
	},
}

var nodePool = sync.Pool{
	New: func() interface{} {
		return new(node)
	},
}

func newLeafNode() *node {
	n := leafToNode(leafPool.Get().(*leafNode))
	n.leaf = true
	n.ref = 1
	return n
}

func newNode() *node {
	n := nodePool.Get().(*node)
	n.ref = 1
	return n
}

// mut creates and returns a mutable node reference. If the node is not shared
// with any other trees then it can be modified in place. Otherwise, it must be
// cloned to ensure unique ownership. In this way, we enforce a copy-on-write
// policy which transparently incorporates the idea of local mutations, like
// Clojure's transients or Haskell's ST monad, where nodes are only copied
// during the first time that they are modified between Clone operations.
//
// When a node is cloned, the provided pointer will be redirected to the new
// mutable node.
func mut(n **node) *node {
	if atomic.LoadInt32(&(*n).ref) == 1 {
		// Exclusive ownership. Can mutate in place.
		return *n
	}
	// If we do not have unique ownership over the node then we
	// clone it to gain unique ownership. After doing so, we can
	// release our reference to the old node.
	c := (*n).clone()
	(*n).decRef(true /* recursive */)
	*n = c
	return *n
}

// incRef acquires a reference to the node.
func (n *node) incRef() {
	atomic.AddInt32(&n.ref, 1)
}

// decRef releases a reference to the node. If requested, the method
// will recurse into child nodes and decrease their refcounts as well.
func (n *node) decRef(recursive bool) {
	if atomic.AddInt32(&n.ref, -1) > 0 {
		// Other references remain. Can't free.
		return
	}
	// Clear and release node into memory pool.
	if n.leaf {
		ln := nodeToLeaf(n)
		*ln = leafNode{}
		leafPool.Put(ln)
	} else {
		// Release child references first, if requested.
		if recursive {
			for i := int16(0); i <= n.count; i++ {
				n.children[i].decRef(true /* recursive */)
			}
		}
		*n = node{}
		nodePool.Put(n)
	}
}

// clone creates a clone of the receiver with a single reference count.
func (n *node) clone() *node {
	var c *node
	if n.leaf {
		c = newLeafNode()
	} else {
		c = newNode()
	}
	// NB: copy field-by-field without touching n.ref to avoid
	// triggering the race detector and looking like a data race.
	c.count = n.count
	c.max = n.max
	c.latches = n.latches
	if !c.leaf {
		// Copy children and increase each refcount.
		c.children = n.children
		for i := int16(0); i <= c.count; i++ {
			c.children[i].incRef()
		}
	}
	return c
}

func (n *node) insertAt(index int, la *latch, nd *node) {
	if index < int(n.count) {
		copy(n.latches[index+1:n.count+1], n.latches[index:n.count])
		if !n.leaf {
			copy(n.children[index+2:n.count+2], n.children[index+1:n.count+1])
		}
	}
	n.latches[index] = la
	if !n.leaf {
		n.children[index+1] = nd
	}
	n.count++
}

func (n *node) pushBack(la *latch, nd *node) {
	n.latches[n.count] = la
	if !n.leaf {
		n.children[n.count+1] = nd
	}
	n.count++
}

func (n *node) pushFront(la *latch, nd *node) {
	if !n.leaf {
		copy(n.children[1:n.count+2], n.children[:n.count+1])
		n.children[0] = nd
	}
	copy(n.latches[1:n.count+1], n.latches[:n.count])
	n.latches[0] = la
	n.count++
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (n *node) removeAt(index int) (*latch, *node) {
	var child *node
	if !n.leaf {
		child = n.children[index+1]
		copy(n.children[index+1:n.count], n.children[index+2:n.count+1])
		n.children[n.count] = nil
	}
	n.count--
	out := n.latches[index]
	copy(n.latches[index:n.count], n.latches[index+1:n.count+1])
	n.latches[n.count] = nil
	return out, child
}

// popBack removes and returns the last element in the list.
func (n *node) popBack() (*latch, *node) {
	n.count--
	out := n.latches[n.count]
	n.latches[n.count] = nil
	if n.leaf {
		return out, nil
	}
	child := n.children[n.count+1]
	n.children[n.count+1] = nil
	return out, child
}

// popFront removes and returns the first element in the list.
func (n *node) popFront() (*latch, *node) {
	n.count--
	var child *node
	if !n.leaf {
		child = n.children[0]
		copy(n.children[:n.count+1], n.children[1:n.count+2])
		n.children[n.count+1] = nil
	}
	out := n.latches[0]
	copy(n.latches[:n.count], n.latches[1:n.count+1])
	n.latches[n.count] = nil
	return out, child
}

// find returns the index where the given latch should be inserted into this
// list. 'found' is true if the latch already exists in the list at the given
// index.
func (n *node) find(la *latch) (index int, found bool) {
	// Logic copied from sort.Search. Inlining this gave
	// an 11% speedup on BenchmarkBTreeDeleteInsert.
	i, j := 0, int(n.count)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		v := cmp(la, n.latches[h])
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
// and this function returns the latch that existed at that index and a new
// node containing all latches/children after it.
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
func (n *node) split(i int) (*latch, *node) {
	out := n.latches[i]
	var next *node
	if n.leaf {
		next = newLeafNode()
	} else {
		next = newNode()
	}
	next.count = n.count - int16(i+1)
	copy(next.latches[:], n.latches[i+1:n.count])
	for j := int16(i); j < n.count; j++ {
		n.latches[j] = nil
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
		// If upper bound wasn't from new node or latch
		// at index i, it must still be from old node.
	} else {
		n.max = n.findUpperBound()
	}
	return out, next
}

// insert inserts a latch into the subtree rooted at this node, making sure no
// nodes in the subtree exceed maxLatches latches. Returns true if an existing
// latch was replaced and false if a latch was inserted. Also returns whether
// the node's upper bound changes.
func (n *node) insert(la *latch) (replaced, newBound bool) {
	i, found := n.find(la)
	if found {
		n.latches[i] = la
		return true, false
	}
	if n.leaf {
		n.insertAt(i, la, nil)
		return false, n.adjustUpperBoundOnInsertion(la, nil)
	}
	if n.children[i].count >= maxLatches {
		splitLa, splitNode := mut(&n.children[i]).split(maxLatches / 2)
		n.insertAt(i, splitLa, splitNode)

		switch cmp := cmp(la, n.latches[i]); {
		case cmp < 0:
			// no change, we want first split node
		case cmp > 0:
			i++ // we want second split node
		default:
			n.latches[i] = la
			return true, false
		}
	}
	replaced, newBound = mut(&n.children[i]).insert(la)
	if newBound {
		newBound = n.adjustUpperBoundOnInsertion(la, nil)
	}
	return replaced, newBound
}

// removeMax removes and returns the maximum latch from the subtree rooted
// at this node.
func (n *node) removeMax() *latch {
	if n.leaf {
		n.count--
		out := n.latches[n.count]
		n.latches[n.count] = nil
		n.adjustUpperBoundOnRemoval(out, nil)
		return out
	}
	child := mut(&n.children[n.count])
	if child.count <= minLatches {
		n.rebalanceOrMerge(int(n.count))
		return n.removeMax()
	}
	return child.removeMax()
}

// remove removes a latch from the subtree rooted at this node. Returns
// the latch that was removed or nil if no matching latch was found.
// Also returns whether the node's upper bound changes.
func (n *node) remove(la *latch) (out *latch, newBound bool) {
	i, found := n.find(la)
	if n.leaf {
		if found {
			out, _ = n.removeAt(i)
			return out, n.adjustUpperBoundOnRemoval(out, nil)
		}
		return nil, false
	}
	if n.children[i].count <= minLatches {
		// Child not large enough to remove from.
		n.rebalanceOrMerge(i)
		return n.remove(la)
	}
	child := mut(&n.children[i])
	if found {
		// Replace the latch being removed with the max latch in our left child.
		out = n.latches[i]
		n.latches[i] = child.removeMax()
		return out, n.adjustUpperBoundOnRemoval(out, nil)
	}
	// Latch is not in this node and child is large enough to remove from.
	out, newBound = child.remove(la)
	if newBound {
		newBound = n.adjustUpperBoundOnRemoval(out, nil)
	}
	return out, newBound
}

// rebalanceOrMerge grows child 'i' to ensure it has sufficient room to remove
// a latch from it while keeping it at or above minLatches.
func (n *node) rebalanceOrMerge(i int) {
	switch {
	case i > 0 && n.children[i-1].count > minLatches:
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
		left := mut(&n.children[i-1])
		child := mut(&n.children[i])
		xLa, grandChild := left.popBack()
		yLa := n.latches[i-1]
		child.pushFront(yLa, grandChild)
		n.latches[i-1] = xLa

		left.adjustUpperBoundOnRemoval(xLa, grandChild)
		child.adjustUpperBoundOnInsertion(yLa, grandChild)

	case i < int(n.count) && n.children[i+1].count > minLatches:
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
		right := mut(&n.children[i+1])
		child := mut(&n.children[i])
		xLa, grandChild := right.popFront()
		yLa := n.latches[i]
		child.pushBack(yLa, grandChild)
		n.latches[i] = xLa

		right.adjustUpperBoundOnRemoval(xLa, grandChild)
		child.adjustUpperBoundOnInsertion(yLa, grandChild)

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
		child := mut(&n.children[i])
		// Make mergeChild mutable, bumping the refcounts on its children if necessary.
		_ = mut(&n.children[i+1])
		mergeLa, mergeChild := n.removeAt(i)
		child.latches[child.count] = mergeLa
		copy(child.latches[child.count+1:], mergeChild.latches[:mergeChild.count])
		if !child.leaf {
			copy(child.children[child.count+1:], mergeChild.children[:mergeChild.count+1])
		}
		child.count += mergeChild.count + 1

		child.adjustUpperBoundOnInsertion(mergeLa, mergeChild)
		mergeChild.decRef(false /* recursive */)
	}
}

// findUpperBound returns the largest end key node range, assuming that its
// children have correct upper bounds already set.
func (n *node) findUpperBound() keyBound {
	var max keyBound
	for i := int16(0); i < n.count; i++ {
		up := upperBound(n.latches[i])
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
// given a latch and an optional child node that was inserted. Returns
// true is the upper bound was changed and false if not.
func (n *node) adjustUpperBoundOnInsertion(la *latch, child *node) bool {
	up := upperBound(la)
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
// given a latch and an optional child node that were removed. Returns
// true is the upper bound was changed and false if not.
func (n *node) adjustUpperBoundOnRemoval(la *latch, child *node) bool {
	up := upperBound(la)
	if child != nil {
		if up.compare(child.max) < 0 {
			up = child.max
		}
	}
	if n.max.compare(up) == 0 {
		// up was previous upper bound of n.
		n.max = n.findUpperBound()
		return n.max.compare(up) != 0
	}
	return false
}

// btree is an implementation of an augmented interval B-Tree.
//
// btree stores latches in an ordered structure, allowing easy insertion,
// removal, and iteration. It represents intervals and permits an interval
// search operation following the approach laid out in CLRS, Chapter 14.
// The B-Tree stores latches in order based on their start key and each
// B-Tree node maintains the upper-bound end key of all latches in its
// subtree.
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type btree struct {
	root   *node
	length int
}

// Reset removes all latches from the btree. In doing so, it allows memory
// held by the btree to be recycled. Failure to call this method before
// letting a btree be GCed is safe in that it won't cause a memory leak,
// but it will prevent btree nodes from being efficiently re-used.
func (t *btree) Reset() {
	if t.root != nil {
		t.root.decRef(true /* recursive */)
		t.root = nil
	}
	t.length = 0
}

// Clone clones the btree, lazily.
func (t *btree) Clone() btree {
	c := *t
	if c.root != nil {
		c.root.incRef()
	}
	return c
}

// Delete removes a latch equal to the passed in latch from the tree.
func (t *btree) Delete(la *latch) {
	if t.root == nil || t.root.count == 0 {
		return
	}
	if out, _ := mut(&t.root).remove(la); out != nil {
		t.length--
	}
	if t.root.count == 0 {
		old := t.root
		if t.root.leaf {
			t.root = nil
		} else {
			t.root = t.root.children[0]
		}
		old.decRef(false /* recursive */)
	}
}

// Set adds the given latch to the tree. If a latch in the tree already
// equals the given one, it is replaced with the new latch.
func (t *btree) Set(la *latch) {
	if t.root == nil {
		t.root = newLeafNode()
	} else if t.root.count >= maxLatches {
		splitLa, splitNode := mut(&t.root).split(maxLatches / 2)
		newRoot := newNode()
		newRoot.count = 1
		newRoot.latches[0] = splitLa
		newRoot.children[0] = t.root
		newRoot.children[1] = splitNode
		newRoot.max = newRoot.findUpperBound()
		t.root = newRoot
	}
	if replaced, _ := mut(&t.root).insert(la); !replaced {
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

// Len returns the number of latches currently in the tree.
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
			b.WriteString(n.latches[i].span.String())
		}
		return
	}
	for i := int16(0); i <= n.count; i++ {
		b.WriteString("(")
		n.children[i].writeString(b)
		b.WriteString(")")
		if i < n.count {
			b.WriteString(n.latches[i].span.String())
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

// SeekGE seeks to the first latch greater-than or equal to the provided
// latch.
func (i *iterator) SeekGE(la *latch) {
	i.reset()
	if i.n == nil {
		return
	}
	for {
		pos, found := i.n.find(la)
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

// SeekLT seeks to the first latch less-than the provided latch.
func (i *iterator) SeekLT(la *latch) {
	i.reset()
	if i.n == nil {
		return
	}
	for {
		pos, found := i.n.find(la)
		i.pos = int16(pos)
		if found || i.n.leaf {
			i.Prev()
			return
		}
		i.descend(i.n, i.pos)
	}
}

// First seeks to the first latch in the btree.
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

// Last seeks to the last latch in the btree.
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

// Next positions the iterator to the latch immediately following
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

// Prev positions the iterator to the latch immediately preceding
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

// Cur returns the latch at the iterator's current position. It is illegal
// to call Latch if the iterator is not valid.
func (i *iterator) Cur() *latch {
	return i.n.latches[i.pos]
}

// An overlap scan is a scan over all latches that overlap with the provided
// latch in order of the overlapping latches' start keys. The goal of the scan
// is to minimize the number of key comparisons performed in total. The
// algorithm operates based on the following two invariants maintained by
// augmented interval btree:
// 1. all latches are sorted in the btree based on their start key.
// 2. all btree nodes maintain the upper bound end key of all latches
//    in their subtree.
//
// The scan algorithm starts in "unconstrained minimum" and "unconstrained
// maximum" states. To enter a "constrained minimum" state, the scan must reach
// latches in the tree with start keys above the search range's start key.
// Because latches in the tree are sorted by start key, once the scan enters the
// "constrained minimum" state it will remain there. To enter a "constrained
// maximum" state, the scan must determine the first child btree node in a given
// subtree that can have latches with start keys above the search range's end
// key. The scan then remains in the "constrained maximum" state until it
// traverse into this child node, at which point it moves to the "unconstrained
// maximum" state again.
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
//    must be performed with each latch in the tree. This is necessary because
//    any of these latches may have end keys that cause them to overlap with the
//    search range.
// 5. once the scan reaches the lower bound constraint position (the first latch
//    with a start key equal to or greater than the search range's start key),
//    it can begin scaning without performing key comparisons. This is allowed
//    because all latches from this point forward will have end keys that are
//    greater than the search range's start key.
// 6. once the scan reaches the upper bound constraint position, it terminates.
//    It does so because the latch at this position is the first latch with a
//    start key larger than the search range's end key.
type overlapScan struct {
	la *latch // search latch

	// The "soft" lower-bound constraint.
	constrMinN       *node
	constrMinPos     int16
	constrMinReached bool

	// The "hard" upper-bound constraint.
	constrMaxN   *node
	constrMaxPos int16
}

// FirstOverlap seeks to the first latch in the btree that overlaps with the
// provided search latch.
func (i *iterator) FirstOverlap(la *latch) {
	i.reset()
	if i.n == nil {
		return
	}
	i.pos = 0
	i.o = overlapScan{la: la}
	i.constrainMinSearchBounds()
	i.constrainMaxSearchBounds()
	i.findNextOverlap()
}

// NextOverlap positions the iterator to the latch immediately following
// its current position that overlaps with the search latch.
func (i *iterator) NextOverlap() {
	if i.n == nil {
		return
	}
	if i.o.la == nil {
		// Invalid. Mixed overlap scan with non-overlap scan.
		i.pos = i.n.count
		return
	}
	i.pos++
	i.findNextOverlap()
}

func (i *iterator) constrainMinSearchBounds() {
	k := i.o.la.span.Key
	j := sort.Search(int(i.n.count), func(j int) bool {
		return bytes.Compare(k, i.n.latches[j].span.Key) <= 0
	})
	i.o.constrMinN = i.n
	i.o.constrMinPos = int16(j)
}

func (i *iterator) constrainMaxSearchBounds() {
	up := upperBound(i.o.la)
	j := sort.Search(int(i.n.count), func(j int) bool {
		return !up.contains(i.n.latches[j])
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
			if i.o.constrMinReached || i.n.children[i.pos].max.contains(i.o.la) {
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
			// Check for overlapping latch.
			if i.o.constrMinReached {
				// Fast-path to avoid span comparison. i.o.constrMinReached
				// tells us that all latches have end keys above our search
				// span's start key.
				return
			}
			if upperBound(i.n.latches[i.pos]).contains(i.o.la) {
				return
			}
		}
		i.pos++
	}
}
