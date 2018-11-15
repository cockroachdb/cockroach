// Copyright 2016 The Cockroach Authors.
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
//
// This code is based on: https://github.com/google/btree.

package interval

import (
	"errors"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const (
	// DefaultBTreeMinimumDegree is the default B-tree minimum degree. Benchmarks
	// show that the interval tree performs best with this minimum degree.
	DefaultBTreeMinimumDegree = 32
	// DefaultBTreeFreeListSize is the default size of a B-tree's freelist.
	DefaultBTreeFreeListSize = 32
)

var (
	nilItems    = make(items, 16)
	nilChildren = make(children, 16)
)

// FreeList represents a free list of btree nodes. By default each
// BTree has its own FreeList, but multiple BTrees can share the same
// FreeList.
// Two Btrees using the same freelist are safe for concurrent write access.
type FreeList struct {
	mu       syncutil.Mutex
	freelist []*node
}

// NewFreeList creates a new free list.
// size is the maximum size of the returned free list.
func NewFreeList(size int) *FreeList {
	return &FreeList{freelist: make([]*node, 0, size)}
}

func (f *FreeList) newNode() (n *node) {
	f.mu.Lock()
	index := len(f.freelist) - 1
	if index < 0 {
		f.mu.Unlock()
		return new(node)
	}
	n = f.freelist[index]
	f.freelist[index] = nil
	f.freelist = f.freelist[:index]
	f.mu.Unlock()
	return
}

// freeNode adds the given node to the list, returning true if it was added
// and false if it was discarded.
func (f *FreeList) freeNode(n *node) (out bool) {
	f.mu.Lock()
	if len(f.freelist) < cap(f.freelist) {
		f.freelist = append(f.freelist, n)
		out = true
	}
	f.mu.Unlock()
	return
}

// newBTree creates a new interval tree with the given overlapper function and
// the default B-tree minimum degree.
func newBTree(overlapper Overlapper) *btree {
	return newBTreeWithDegree(overlapper, DefaultBTreeMinimumDegree)
}

// newBTreeWithDegree creates a new interval tree with the given overlapper
// function and the given minimum degree. A minimum degree less than 2 will
// cause a panic.
//
// newBTreeWithDegree(overlapper, 2), for example, will create a 2-3-4 tree (each
// node contains 1-3 Interfaces and 2-4 children).
func newBTreeWithDegree(overlapper Overlapper, minimumDegree int) *btree {
	if minimumDegree < 2 {
		panic("bad minimum degree")
	}
	f := NewFreeList(DefaultBTreeFreeListSize)
	return &btree{
		minimumDegree: minimumDegree,
		overlapper:    overlapper,
		cow:           &copyOnWriteContext{freelist: f},
	}
}

func isValidInterface(a Interface) error {
	if a == nil {
		return log.Safe(errors.New("nil interface"))
	}
	r := a.Range()
	return rangeError(r)
}

// interfaces stores Interfaces sorted by Range().End in a node.
type items []Interface

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *items) insertAt(index int, e Interface) {
	oldLen := len(*s)
	*s = append(*s, nil)
	if index < oldLen {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = e
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *items) removeAt(index int) Interface {
	e := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
	return e
}

// pop removes and returns the last element in the list.
func (s *items) pop() (out Interface) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

// truncate truncates this instance at index so that it contains only the
// first index items. index must be less than or equal to length.
func (s *items) truncate(index int) {
	var toClear items
	*s, toClear = (*s)[:index], (*s)[index:]
	for len(toClear) > 0 {
		toClear = toClear[copy(toClear, nilItems):]
	}
}

// find returns the index where the given Interface should be inserted into this
// list. 'found' is true if the interface already exists in the list at the
// given index.
func (s items) find(e Interface) (index int, found bool) {
	i := sort.Search(len(s), func(i int) bool {
		return Compare(e, s[i]) < 0
	})
	if i > 0 && Equal(s[i-1], e) {
		return i - 1, true
	}
	return i, false
}

// children stores child nodes sorted by Range.End in a node.
type children []*node

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *children) insertAt(index int, n *node) {
	oldLen := len(*s)
	*s = append(*s, nil)
	if index < oldLen {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = n
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *children) removeAt(index int) *node {
	n := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
	return n
}

// pop removes and returns the last element in the list.
func (s *children) pop() (out *node) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

// truncate truncates this instance at index so that it contains only the
// first index children. index must be less than or equal to length.
func (s *children) truncate(index int) {
	var toClear children
	*s, toClear = (*s)[:index], (*s)[index:]
	for len(toClear) > 0 {
		toClear = toClear[copy(toClear, nilChildren):]
	}
}

// node is an internal node in a tree.
//
// It must at all times maintain the invariant that either
//   * len(children) == 0, len(interfaces) unconstrained
//   * len(children) == len(interfaces) + 1
type node struct {
	// Range is the node range which covers all the ranges in the subtree rooted
	// at the node. Range.Start is the leftmost position. Range.End is the
	// rightmost position. Here we follow the approach employed by
	// https://github.com/biogo/store/tree/master/interval since it make it easy
	// to analyze the traversal of intervals which overlaps with a given interval.
	// CLRS only uses Range.End.
	Range    Range
	items    items
	children children
	cow      *copyOnWriteContext
}

func (n *node) mutableFor(cow *copyOnWriteContext) *node {
	if n.cow == cow {
		return n
	}
	out := cow.newNode()
	out.Range = n.Range
	if cap(out.items) >= len(n.items) {
		out.items = out.items[:len(n.items)]
	} else {
		out.items = make(items, len(n.items), cap(n.items))
	}
	copy(out.items, n.items)
	// Copy children
	if cap(out.children) >= len(n.children) {
		out.children = out.children[:len(n.children)]
	} else {
		out.children = make(children, len(n.children), cap(n.children))
	}
	copy(out.children, n.children)
	return out
}

func (n *node) mutableChild(i int) *node {
	c := n.children[i].mutableFor(n.cow)
	n.children[i] = c
	return c
}

// split splits the given node at the given index. The current node shrinks, and
// this function returns the Interface that existed at that index and a new node
// containing all interfaces/children after it. Before splitting:
//
//          +-----------+
//          |   x y z   |
//          ---/-/-\-\--+
//
// After splitting:
//
//          +-----------+
//          |     y     |
//          -----/-\----+
//              /   \
//             v     v
// +-----------+     +-----------+
// |         x |     | z         |
// +-----------+     +-----------+
//
func (n *node) split(i int, fast bool) (Interface, *node) {
	e := n.items[i]
	second := n.cow.newNode()
	second.items = append(second.items, n.items[i+1:]...)
	n.items.truncate(i)
	if len(n.children) > 0 {
		second.children = append(second.children, n.children[i+1:]...)
		n.children.truncate(i + 1)
	}
	if !fast {
		// adjust range for the first split part
		oldRangeEnd := n.Range.End
		n.Range.End = n.rangeEnd()

		// adjust range for the second split part
		second.Range.Start = second.rangeStart()
		if n.Range.End.Equal(oldRangeEnd) || e.Range().End.Equal(oldRangeEnd) {
			second.Range.End = second.rangeEnd()
		} else {
			second.Range.End = oldRangeEnd
		}
	}
	return e, second
}

// maybeSplitChild checks if a child should be split, and if so splits it.
// Returns whether or not a split occurred.
func (n *node) maybeSplitChild(i, maxItems int, fast bool) bool {
	if len(n.children[i].items) < maxItems {
		return false
	}
	first := n.mutableChild(i)
	e, second := first.split(maxItems/2, fast)
	n.items.insertAt(i, e)
	n.children.insertAt(i+1, second)
	return true
}

// insert inserts an Interface into the subtree rooted at this node, making sure
// no nodes in the subtree exceed maxItems Interfaces.
func (n *node) insert(e Interface, maxItems int, fast bool) (out Interface, extended bool) {
	i, found := n.items.find(e)
	if found {
		out = n.items[i]
		n.items[i] = e
		return
	}
	if len(n.children) == 0 {
		n.items.insertAt(i, e)
		out = nil
		if !fast {
			if i == 0 {
				extended = true
				n.Range.Start = n.items[0].Range().Start
			}
			if n.items[i].Range().End.Compare(n.Range.End) > 0 {
				extended = true
				n.Range.End = n.items[i].Range().End
			}
		}
		return
	}
	if n.maybeSplitChild(i, maxItems, fast) {
		inTree := n.items[i]
		switch Compare(e, inTree) {
		case -1:
			// no change, we want first split node
		case 1:
			i++ // we want second split node
		default:
			out = n.items[i]
			n.items[i] = e
			return
		}
	}
	out, extended = n.mutableChild(i).insert(e, maxItems, fast)
	if !fast && extended {
		extended = false
		if i == 0 && n.children[0].Range.Start.Compare(n.Range.Start) < 0 {
			extended = true
			n.Range.Start = n.children[0].Range.Start
		}
		if n.children[i].Range.End.Compare(n.Range.End) > 0 {
			extended = true
			n.Range.End = n.children[i].Range.End
		}
	}
	return
}

func (t *btree) isEmpty() bool {
	return t.root == nil || len(t.root.items) == 0
}

func (t *btree) Get(r Range) (o []Interface) {
	return t.GetWithOverlapper(r, t.overlapper)
}

func (t *btree) GetWithOverlapper(r Range, overlapper Overlapper) (o []Interface) {
	if err := rangeError(r); err != nil {
		return
	}
	if !t.overlappable(r) {
		return
	}
	t.root.doMatch(func(e Interface) (done bool) { o = append(o, e); return }, r, overlapper)
	return
}

func (t *btree) DoMatching(fn Operation, r Range) bool {
	if err := rangeError(r); err != nil {
		return false
	}
	if !t.overlappable(r) {
		return false
	}
	return t.root.doMatch(fn, r, t.overlapper)
}

func (t *btree) overlappable(r Range) bool {
	if t.isEmpty() || !t.overlapper.Overlap(r, t.root.Range) {
		return false
	}
	return true
}

// benchmarks show that if Comparable.Compare is invoked directly instead of
// through an indirection with Overlapper, Insert, Delete and a traversal to
// visit overlapped intervals have a noticeable speed-up. So two versions of
// doMatch are created. One is for InclusiveOverlapper. The other is for
// ExclusiveOverlapper.
func (n *node) doMatch(fn Operation, r Range, overlapper Overlapper) (done bool) {
	if overlapper == InclusiveOverlapper {
		return n.inclusiveDoMatch(fn, r, overlapper)
	}
	return n.exclusiveDoMatch(fn, r, overlapper)
}

// doMatch for InclusiveOverlapper.
func (n *node) inclusiveDoMatch(fn Operation, r Range, overlapper Overlapper) (done bool) {
	length := sort.Search(len(n.items), func(i int) bool {
		return n.items[i].Range().Start.Compare(r.End) > 0
	})

	if len(n.children) == 0 {
		for _, e := range n.items[:length] {
			if r.Start.Compare(e.Range().End) <= 0 {
				if done = fn(e); done {
					return
				}
			}
		}
		return
	}

	for i := 0; i < length; i++ {
		c := n.children[i]
		if r.Start.Compare(c.Range.End) <= 0 {
			if done = c.inclusiveDoMatch(fn, r, overlapper); done {
				return
			}
		}
		e := n.items[i]
		if r.Start.Compare(e.Range().End) <= 0 {
			if done = fn(e); done {
				return
			}
		}
	}

	if overlapper.Overlap(r, n.children[length].Range) {
		done = n.children[length].inclusiveDoMatch(fn, r, overlapper)
	}
	return
}

// doMatch for ExclusiveOverlapper.
func (n *node) exclusiveDoMatch(fn Operation, r Range, overlapper Overlapper) (done bool) {
	length := sort.Search(len(n.items), func(i int) bool {
		return n.items[i].Range().Start.Compare(r.End) >= 0
	})

	if len(n.children) == 0 {
		for _, e := range n.items[:length] {
			if r.Start.Compare(e.Range().End) < 0 {
				if done = fn(e); done {
					return
				}
			}
		}
		return
	}

	for i := 0; i < length; i++ {
		c := n.children[i]
		if r.Start.Compare(c.Range.End) < 0 {
			if done = c.exclusiveDoMatch(fn, r, overlapper); done {
				return
			}
		}
		e := n.items[i]
		if r.Start.Compare(e.Range().End) < 0 {
			if done = fn(e); done {
				return
			}
		}
	}

	if overlapper.Overlap(r, n.children[length].Range) {
		done = n.children[length].exclusiveDoMatch(fn, r, overlapper)
	}
	return
}

func (t *btree) Do(fn Operation) bool {
	if t.root == nil {
		return false
	}
	return t.root.do(fn)
}

func (n *node) do(fn Operation) (done bool) {
	cLen := len(n.children)
	if cLen == 0 {
		for _, e := range n.items {
			if done = fn(e); done {
				return
			}
		}
		return
	}

	for i := 0; i < cLen-1; i++ {
		c := n.children[i]
		if done = c.do(fn); done {
			return
		}
		e := n.items[i]
		if done = fn(e); done {
			return
		}
	}
	done = n.children[cLen-1].do(fn)
	return
}

// toRemove details what interface to remove in a node.remove call.
type toRemove int

const (
	removeItem toRemove = iota // removes the given interface
	removeMin                  // removes smallest interface in the subtree
	removeMax                  // removes largest interface in the subtree
)

// remove removes an interface from the subtree rooted at this node.
func (n *node) remove(
	e Interface, minItems int, typ toRemove, fast bool,
) (out Interface, shrunk bool) {
	var i int
	var found bool
	switch typ {
	case removeMax:
		if len(n.children) == 0 {
			return n.removeFromLeaf(len(n.items)-1, fast)
		}
		i = len(n.items)
	case removeMin:
		if len(n.children) == 0 {
			return n.removeFromLeaf(0, fast)
		}
		i = 0
	case removeItem:
		i, found = n.items.find(e)
		if len(n.children) == 0 {
			if found {
				return n.removeFromLeaf(i, fast)
			}
			return
		}
	default:
		panic("invalid remove type")
	}
	// If we get to here, we have children.
	if len(n.children[i].items) <= minItems {
		out, shrunk = n.growChildAndRemove(i, e, minItems, typ, fast)
		return
	}
	child := n.mutableChild(i)
	// Either we had enough interfaces to begin with, or we've done some
	// merging/stealing, because we've got enough now and we're ready to return
	// stuff.
	if found {
		// The interface exists at index 'i', and the child we've selected can give
		// us a predecessor, since if we've gotten here it's got > minItems
		// interfaces in it.
		out = n.items[i]
		// We use our special-case 'remove' call with typ=removeMax to pull the
		// predecessor of interface i (the rightmost leaf of our immediate left
		// child) and set it into where we pulled the interface from.
		n.items[i], _ = child.remove(nil, minItems, removeMax, fast)
		if !fast {
			shrunk = n.adjustRangeEndForRemoval(out, nil)
		}
		return
	}
	// Final recursive call. Once we're here, we know that the interface isn't in
	// this node and that the child is big enough to remove from.
	out, shrunk = child.remove(e, minItems, typ, fast)
	if !fast && shrunk {
		shrunkOnStart := false
		if i == 0 {
			if n.Range.Start.Compare(child.Range.Start) < 0 {
				shrunkOnStart = true
				n.Range.Start = child.Range.Start
			}
		}
		shrunkOnEnd := n.adjustRangeEndForRemoval(out, nil)
		shrunk = shrunkOnStart || shrunkOnEnd
	}
	return
}

// adjustRangeEndForRemoval adjusts Range.End for the node after an interface
// and/or a child is removed.
func (n *node) adjustRangeEndForRemoval(e Interface, c *node) (decreased bool) {
	if (e != nil && e.Range().End.Equal(n.Range.End)) || (c != nil && c.Range.End.Equal(n.Range.End)) {
		newEnd := n.rangeEnd()
		if n.Range.End.Compare(newEnd) > 0 {
			decreased = true
			n.Range.End = newEnd
		}
	}
	return
}

// removeFromLeaf removes children[i] from the leaf node.
func (n *node) removeFromLeaf(i int, fast bool) (out Interface, shrunk bool) {
	if i == len(n.items)-1 {
		out = n.items.pop()
	} else {
		out = n.items.removeAt(i)
	}
	if !fast && len(n.items) > 0 {
		shrunkOnStart := false
		if i == 0 {
			oldStart := n.Range.Start
			n.Range.Start = n.items[0].Range().Start
			if !n.Range.Start.Equal(oldStart) {
				shrunkOnStart = true
			}
		}
		shrunkOnEnd := n.adjustRangeEndForRemoval(out, nil)
		shrunk = shrunkOnStart || shrunkOnEnd
	}
	return
}

// growChildAndRemove grows child 'i' to make sure it's possible to remove an
// Interface from it while keeping it at minItems, then calls remove to
// actually remove it.
//
// Most documentation says we have to do two sets of special casing:
//   1) interface is in this node
//   2) interface is in child
// In both cases, we need to handle the two subcases:
//   A) node has enough values that it can spare one
//   B) node doesn't have enough values
// For the latter, we have to check:
//   a) left sibling has node to spare
//   b) right sibling has node to spare
//   c) we must merge
// To simplify our code here, we handle cases #1 and #2 the same:
// If a node doesn't have enough Interfaces, we make sure it does (using a,b,c).
// We then simply redo our remove call, and the second time (regardless of
// whether we're in case 1 or 2), we'll have enough Interfaces and can guarantee
// that we hit case A.
func (n *node) growChildAndRemove(
	i int, e Interface, minItems int, typ toRemove, fast bool,
) (out Interface, shrunk bool) {
	if i > 0 && len(n.children[i-1].items) > minItems {
		n.stealFromLeftChild(i, fast)
	} else if i < len(n.items) && len(n.children[i+1].items) > minItems {
		n.stealFromRightChild(i, fast)
	} else {
		if i >= len(n.items) {
			i--
		}
		n.mergeWithRightChild(i, fast)
	}
	return n.remove(e, minItems, typ, fast)
}

// Steal from left child. Before stealing:
//
//          +-----------+
//          |     y     |
//          -----/-\----+
//              /   \
//             v     v
// +-----------+     +-----------+
// |         x |     |           |
// +----------\+     +-----------+
//             \
//              v
//              a
//
// After stealing:
//
//          +-----------+
//          |     x     |
//          -----/-\----+
//              /   \
//             v     v
// +-----------+     +-----------+
// |           |     | y         |
// +-----------+     +/----------+
//                   /
//                  v
//                  a
//
func (n *node) stealFromLeftChild(i int, fast bool) {
	// steal
	stealTo := n.mutableChild(i)
	stealFrom := n.mutableChild(i - 1)
	x := stealFrom.items.pop()
	y := n.items[i-1]
	stealTo.items.insertAt(0, y)
	n.items[i-1] = x
	var a *node
	if len(stealFrom.children) > 0 {
		a = stealFrom.children.pop()
		stealTo.children.insertAt(0, a)
	}

	if !fast {
		// adjust range for stealFrom
		stealFrom.adjustRangeEndForRemoval(x, a)

		// adjust range for stealTo
		stealTo.Range.Start = stealTo.rangeStart()
		if y.Range().End.Compare(stealTo.Range.End) > 0 {
			stealTo.Range.End = y.Range().End
		}
		if a != nil && a.Range.End.Compare(stealTo.Range.End) > 0 {
			stealTo.Range.End = a.Range.End
		}
	}
}

// Steal from right child. Before stealing:
//
//          +-----------+
//          |     y     |
//          -----/-\----+
//              /   \
//             v     v
// +-----------+     +-----------+
// |           |     | x         |
// +---------- +     +/----------+
//                   /
//                  v
//                  a
//
// After stealing:
//
//          +-----------+
//          |     x     |
//          -----/-\----+
//              /   \
//             v     v
// +-----------+     +-----------+
// |         y |     |           |
// +----------\+     +-----------+
//             \
//              v
//              a
//
func (n *node) stealFromRightChild(i int, fast bool) {
	// steal
	stealTo := n.mutableChild(i)
	stealFrom := n.mutableChild(i + 1)
	x := stealFrom.items.removeAt(0)
	y := n.items[i]
	stealTo.items = append(stealTo.items, y)
	n.items[i] = x
	var a *node
	if len(stealFrom.children) > 0 {
		a = stealFrom.children.removeAt(0)
		stealTo.children = append(stealTo.children, a)
	}

	if !fast {
		// adjust range for stealFrom
		stealFrom.Range.Start = stealFrom.rangeStart()
		stealFrom.adjustRangeEndForRemoval(x, a)

		// adjust range for stealTo
		if y.Range().End.Compare(stealTo.Range.End) > 0 {
			stealTo.Range.End = y.Range().End
		}
		if a != nil && a.Range.End.Compare(stealTo.Range.End) > 0 {
			stealTo.Range.End = a.Range.End
		}
	}
}

// Merge with right child. Before merging:
//
//          +-----------+
//          |   u y v   |
//          -----/-\----+
//              /   \
//             v     v
// +-----------+     +-----------+
// |         x |     | z         |
// +---------- +     +-----------+
//
// After merging:
//
//          +-----------+
//          |    u v    |
//          ------|-----+
//                |
//                v
//          +-----------+
//          |   x y z   |
//          +---------- +
//
func (n *node) mergeWithRightChild(i int, fast bool) {
	// merge
	child := n.mutableChild(i)
	mergeItem := n.items.removeAt(i)
	mergeChild := n.children.removeAt(i + 1)
	child.items = append(child.items, mergeItem)
	child.items = append(child.items, mergeChild.items...)
	child.children = append(child.children, mergeChild.children...)

	if !fast {
		if mergeItem.Range().End.Compare(child.Range.End) > 0 {
			child.Range.End = mergeItem.Range().End
		}
		if mergeChild.Range.End.Compare(child.Range.End) > 0 {
			child.Range.End = mergeChild.Range.End
		}
	}
	n.cow.freeNode(mergeChild)
}

var _ Tree = (*btree)(nil)

// btree is an interval tree based on an augmented BTree.
//
// Tree stores Instances in an ordered structure, allowing easy insertion,
// removal, and iteration.
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type btree struct {
	length        int
	minimumDegree int
	overlapper    Overlapper
	root          *node
	cow           *copyOnWriteContext
}

// copyOnWriteContext pointers determine node ownership... a tree with a write
// context equivalent to a node's write context is allowed to modify that node.
// A tree whose write context does not match a node's is not allowed to modify
// it, and must create a new, writable copy (IE: it's a Clone).
//
// When doing any write operation, we maintain the invariant that the current
// node's context is equal to the context of the tree that requested the write.
// We do this by, before we descend into any node, creating a copy with the
// correct context if the contexts don't match.
//
// Since the node we're currently visiting on any write has the requesting
// tree's context, that node is modifiable in place.  Children of that node may
// not share context, but before we descend into them, we'll make a mutable
// copy.
type copyOnWriteContext struct {
	freelist *FreeList
}

// cloneInternal clones the btree, lazily.  Clone should not be called concurrently,
// but the original tree (t) and the new tree (t2) can be used concurrently
// once the Clone call completes.
//
// The internal tree structure of b is marked read-only and shared between t and
// t2.  Writes to both t and t2 use copy-on-write logic, creating new nodes
// whenever one of b's original nodes would have been modified.  Read operations
// should have no performance degredation.  Write operations for both t and t2
// will initially experience minor slow-downs caused by additional allocs and
// copies due to the aforementioned copy-on-write logic, but should converge to
// the original performance characteristics of the original tree.
func (t *btree) cloneInternal() (t2 *btree) {
	// Create two entirely new copy-on-write contexts.
	// This operation effectively creates three trees:
	//   the original, shared nodes (old b.cow)
	//   the new b.cow nodes
	//   the new out.cow nodes
	cow1, cow2 := *t.cow, *t.cow
	out := *t
	t.cow = &cow1
	out.cow = &cow2
	return &out
}

// Clone clones the btree, lazily.
func (t *btree) Clone() Tree {
	return t.cloneInternal()
}

// adjustRange sets the Range to the maximum extent of the childrens' Range
// spans and its range spans.
func (n *node) adjustRange() {
	n.Range.Start = n.rangeStart()
	n.Range.End = n.rangeEnd()
}

// rangeStart returns the leftmost position for the node range, assuming that
// its children have correct range extents.
func (n *node) rangeStart() Comparable {
	minStart := n.items[0].Range().Start
	if len(n.children) > 0 {
		minStart = n.children[0].Range.Start
	}
	return minStart
}

// rangeEnd returns the rightmost position for the node range, assuming that its
// children have correct range extents.
func (n *node) rangeEnd() Comparable {
	if len(n.items) == 0 {
		maxEnd := n.children[0].Range.End
		for _, c := range n.children[1:] {
			if end := c.Range.End; maxEnd.Compare(end) < 0 {
				maxEnd = end
			}
		}
		return maxEnd
	}
	maxEnd := n.items[0].Range().End
	for _, e := range n.items[1:] {
		if end := e.Range().End; maxEnd.Compare(end) < 0 {
			maxEnd = end
		}
	}
	for _, c := range n.children {
		if end := c.Range.End; maxEnd.Compare(end) < 0 {
			maxEnd = end
		}
	}
	return maxEnd
}

func (t *btree) AdjustRanges() {
	if t.isEmpty() {
		return
	}
	t.root.adjustRanges(t.root.cow)
}

func (n *node) adjustRanges(c *copyOnWriteContext) {
	if n.cow != c {
		// Could not have been modified.
		return
	}
	for _, child := range n.children {
		child.adjustRanges(c)
	}
	n.adjustRange()
}

// maxItems returns the max number of Interfaces to allow per node.
func (t *btree) maxItems() int {
	return t.minimumDegree*2 - 1
}

// minItems returns the min number of Interfaces to allow per node (ignored
// for the root node).
func (t *btree) minItems() int {
	return t.minimumDegree - 1
}

func (c *copyOnWriteContext) newNode() (n *node) {
	n = c.freelist.newNode()
	n.cow = c
	return
}

type freeType int

const (
	ftFreelistFull freeType = iota // node was freed (available for GC, not stored in freelist)
	ftStored                       // node was stored in the freelist for later use
	ftNotOwned                     // node was ignored by COW, since it's owned by another one
)

// freeNode frees a node within a given COW context, if it's owned by that
// context.  It returns what happened to the node (see freeType const
// documentation).
func (c *copyOnWriteContext) freeNode(n *node) freeType {
	if n.cow == c {
		// clear to allow GC
		n.items.truncate(0)
		n.children.truncate(0)
		n.cow = nil // clear to allow GC
		if c.freelist.freeNode(n) {
			return ftStored
		}
		return ftFreelistFull
	}
	return ftNotOwned
}

func (t *btree) Insert(e Interface, fast bool) (err error) {
	// t.metrics("Insert")
	if err = isValidInterface(e); err != nil {
		return
	}

	if t.root == nil {
		t.root = t.cow.newNode()
		t.root.items = append(t.root.items, e)
		t.length++
		if !fast {
			t.root.Range.Start = e.Range().Start
			t.root.Range.End = e.Range().End
		}
		return nil
	}

	t.root = t.root.mutableFor(t.cow)
	if len(t.root.items) >= t.maxItems() {
		oldroot := t.root
		t.root = t.cow.newNode()
		if !fast {
			t.root.Range.Start = oldroot.Range.Start
			t.root.Range.End = oldroot.Range.End
		}
		e2, second := oldroot.split(t.maxItems()/2, fast)
		t.root.items = append(t.root.items, e2)
		t.root.children = append(t.root.children, oldroot, second)
	}

	out, _ := t.root.insert(e, t.maxItems(), fast)
	if out == nil {
		t.length++
	}
	return
}

func (t *btree) Delete(e Interface, fast bool) (err error) {
	// t.metrics("Delete")
	if err = isValidInterface(e); err != nil {
		return
	}
	if !t.overlappable(e.Range()) {
		return
	}
	t.delete(e, removeItem, fast)
	return
}

func (t *btree) delete(e Interface, typ toRemove, fast bool) Interface {
	t.root = t.root.mutableFor(t.cow)
	out, _ := t.root.remove(e, t.minItems(), typ, fast)
	if len(t.root.items) == 0 && len(t.root.children) > 0 {
		oldroot := t.root
		t.root = t.root.children[0]
		t.cow.freeNode(oldroot)
	}
	if out != nil {
		t.length--
	}
	return out
}

func (t *btree) Len() int {
	return t.length
}

type stackElem struct {
	node  *node
	index int
}

type btreeIterator struct {
	stack []*stackElem
}

func (ti *btreeIterator) Next() (i Interface, ok bool) {
	if len(ti.stack) == 0 {
		return nil, false
	}
	elem := ti.stack[len(ti.stack)-1]
	curItem := elem.node.items[elem.index]
	elem.index++
	if elem.index >= len(elem.node.items) {
		ti.stack = ti.stack[:len(ti.stack)-1]
	}
	if len(elem.node.children) > 0 {
		for r := elem.node.children[elem.index]; r != nil; r = r.children[0] {
			ti.stack = append(ti.stack, &stackElem{r, 0})
			if len(r.children) == 0 {
				break
			}
		}
	}
	return curItem, true
}

func (t *btree) Iterator() TreeIterator {
	var ti btreeIterator
	for n := t.root; n != nil; n = n.children[0] {
		ti.stack = append(ti.stack, &stackElem{n, 0})
		if len(n.children) == 0 {
			break
		}
	}
	return &ti
}

// ClearWithOpt removes all items from the btree.  If addNodesToFreelist is
// true, t's nodes are added to its freelist as part of this call, until the
// freelist is full.  Otherwise, the root node is simply dereferenced and the
// subtree left to Go's normal GC processes.
//
// This can be much faster than calling Delete on all elements, because that
// requires finding/removing each element in the tree and updating the tree
// accordingly.  It also is somewhat faster than creating a new tree to replace
// the old one, because nodes from the old tree are reclaimed into the freelist
// for use by the new one, instead of being lost to the garbage collector.
//
// This call takes:
//   O(1): when addNodesToFreelist is false, this is a single operation.
//   O(1): when the freelist is already full, it breaks out immediately
//   O(freelist size):  when the freelist is empty and the nodes are all owned
//       by this tree, nodes are added to the freelist until full.
//   O(tree size):  when all nodes are owned by another tree, all nodes are
//       iterated over looking for nodes to add to the freelist, and due to
//       ownership, none are.
func (t *btree) ClearWithOpt(addNodesToFreelist bool) {
	if t.root != nil && addNodesToFreelist {
		t.root.reset(t.cow)
	}
	t.root, t.length = nil, 0
}

func (t *btree) Clear() {
	t.ClearWithOpt(true)
}

// reset returns a subtree to the freelist.  It breaks out immediately if the
// freelist is full, since the only benefit of iterating is to fill that
// freelist up.  Returns true if parent reset call should continue.
func (n *node) reset(c *copyOnWriteContext) bool {
	if n.cow != c {
		return false
	}
	for _, child := range n.children {
		if !child.reset(c) {
			return false
		}
	}
	return c.freeNode(n) != ftFreelistFull
}
