// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package interval

import (
	"github.com/biogo/store/llrb"
)

// An IntOverlapper can determine whether it overlaps an integer range.
type IntOverlapper interface {
	// Overlap returns a boolean indicating whether the receiver overlaps a range.
	Overlap(IntRange) bool
}

// An IntRange is a type that describes the basic characteristics of an interval over the
// integer number line.
type IntRange struct {
	Start, End int
}

// An IntInterface is a type that can be inserted into a IntTree.
type IntInterface interface {
	IntOverlapper
	Range() IntRange
	ID() uintptr // Returns a unique ID for the element.
}

// A IntNode represents a node in an IntTree.
type IntNode struct {
	Elem        IntInterface
	Interval    IntRange
	Range       IntRange
	Left, Right *IntNode
	Color       llrb.Color
}

// A IntTree manages the root node of an integer line interval tree.
// Public methods are exposed through this type.
type IntTree struct {
	Root  *IntNode // Root node of the tree.
	Count int      // Number of elements stored.
}

// Helper methods

// color returns the effect color of a IntNode. A nil node returns black.
func (n *IntNode) color() llrb.Color {
	if n == nil {
		return llrb.Black
	}
	return n.Color
}

// intMaxRange returns the furthest right position held by the subtree
// rooted at root, assuming that the left and right nodes have correct
// range extents.
func intMaxRange(root, left, right *IntNode) int {
	end := root.Interval.End
	if left != nil && left.Range.End > end {
		end = left.Range.End
	}
	if right != nil && right.Range.End > end {
		end = right.Range.End
	}
	return end
}

// (a,c)b -rotL-> ((a,)b,)c
func (n *IntNode) rotateLeft() (root *IntNode) {
	// Assumes: n has a right child.
	root = n.Right
	n.Right = root.Left
	root.Left = n
	root.Color = n.Color
	n.Color = llrb.Red

	root.Left.Range.End = intMaxRange(root.Left, root.Left.Left, root.Left.Right)
	if root.Left == nil {
		root.Range.Start = root.Interval.Start
	} else {
		root.Range.Start = root.Left.Range.Start
	}
	root.Range.End = intMaxRange(root, root.Left, root.Right)

	return
}

// (a,c)b -rotR-> (,(,c)b)a
func (n *IntNode) rotateRight() (root *IntNode) {
	// Assumes: n has a left child.
	root = n.Left
	n.Left = root.Right
	root.Right = n
	root.Color = n.Color
	n.Color = llrb.Red

	if root.Right.Left == nil {
		root.Right.Range.Start = root.Right.Interval.Start
	} else {
		root.Right.Range.Start = root.Right.Left.Range.Start
	}
	root.Right.Range.End = intMaxRange(root.Right, root.Right.Left, root.Right.Right)
	root.Range.End = intMaxRange(root, root.Left, root.Right)

	return
}

// (aR,cR)bB -flipC-> (aB,cB)bR | (aB,cB)bR -flipC-> (aR,cR)bB
func (n *IntNode) flipColors() {
	// Assumes: n has two children.
	n.Color = !n.Color
	n.Left.Color = !n.Left.Color
	n.Right.Color = !n.Right.Color
}

// fixUp ensures that black link balance is correct, that red nodes lean left,
// and that 4 nodes are split in the case of BU23 and properly balanced in TD234.
func (n *IntNode) fixUp(fast bool) *IntNode {
	if !fast {
		n.adjustRange()
	}
	if n.Right.color() == llrb.Red {
		if Mode == TD234 && n.Right.Left.color() == llrb.Red {
			n.Right = n.Right.rotateRight()
		}
		n = n.rotateLeft()
	}
	if n.Left.color() == llrb.Red && n.Left.Left.color() == llrb.Red {
		n = n.rotateRight()
	}
	if Mode == BU23 && n.Left.color() == llrb.Red && n.Right.color() == llrb.Red {
		n.flipColors()
	}

	return n
}

// adjustRange sets the Range to the maximum extent of the childrens' Range
// spans and the node's Elem span.
func (n *IntNode) adjustRange() {
	if n.Left == nil {
		n.Range.Start = n.Interval.Start
	} else {
		n.Range.Start = n.Left.Range.Start
	}
	n.Range.End = intMaxRange(n, n.Left, n.Right)
}

func (n *IntNode) moveRedLeft() *IntNode {
	n.flipColors()
	if n.Right.Left.color() == llrb.Red {
		n.Right = n.Right.rotateRight()
		n = n.rotateLeft()
		n.flipColors()
		if Mode == TD234 && n.Right.Right.color() == llrb.Red {
			n.Right = n.Right.rotateLeft()
		}
	}
	return n
}

func (n *IntNode) moveRedRight() *IntNode {
	n.flipColors()
	if n.Left.Left.color() == llrb.Red {
		n = n.rotateRight()
		n.flipColors()
	}
	return n
}

// Len returns the number of intervals stored in the IntTree.
func (t *IntTree) Len() int {
	return t.Count
}

// Get returns a slice of IntInterfaces that overlap q in the IntTree according
// to q.Overlap().
func (t *IntTree) Get(q IntOverlapper) (o []IntInterface) {
	if t.Root != nil && q.Overlap(t.Root.Range) {
		t.Root.doMatch(func(e IntInterface) (done bool) { o = append(o, e); return }, q)
	}
	return
}

// AdjustRanges fixes range fields for all IntNodes in the IntTree. This must be called
// before Get or DoMatching* is used if fast insertion or deletion has been performed.
func (t *IntTree) AdjustRanges() {
	if t.Root == nil {
		return
	}
	t.Root.adjustRanges()
}

func (n *IntNode) adjustRanges() {
	if n.Left != nil {
		n.Left.adjustRanges()
	}
	if n.Right != nil {
		n.Right.adjustRanges()
	}
	n.adjustRange()
}

// Insert inserts the IntInterface e into the IntTree. Insertions may replace
// existing stored intervals.
func (t *IntTree) Insert(e IntInterface, fast bool) (err error) {
	if r := e.Range(); r.Start > r.End {
		return ErrInvertedRange
	}
	var d int
	t.Root, d = t.Root.insert(e, e.Range(), e.ID(), fast)
	t.Count += d
	t.Root.Color = llrb.Black
	return
}

func (n *IntNode) insert(e IntInterface, r IntRange, id uintptr, fast bool) (root *IntNode, d int) {
	if n == nil {
		return &IntNode{Elem: e, Interval: r, Range: r}, 1
	} else if n.Elem == nil {
		n.Elem = e
		n.Interval = r
		if !fast {
			n.adjustRange()
		}
		return n, 1
	}

	if Mode == TD234 {
		if n.Left.color() == llrb.Red && n.Right.color() == llrb.Red {
			n.flipColors()
		}
	}

	switch c := r.Start - n.Interval.Start; {
	case c == 0:
		switch cid := id - n.Elem.ID(); {
		case cid == 0:
			n.Elem = e
			n.Interval = r
			if !fast {
				n.Range.End = r.End
			}
		case cid < 0:
			n.Left, d = n.Left.insert(e, r, id, fast)
		default:
			n.Right, d = n.Right.insert(e, r, id, fast)
		}
	case c < 0:
		n.Left, d = n.Left.insert(e, r, id, fast)
	default:
		n.Right, d = n.Right.insert(e, r, id, fast)
	}

	if n.Right.color() == llrb.Red && n.Left.color() == llrb.Black {
		n = n.rotateLeft()
	}
	if n.Left.color() == llrb.Red && n.Left.Left.color() == llrb.Red {
		n = n.rotateRight()
	}

	if Mode == BU23 {
		if n.Left.color() == llrb.Red && n.Right.color() == llrb.Red {
			n.flipColors()
		}
	}

	if !fast {
		n.adjustRange()
	}
	root = n

	return
}

// DeleteMin deletes the left-most interval.
func (t *IntTree) DeleteMin(fast bool) {
	if t.Root == nil {
		return
	}
	var d int
	t.Root, d = t.Root.deleteMin(fast)
	t.Count += d
	if t.Root == nil {
		return
	}
	t.Root.Color = llrb.Black
}

func (n *IntNode) deleteMin(fast bool) (root *IntNode, d int) {
	if n.Left == nil {
		return nil, -1
	}
	if n.Left.color() == llrb.Black && n.Left.Left.color() == llrb.Black {
		n = n.moveRedLeft()
	}
	n.Left, d = n.Left.deleteMin(fast)
	if n.Left == nil {
		n.Range.Start = n.Elem.Range().Start
	}

	root = n.fixUp(fast)

	return
}

// DeleteMax deletes the right-most interval.
func (t *IntTree) DeleteMax(fast bool) {
	if t.Root == nil {
		return
	}
	var d int
	t.Root, d = t.Root.deleteMax(fast)
	t.Count += d
	if t.Root == nil {
		return
	}
	t.Root.Color = llrb.Black
}

func (n *IntNode) deleteMax(fast bool) (root *IntNode, d int) {
	if n.Left != nil && n.Left.color() == llrb.Red {
		n = n.rotateRight()
	}
	if n.Right == nil {
		return nil, -1
	}
	if n.Right.color() == llrb.Black && n.Right.Left.color() == llrb.Black {
		n = n.moveRedRight()
	}
	n.Right, d = n.Right.deleteMax(fast)
	if n.Right == nil {
		n.Range.End = n.Elem.Range().End
	}

	root = n.fixUp(fast)

	return
}

// Delete deletes the element e if it exists in the IntTree.
func (t *IntTree) Delete(e IntInterface, fast bool) (err error) {
	if r := e.Range(); r.Start > r.End {
		return ErrInvertedRange
	}
	if t.Root == nil || !e.Overlap(t.Root.Range) {
		return
	}
	var d int
	t.Root, d = t.Root.delete(e.Range().Start, e.ID(), fast)
	t.Count += d
	if t.Root == nil {
		return
	}
	t.Root.Color = llrb.Black
	return
}

func (n *IntNode) delete(m int, id uintptr, fast bool) (root *IntNode, d int) {
	if p := m - n.Interval.Start; p < 0 || (p == 0 && id < n.Elem.ID()) {
		if n.Left != nil {
			if n.Left.color() == llrb.Black && n.Left.Left.color() == llrb.Black {
				n = n.moveRedLeft()
			}
			n.Left, d = n.Left.delete(m, id, fast)
			if n.Left == nil {
				n.Range.Start = n.Interval.Start
			}
		}
	} else {
		if n.Left.color() == llrb.Red {
			n = n.rotateRight()
		}
		if n.Right == nil && id == n.Elem.ID() {
			return nil, -1
		}
		if n.Right != nil {
			if n.Right.color() == llrb.Black && n.Right.Left.color() == llrb.Black {
				n = n.moveRedRight()
			}
			if id == n.Elem.ID() {
				m := n.Right.min()
				n.Elem = m.Elem
				n.Interval = m.Interval
				n.Right, d = n.Right.deleteMin(fast)
			} else {
				n.Right, d = n.Right.delete(m, id, fast)
			}
			if n.Right == nil {
				n.Range.End = n.Interval.End
			}
		}
	}

	root = n.fixUp(fast)

	return
}

// Return the left-most interval stored in the tree.
func (t *IntTree) Min() IntInterface {
	if t.Root == nil {
		return nil
	}
	return t.Root.min().Elem
}

func (n *IntNode) min() *IntNode {
	for ; n.Left != nil; n = n.Left {
	}
	return n
}

// Return the right-most interval stored in the tree.
func (t *IntTree) Max() IntInterface {
	if t.Root == nil {
		return nil
	}
	return t.Root.max().Elem
}

func (n *IntNode) max() *IntNode {
	for ; n.Right != nil; n = n.Right {
	}
	return n
}

// Floor returns the largest value equal to or less than the query q according to
// q.Start().Compare(), with ties broken by comparison of ID() values.
func (t *IntTree) Floor(q IntInterface) (o IntInterface, err error) {
	if t.Root == nil {
		return
	}
	n := t.Root.floor(q.Range().Start, q.ID())
	if n == nil {
		return
	}
	return n.Elem, nil
}

func (n *IntNode) floor(m int, id uintptr) *IntNode {
	if n == nil {
		return nil
	}
	switch c := m - n.Interval.Start; {
	case c == 0:
		switch cid := id - n.Elem.ID(); {
		case cid == 0:
			return n
		case cid < 0:
			return n.Left.floor(m, id)
		default:
			if r := n.Right.floor(m, id); r != nil {
				return r
			}
		}
	case c < 0:
		return n.Left.floor(m, id)
	default:
		if r := n.Right.floor(m, id); r != nil {
			return r
		}
	}
	return n
}

// Ceil returns the smallest value equal to or greater than the query q according to
// q.Start().Compare(), with ties broken by comparison of ID() values.
func (t *IntTree) Ceil(q IntInterface) (o IntInterface, err error) {
	if t.Root == nil {
		return
	}
	n := t.Root.ceil(q.Range().Start, q.ID())
	if n == nil {
		return
	}
	return n.Elem, nil
}

func (n *IntNode) ceil(m int, id uintptr) *IntNode {
	if n == nil {
		return nil
	}
	switch c := m - n.Interval.Start; {
	case c == 0:
		switch cid := id - n.Elem.ID(); {
		case cid == 0:
			return n
		case cid > 0:
			return n.Right.ceil(m, id)
		default:
			if l := n.Left.ceil(m, id); l != nil {
				return l
			}
		}
	case c > 0:
		return n.Right.ceil(m, id)
	default:
		if l := n.Left.ceil(m, id); l != nil {
			return l
		}
	}
	return n
}

// An IntOperation is a function that operates on an IntInterface. If done is returned true, the
// IntOperation is indicating that no further work needs to be done and so the Do function should
// traverse no further.
type IntOperation func(IntInterface) (done bool)

// Do performs fn on all intervals stored in the tree. A boolean is returned indicating whether the
// Do traversal was interrupted by an IntOperation returning true. If fn alters stored intervals'
// end points, future tree operation behaviors are undefined.
func (t *IntTree) Do(fn IntOperation) bool {
	if t.Root == nil {
		return false
	}
	return t.Root.do(fn)
}

func (n *IntNode) do(fn IntOperation) (done bool) {
	if n.Left != nil {
		done = n.Left.do(fn)
		if done {
			return
		}
	}
	done = fn(n.Elem)
	if done {
		return
	}
	if n.Right != nil {
		done = n.Right.do(fn)
	}
	return
}

// DoReverse performs fn on all intervals stored in the tree, but in reverse of sort order. A boolean
// is returned indicating whether the Do traversal was interrupted by an IntOperation returning true.
// If fn alters stored intervals' end points, future tree operation behaviors are undefined.
func (t *IntTree) DoReverse(fn IntOperation) bool {
	if t.Root == nil {
		return false
	}
	return t.Root.doReverse(fn)
}

func (n *IntNode) doReverse(fn IntOperation) (done bool) {
	if n.Right != nil {
		done = n.Right.doReverse(fn)
		if done {
			return
		}
	}
	done = fn(n.Elem)
	if done {
		return
	}
	if n.Left != nil {
		done = n.Left.doReverse(fn)
	}
	return
}

// DoMatch performs fn on all intervals stored in the tree that match q according to Overlap, with
// q.Overlap() used to guide tree traversal, so DoMatching() will out perform Do() with a called
// conditional function if the condition is based on sort order, but can not be reliably used if
// the condition is independent of sort order. A boolean is returned indicating whether the Do
// traversal was interrupted by an IntOperation returning true. If fn alters stored intervals' end
// points, future tree operation behaviors are undefined.
func (t *IntTree) DoMatching(fn IntOperation, q IntOverlapper) bool {
	if t.Root != nil && q.Overlap(t.Root.Range) {
		return t.Root.doMatch(fn, q)
	}
	return false
}

func (n *IntNode) doMatch(fn IntOperation, q IntOverlapper) (done bool) {
	if n.Left != nil && q.Overlap(n.Left.Range) {
		done = n.Left.doMatch(fn, q)
		if done {
			return
		}
	}
	if q.Overlap(n.Interval) {
		done = fn(n.Elem)
		if done {
			return
		}
	}
	if n.Right != nil && q.Overlap(n.Right.Range) {
		done = n.Right.doMatch(fn, q)
	}
	return
}

// DoMatchReverse performs fn on all intervals stored in the tree that match q according to Overlap,
// with q.Overlap() used to guide tree traversal, so DoMatching() will out perform Do() with a called
// conditional function if the condition is based on sort order, but can not be reliably used if
// the condition is independent of sort order. A boolean is returned indicating whether the Do
// traversal was interrupted by an IntOperation returning true. If fn alters stored intervals' end
// points, future tree operation behaviors are undefined.
func (t *IntTree) DoMatchingReverse(fn IntOperation, q IntOverlapper) bool {
	if t.Root != nil && q.Overlap(t.Root.Range) {
		return t.Root.doMatch(fn, q)
	}
	return false
}

func (n *IntNode) doMatchReverse(fn IntOperation, q IntOverlapper) (done bool) {
	if n.Right != nil && q.Overlap(n.Right.Range) {
		done = n.Right.doMatchReverse(fn, q)
		if done {
			return
		}
	}
	if q.Overlap(n.Interval) {
		done = fn(n.Elem)
		if done {
			return
		}
	}
	if n.Left != nil && q.Overlap(n.Left.Range) {
		done = n.Left.doMatchReverse(fn, q)
	}
	return
}
