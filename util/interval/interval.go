// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package interval implements an interval tree based on an augmented
// Left-Leaning Red Black tree.
package interval

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/biogo/store/llrb"
)

// Operation mode of the underlying LLRB tree.
const (
	TD234 = iota
	BU23
)

func init() {
	if Mode != TD234 && Mode != BU23 {
		panic("interval: unknown mode")
	}
}

// ErrInvertedRange is returned if an interval is used where the start value is greater
// than the end value.
var ErrInvertedRange = errors.New("interval: inverted range")

// ErrEmptyRange is returned if an interval is used where the start value is equal
// to the end value.
var ErrEmptyRange = errors.New("interval: empty range")

func rangeError(r Range) error {
	switch r.Start.Compare(r.End) {
	case 1:
		return ErrInvertedRange
	case 0:
		return ErrEmptyRange
	default:
		return nil
	}
}

// A Range is a type that describes the basic characteristics of an interval.
type Range struct {
	Start, End Comparable
}

// OverlapInclusive returns whether the two provided ranges overlap. It
// defines overlapping as a pair of ranges that share a segment, with both
// start and end keys treated as inclusive values.
func (r Range) OverlapInclusive(other Range) bool {
	return r.End.Compare(other.Start) >= 0 && r.Start.Compare(other.End) <= 0
}

// OverlapExclusive returns whether the two provided ranges overlap. It defines
// overlapping as a pair of ranges that share a segment of the keyspace, with the
// start keys treated as inclusive and the end keys treated as exclusive.
func (r Range) OverlapExclusive(other Range) bool {
	return r.End.Compare(other.Start) > 0 && r.Start.Compare(other.End) < 0
}

// Equal returns whether the two ranges are equal.
func (r Range) Equal(other Range) bool {
	return r.End.Equal(other.Start) && r.Start.Equal(other.End)
}

// String implements the Stringer interface.
func (r Range) String() string {
	return fmt.Sprintf("[%x-%x)", r.Start, r.End)
}

// An Interface is a type that can be inserted into a Tree.
type Interface interface {
	Range() Range
	// Returns a unique ID for the element.
	// TODO(nvanbenschoten) Should this be changed to an int64?
	ID() uintptr
}

// A Comparable is a type that describes the ends of a Range.
type Comparable []byte

// Compare returns a value indicating the sort order relationship between the
// receiver and the parameter.
//
// Given c = a.Compare(b):
//  c < 0 if a < b;
//  c == 0 if a == b; and
//  c > 0 if a > b.
//
func (c Comparable) Compare(o Comparable) int {
	return bytes.Compare(c, o)
}

// Equal returns a boolean indicating if the given comparables are equal to
// each other. Note that this has measurably better performance than
// Compare() == 0, so it should be used when only equality state is needed.
func (c Comparable) Equal(o Comparable) bool {
	return bytes.Equal(c, o)
}

// A Node represents a node in a Tree.
type Node struct {
	Elem        Interface
	Range       Range
	Left, Right *Node
	Color       llrb.Color
}

// A Tree manages the root node of an interval tree. Public methods are exposed through this type.
type Tree struct {
	Root       *Node                   // root node of the tree.
	Count      int                     // number of elements stored.
	Overlapper func(Range, Range) bool // determines how to define Range overlap.
}

// Helper methods

// color returns the effect color of a Node. A nil node returns black.
func (n *Node) color() llrb.Color {
	if n == nil {
		return llrb.Black
	}
	return n.Color
}

// maxRange returns the furthest right position held by the subtree
// rooted at root, assuming that the left and right nodes have correct
// range extents.
func maxRange(root, left, right *Node) Comparable {
	end := root.Elem.Range().End
	if left != nil && left.Range.End.Compare(end) > 0 {
		end = left.Range.End
	}
	if right != nil && right.Range.End.Compare(end) > 0 {
		end = right.Range.End
	}
	return end
}

// (a,c)b -rotL-> ((a,)b,)c
func (n *Node) rotateLeft() (root *Node) {
	// Assumes: n has a right child.
	root = n.Right
	n.Right = root.Left
	root.Left = n
	root.Color = n.Color
	n.Color = llrb.Red

	root.Left.Range.End = maxRange(root.Left, root.Left.Left, root.Left.Right)
	if root.Left == nil {
		root.Range.Start = root.Elem.Range().Start
	} else {
		root.Range.Start = root.Left.Range.Start
	}
	root.Range.End = maxRange(root, root.Left, root.Right)

	return
}

// (a,c)b -rotR-> (,(,c)b)a
func (n *Node) rotateRight() (root *Node) {
	// Assumes: n has a left child.
	root = n.Left
	n.Left = root.Right
	root.Right = n
	root.Color = n.Color
	n.Color = llrb.Red

	if root.Right.Left == nil {
		root.Right.Range.Start = root.Right.Elem.Range().Start
	} else {
		root.Right.Range.Start = root.Right.Left.Range.Start
	}
	root.Right.Range.End = maxRange(root.Right, root.Right.Left, root.Right.Right)
	root.Range.End = maxRange(root, root.Left, root.Right)

	return
}

// (aR,cR)bB -flipC-> (aB,cB)bR | (aB,cB)bR -flipC-> (aR,cR)bB
func (n *Node) flipColors() {
	// Assumes: n has two children.
	n.Color = !n.Color
	n.Left.Color = !n.Left.Color
	n.Right.Color = !n.Right.Color
}

// fixUp ensures that black link balance is correct, that red nodes lean left,
// and that 4 nodes are split in the case of BU23 and properly balanced in TD234.
func (n *Node) fixUp(fast bool) *Node {
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
func (n *Node) adjustRange() {
	if n.Left == nil {
		n.Range.Start = n.Elem.Range().Start
	} else {
		n.Range.Start = n.Left.Range.Start
	}
	n.Range.End = maxRange(n, n.Left, n.Right)
}

func (n *Node) moveRedLeft() *Node {
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

func (n *Node) moveRedRight() *Node {
	n.flipColors()
	if n.Left.Left.color() == llrb.Red {
		n = n.rotateRight()
		n.flipColors()
	}
	return n
}

// Len returns the number of intervals stored in the Tree.
func (t *Tree) Len() int {
	return t.Count
}

// Get returns a slice of Interfaces that overlap r in the Tree.
func (t *Tree) Get(r Range) (o []Interface) {
	return t.GetWithOverlapper(r, t.Overlapper)
}

// GetWithOverlapper returns a slice of Interfaces that overlap r
// in the Tree using the provided overlapper function.
func (t *Tree) GetWithOverlapper(r Range, overlapper func(Range, Range) bool) (o []Interface) {
	if t.Root != nil && overlapper(r, t.Root.Range) {
		t.Root.doMatch(func(e Interface) (done bool) { o = append(o, e); return }, r, overlapper)
	}
	return
}

// AdjustRanges fixes range fields for all Nodes in the Tree. This must be called
// before Get or DoMatching* is used if fast insertion or deletion has been performed.
func (t *Tree) AdjustRanges() {
	if t.Root == nil {
		return
	}
	t.Root.adjustRanges()
}

func (n *Node) adjustRanges() {
	if n.Left != nil {
		n.Left.adjustRanges()
	}
	if n.Right != nil {
		n.Right.adjustRanges()
	}
	n.adjustRange()
}

// Insert inserts the Interface e into the Tree. Insertions may replace
// existing stored intervals.
func (t *Tree) Insert(e Interface, fast bool) (err error) {
	r := e.Range()
	if err := rangeError(r); err != nil {
		return err
	}
	var d int
	t.Root, d = t.Root.insert(e, r.Start, e.ID(), fast)
	t.Count += d
	t.Root.Color = llrb.Black
	return
}

func (n *Node) insert(e Interface, min Comparable, id uintptr, fast bool) (root *Node, d int) {
	if n == nil {
		return &Node{Elem: e, Range: e.Range()}, 1
	} else if n.Elem == nil {
		n.Elem = e
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

	switch c := min.Compare(n.Elem.Range().Start); {
	case c == 0:
		switch cid := id - n.Elem.ID(); {
		case cid == 0:
			n.Elem = e
			if !fast {
				n.Range.End = e.Range().End
			}
		case cid < 0:
			n.Left, d = n.Left.insert(e, min, id, fast)
		default:
			n.Right, d = n.Right.insert(e, min, id, fast)
		}
	case c < 0:
		n.Left, d = n.Left.insert(e, min, id, fast)
	default:
		n.Right, d = n.Right.insert(e, min, id, fast)
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
func (t *Tree) DeleteMin(fast bool) {
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

func (n *Node) deleteMin(fast bool) (root *Node, d int) {
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
func (t *Tree) DeleteMax(fast bool) {
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

func (n *Node) deleteMax(fast bool) (root *Node, d int) {
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

// Delete deletes the element e if it exists in the Tree.
func (t *Tree) Delete(e Interface, fast bool) (err error) {
	r := e.Range()
	if err := rangeError(r); err != nil {
		return err
	}
	if t.Root == nil || !t.Overlapper(r, t.Root.Range) {
		return
	}
	var d int
	t.Root, d = t.Root.delete(r.Start, e.ID(), fast)
	t.Count += d
	if t.Root == nil {
		return
	}
	t.Root.Color = llrb.Black
	return
}

func (n *Node) delete(min Comparable, id uintptr, fast bool) (root *Node, d int) {
	if p := min.Compare(n.Elem.Range().Start); p < 0 || (p == 0 && id < n.Elem.ID()) {
		if n.Left != nil {
			if n.Left.color() == llrb.Black && n.Left.Left.color() == llrb.Black {
				n = n.moveRedLeft()
			}
			n.Left, d = n.Left.delete(min, id, fast)
			if n.Left == nil {
				n.Range.Start = n.Elem.Range().Start
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
				n.Elem = n.Right.min().Elem
				n.Right, d = n.Right.deleteMin(fast)
			} else {
				n.Right, d = n.Right.delete(min, id, fast)
			}
			if n.Right == nil {
				n.Range.End = n.Elem.Range().End
			}
		}
	}

	root = n.fixUp(fast)

	return
}

// Min returns the left-most interval stored in the tree.
func (t *Tree) Min() Interface {
	if t.Root == nil {
		return nil
	}
	return t.Root.min().Elem
}

func (n *Node) min() *Node {
	for ; n.Left != nil; n = n.Left {
	}
	return n
}

// Max returns the right-most interval stored in the tree.
func (t *Tree) Max() Interface {
	if t.Root == nil {
		return nil
	}
	return t.Root.max().Elem
}

func (n *Node) max() *Node {
	for ; n.Right != nil; n = n.Right {
	}
	return n
}

// Floor returns the largest value equal to or less than the query q according to
// q.Start.Compare(), with ties broken by comparison of ID() values.
func (t *Tree) Floor(q Interface) (o Interface, err error) {
	if t.Root == nil {
		return
	}
	n := t.Root.floor(q.Range().Start, q.ID())
	if n == nil {
		return
	}
	return n.Elem, nil
}

func (n *Node) floor(m Comparable, id uintptr) *Node {
	if n == nil {
		return nil
	}
	switch c := m.Compare(n.Elem.Range().Start); {
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
// q.Start.Compare(), with ties broken by comparison of ID() values.
func (t *Tree) Ceil(q Interface) (o Interface, err error) {
	if t.Root == nil {
		return
	}
	n := t.Root.ceil(q.Range().Start, q.ID())
	if n == nil {
		return
	}
	return n.Elem, nil
}

func (n *Node) ceil(m Comparable, id uintptr) *Node {
	if n == nil {
		return nil
	}
	switch c := m.Compare(n.Elem.Range().Start); {
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

// An Operation is a function that operates on an Interface. If done is returned true, the
// Operation is indicating that no further work needs to be done and so the Do function should
// traverse no further.
type Operation func(Interface) (done bool)

// Do performs fn on all intervals stored in the tree. A boolean is returned indicating whether the
// Do traversal was interrupted by an Operation returning true. If fn alters stored intervals' sort
// relationships, future tree operation behaviors are undefined.
func (t *Tree) Do(fn Operation) bool {
	if t.Root == nil {
		return false
	}
	return t.Root.do(fn)
}

func (n *Node) do(fn Operation) (done bool) {
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
// is returned indicating whether the Do traversal was interrupted by an Operation returning true.
// If fn alters stored intervals' sort relationships, future tree operation behaviors are undefined.
func (t *Tree) DoReverse(fn Operation) bool {
	if t.Root == nil {
		return false
	}
	return t.Root.doReverse(fn)
}

func (n *Node) doReverse(fn Operation) (done bool) {
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

// DoMatching performs fn on all intervals stored in the tree that match r according to
// t.Overlapper, with Overlapper() used to guide tree traversal, so DoMatching() will
// outperform Do() with a called conditional function if the condition is based on sort
// order, but can not be reliably used if the condition is independent of sort order. A
// boolean is returned indicating whether the Do traversal was interrupted by an Operation
// returning true. If fn alters stored intervals' sort relationships, future tree operation
// behaviors are undefined.
func (t *Tree) DoMatching(fn Operation, r Range) bool {
	if t.Root != nil && t.Overlapper(r, t.Root.Range) {
		return t.Root.doMatch(fn, r, t.Overlapper)
	}
	return false
}

func (n *Node) doMatch(fn Operation, r Range, overlaps func(Range, Range) bool) (done bool) {
	if n.Left != nil && overlaps(r, n.Left.Range) {
		done = n.Left.doMatch(fn, r, overlaps)
		if done {
			return
		}
	}
	if overlaps(r, n.Elem.Range()) {
		done = fn(n.Elem)
		if done {
			return
		}
	}
	if n.Right != nil && overlaps(r, n.Right.Range) {
		done = n.Right.doMatch(fn, r, overlaps)
	}
	return
}

// DoMatchingReverse performs fn on all intervals stored in the tree that match r according to
// t.Overlapper, with Overlapper() used to guide tree traversal, so DoMatching() will outperform
// Do() with a called conditional function if the condition is based on sort order, but can not
// be reliably used if the condition is independent of sort order. A boolean is returned indicating
// whether the Do traversal was interrupted by an Operation returning true. If fn alters stored
// intervals' sort relationships, future tree operation behaviors are undefined.
func (t *Tree) DoMatchingReverse(fn Operation, r Range) bool {
	if t.Root != nil && t.Overlapper(r, t.Root.Range) {
		return t.Root.doMatchReverse(fn, r, t.Overlapper)
	}
	return false
}

func (n *Node) doMatchReverse(fn Operation, r Range, overlaps func(Range, Range) bool) (done bool) {
	if n.Right != nil && overlaps(r, n.Right.Range) {
		done = n.Right.doMatchReverse(fn, r, overlaps)
		if done {
			return
		}
	}
	if overlaps(r, n.Elem.Range()) {
		done = fn(n.Elem)
		if done {
			return
		}
	}
	if n.Left != nil && overlaps(r, n.Left.Range) {
		done = n.Left.doMatchReverse(fn, r, overlaps)
	}
	return
}
