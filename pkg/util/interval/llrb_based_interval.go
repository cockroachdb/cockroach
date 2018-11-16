// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-biogo.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
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

// This code originated in the github.com/biogo/store/interval package.

package interval

import (
	"github.com/biogo/store/llrb"
)

// Operation LLRBMode of the underlying LLRB tree.
const (
	TD234 = iota
	BU23
)

func init() {
	if LLRBMode != TD234 && LLRBMode != BU23 {
		panic("interval: unknown LLRBMode")
	}
}

// A Node represents a node in a tree.
type llrbNode struct {
	Elem        Interface
	Range       Range
	Left, Right *llrbNode
	Color       llrb.Color
}

var _ Tree = (*llrbTree)(nil)

// llrbTree an interval tree based on an augmented Left-Leaning Red Black tree.
type llrbTree struct {
	Root       *llrbNode // root node of the tree.
	Count      int       // number of elements stored.
	Overlapper Overlapper
}

// newLLRBTree creates a new interval tree with the given overlapper function.
func newLLRBTree(overlapper Overlapper) *llrbTree {
	return &llrbTree{Overlapper: overlapper}
}

// Helper methods

// color returns the effect color of a Node. A nil node returns black.
func (n *llrbNode) color() llrb.Color {
	if n == nil {
		return llrb.Black
	}
	return n.Color
}

// maxRange returns the furthest right position held by the subtree
// rooted at root, assuming that the left and right nodes have correct
// range extents.
func maxRange(root, left, right *llrbNode) Comparable {
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
func (n *llrbNode) rotateLeft() (root *llrbNode) {
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
func (n *llrbNode) rotateRight() (root *llrbNode) {
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
func (n *llrbNode) flipColors() {
	// Assumes: n has two children.
	n.Color = !n.Color
	n.Left.Color = !n.Left.Color
	n.Right.Color = !n.Right.Color
}

// fixUp ensures that black link balance is correct, that red nodes lean left,
// and that 4 nodes are split in the case of BU23 and properly balanced in TD234.
func (n *llrbNode) fixUp(fast bool) *llrbNode {
	if !fast {
		n.adjustRange()
	}
	if n.Right.color() == llrb.Red {
		if LLRBMode == TD234 && n.Right.Left.color() == llrb.Red {
			n.Right = n.Right.rotateRight()
		}
		n = n.rotateLeft()
	}
	if n.Left.color() == llrb.Red && n.Left.Left.color() == llrb.Red {
		n = n.rotateRight()
	}
	if LLRBMode == BU23 && n.Left.color() == llrb.Red && n.Right.color() == llrb.Red {
		n.flipColors()
	}

	return n
}

// adjustRange sets the Range to the maximum extent of the children's Range
// spans and the node's Elem span.
func (n *llrbNode) adjustRange() {
	if n.Left == nil {
		n.Range.Start = n.Elem.Range().Start
	} else {
		n.Range.Start = n.Left.Range.Start
	}
	n.Range.End = maxRange(n, n.Left, n.Right)
}

func (n *llrbNode) moveRedLeft() *llrbNode {
	n.flipColors()
	if n.Right.Left.color() == llrb.Red {
		n.Right = n.Right.rotateRight()
		n = n.rotateLeft()
		n.flipColors()
		if LLRBMode == TD234 && n.Right.Right.color() == llrb.Red {
			n.Right = n.Right.rotateLeft()
		}
	}
	return n
}

func (n *llrbNode) moveRedRight() *llrbNode {
	n.flipColors()
	if n.Left.Left.color() == llrb.Red {
		n = n.rotateRight()
		n.flipColors()
	}
	return n
}

func (t *llrbTree) Len() int {
	return t.Count
}

func (t *llrbTree) Get(r Range) (o []Interface) {
	return t.GetWithOverlapper(r, t.Overlapper)
}

func (t *llrbTree) GetWithOverlapper(r Range, overlapper Overlapper) (o []Interface) {
	if t.Root != nil && overlapper.Overlap(r, t.Root.Range) {
		t.Root.doMatch(func(e Interface) (done bool) { o = append(o, e); return }, r, overlapper.Overlap)
	}
	return
}

func (t *llrbTree) AdjustRanges() {
	if t.Root == nil {
		return
	}
	t.Root.adjustRanges()
}

func (n *llrbNode) adjustRanges() {
	if n.Left != nil {
		n.Left.adjustRanges()
	}
	if n.Right != nil {
		n.Right.adjustRanges()
	}
	n.adjustRange()
}

func (t *llrbTree) Insert(e Interface, fast bool) (err error) {
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

func (n *llrbNode) insert(
	e Interface, min Comparable, id uintptr, fast bool,
) (root *llrbNode, d int) {
	if n == nil {
		return &llrbNode{Elem: e, Range: e.Range()}, 1
	} else if n.Elem == nil {
		n.Elem = e
		if !fast {
			n.adjustRange()
		}
		return n, 1
	}

	if LLRBMode == TD234 {
		if n.Left.color() == llrb.Red && n.Right.color() == llrb.Red {
			n.flipColors()
		}
	}

	switch c := min.Compare(n.Elem.Range().Start); {
	case c == 0:
		switch eid := n.Elem.ID(); {
		case id == eid:
			n.Elem = e
			if !fast {
				n.Range.End = e.Range().End
			}
		case id < eid:
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

	if LLRBMode == BU23 {
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

var _ = (*llrbTree)(nil).DeleteMin

// DeleteMin deletes the leftmost interval.
func (t *llrbTree) DeleteMin(fast bool) {
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

func (n *llrbNode) deleteMin(fast bool) (root *llrbNode, d int) {
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

var _ = (*llrbTree)(nil).DeleteMax

// DeleteMax deletes the rightmost interval.
func (t *llrbTree) DeleteMax(fast bool) {
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

func (n *llrbNode) deleteMax(fast bool) (root *llrbNode, d int) {
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

func (t *llrbTree) Delete(e Interface, fast bool) (err error) {
	r := e.Range()
	if err := rangeError(r); err != nil {
		return err
	}
	if t.Root == nil || !t.Overlapper.Overlap(r, t.Root.Range) {
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

func (n *llrbNode) delete(min Comparable, id uintptr, fast bool) (root *llrbNode, d int) {
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

var _ = (*llrbTree)(nil).Min

// Min returns the leftmost interval stored in the tree.
func (t *llrbTree) Min() Interface {
	if t.Root == nil {
		return nil
	}
	return t.Root.min().Elem
}

func (n *llrbNode) min() *llrbNode {
	for ; n.Left != nil; n = n.Left {
	}
	return n
}

var _ = (*llrbTree)(nil).Max

// Max returns the rightmost interval stored in the tree.
func (t *llrbTree) Max() Interface {
	if t.Root == nil {
		return nil
	}
	return t.Root.max().Elem
}

func (n *llrbNode) max() *llrbNode {
	for ; n.Right != nil; n = n.Right {
	}
	return n
}

var _ = (*llrbTree)(nil).Floor

// Floor returns the largest value equal to or less than the query q according to
// q.Start.Compare(), with ties broken by comparison of ID() values.
func (t *llrbTree) Floor(q Interface) (o Interface, err error) {
	if t.Root == nil {
		return
	}
	n := t.Root.floor(q.Range().Start, q.ID())
	if n == nil {
		return
	}
	return n.Elem, nil
}

func (n *llrbNode) floor(m Comparable, id uintptr) *llrbNode {
	if n == nil {
		return nil
	}
	switch c := m.Compare(n.Elem.Range().Start); {
	case c == 0:
		switch eid := n.Elem.ID(); {
		case id == eid:
			return n
		case id < eid:
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

var _ = (*llrbTree)(nil).Ceil

// Ceil returns the smallest value equal to or greater than the query q according to
// q.Start.Compare(), with ties broken by comparison of ID() values.
func (t *llrbTree) Ceil(q Interface) (o Interface, err error) {
	if t.Root == nil {
		return
	}
	n := t.Root.ceil(q.Range().Start, q.ID())
	if n == nil {
		return
	}
	return n.Elem, nil
}

func (n *llrbNode) ceil(m Comparable, id uintptr) *llrbNode {
	if n == nil {
		return nil
	}
	switch c := m.Compare(n.Elem.Range().Start); {
	case c == 0:
		switch eid := n.Elem.ID(); {
		case id == eid:
			return n
		case id > eid:
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

func (t *llrbTree) Do(fn Operation) bool {
	if t.Root == nil {
		return false
	}
	return t.Root.do(fn)
}

func (n *llrbNode) do(fn Operation) (done bool) {
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

var _ = (*llrbTree)(nil).DoReverse

// DoReverse performs fn on all intervals stored in the tree, but in reverse of sort order. A boolean
// is returned indicating whether the Do traversal was interrupted by an Operation returning true.
// If fn alters stored intervals' sort relationships, future tree operation behaviors are undefined.
func (t *llrbTree) DoReverse(fn Operation) bool {
	if t.Root == nil {
		return false
	}
	return t.Root.doReverse(fn)
}

func (n *llrbNode) doReverse(fn Operation) (done bool) {
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

var _ = (*llrbTree)(nil).DoMatchingReverse

func (t *llrbTree) DoMatching(fn Operation, r Range) bool {
	if t.Root != nil && t.Overlapper.Overlap(r, t.Root.Range) {
		return t.Root.doMatch(fn, r, t.Overlapper.Overlap)
	}
	return false
}

func (n *llrbNode) doMatch(fn Operation, r Range, overlaps func(Range, Range) bool) (done bool) {
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

var _ = (*llrbTree)(nil).DoMatchingReverse

// DoMatchingReverse performs fn on all intervals stored in the tree that match r according to
// t.Overlapper, with Overlapper() used to guide tree traversal, so DoMatching() will outperform
// Do() with a called conditional function if the condition is based on sort order, but can not
// be reliably used if the condition is independent of sort order. A boolean is returned indicating
// whether the Do traversal was interrupted by an Operation returning true. If fn alters stored
// intervals' sort relationships, future tree operation behaviors are undefined.
func (t *llrbTree) DoMatchingReverse(fn Operation, r Range) bool {
	if t.Root != nil && t.Overlapper.Overlap(r, t.Root.Range) {
		return t.Root.doMatchReverse(fn, r, t.Overlapper.Overlap)
	}
	return false
}

func (n *llrbNode) doMatchReverse(
	fn Operation, r Range, overlaps func(Range, Range) bool,
) (done bool) {
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

type llrbTreeIterator struct {
	stack []*llrbNode
}

func (ti *llrbTreeIterator) Next() (i Interface, ok bool) {
	if len(ti.stack) == 0 {
		return nil, false
	}
	n := ti.stack[len(ti.stack)-1]
	ti.stack = ti.stack[:len(ti.stack)-1]
	for r := n.Right; r != nil; r = r.Left {
		ti.stack = append(ti.stack, r)
	}
	return n.Elem, true
}

func (t *llrbTree) Iterator() TreeIterator {
	var ti llrbTreeIterator
	for n := t.Root; n != nil; n = n.Left {
		ti.stack = append(ti.stack, n)
	}
	return &ti
}

func (t *llrbTree) Clear() {
	t.Root = nil
	t.Count = 0
}

func (t *llrbTree) Clone() Tree {
	panic("unimplemented")
}
