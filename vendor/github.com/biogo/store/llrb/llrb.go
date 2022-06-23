// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package llrb implements Left-Leaning Red Black trees as described by Robert Sedgewick.
//
// More details relating to the implementation are available at the following locations:
//
//  http://www.cs.princeton.edu/~rs/talks/LLRB/LLRB.pdf
//  http://www.cs.princeton.edu/~rs/talks/LLRB/Java/RedBlackBST.java
//  http://www.teachsolaisgames.com/articles/balanced_left_leaning.html
package llrb

const (
	TD234 = iota
	BU23
)

// Operation mode of the LLRB tree.
const Mode = BU23

func init() {
	if Mode != TD234 && Mode != BU23 {
		panic("llrb: unknown mode")
	}
}

// A Comparable is a type that can be inserted into a Tree or used as a range
// or equality query on the tree,
type Comparable interface {
	// Compare returns a value indicating the sort order relationship between the
	// receiver and the parameter.
	//
	// Given c = a.Compare(b):
	//  c < 0 if a < b;
	//  c == 0 if a == b; and
	//  c > 0 if a > b.
	//
	Compare(Comparable) int
}

// A Color represents the color of a Node.
type Color bool

// String returns a string representation of a Color.
func (c Color) String() string {
	if c {
		return "Black"
	}
	return "Red"
}

const (
	// Red as false give us the defined behaviour that new nodes are red. Although this
	// is incorrect for the root node, that is resolved on the first insertion.
	Red   Color = false
	Black Color = true
)

// A Node represents a node in the LLRB tree.
type Node struct {
	Elem        Comparable
	Left, Right *Node
	Color       Color
}

// A Tree manages the root node of an LLRB tree. Public methods are exposed through this type.
type Tree struct {
	Root  *Node // Root node of the tree.
	Count int   // Number of elements stored.
}

// Helper methods

// color returns the effect color of a Node. A nil node returns black.
func (n *Node) color() Color {
	if n == nil {
		return Black
	}
	return n.Color
}

// (a,c)b -rotL-> ((a,)b,)c
func (n *Node) rotateLeft() (root *Node) {
	// Assumes: n has two children.
	root = n.Right
	n.Right = root.Left
	root.Left = n
	root.Color = n.Color
	n.Color = Red
	return
}

// (a,c)b -rotR-> (,(,c)b)a
func (n *Node) rotateRight() (root *Node) {
	// Assumes: n has two children.
	root = n.Left
	n.Left = root.Right
	root.Right = n
	root.Color = n.Color
	n.Color = Red
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
func (n *Node) fixUp() *Node {
	if n.Right.color() == Red {
		if Mode == TD234 && n.Right.Left.color() == Red {
			n.Right = n.Right.rotateRight()
		}
		n = n.rotateLeft()
	}
	if n.Left.color() == Red && n.Left.Left.color() == Red {
		n = n.rotateRight()
	}
	if Mode == BU23 && n.Left.color() == Red && n.Right.color() == Red {
		n.flipColors()
	}
	return n
}

func (n *Node) moveRedLeft() *Node {
	n.flipColors()
	if n.Right.Left.color() == Red {
		n.Right = n.Right.rotateRight()
		n = n.rotateLeft()
		n.flipColors()
		if Mode == TD234 && n.Right.Right.color() == Red {
			n.Right = n.Right.rotateLeft()
		}
	}
	return n
}

func (n *Node) moveRedRight() *Node {
	n.flipColors()
	if n.Left.Left.color() == Red {
		n = n.rotateRight()
		n.flipColors()
	}
	return n
}

// Len returns the number of elements stored in the Tree.
func (t *Tree) Len() int {
	return t.Count
}

// Get returns the first match of q in the Tree. If insertion without
// replacement is used, this is probably not what you want.
func (t *Tree) Get(q Comparable) Comparable {
	if t.Root == nil {
		return nil
	}
	n := t.Root.search(q)
	if n == nil {
		return nil
	}
	return n.Elem
}

func (n *Node) search(q Comparable) *Node {
	for n != nil {
		switch c := q.Compare(n.Elem); {
		case c == 0:
			return n
		case c < 0:
			n = n.Left
		default:
			n = n.Right
		}
	}

	return n
}

// Insert inserts the Comparable e into the Tree at the first match found
// with e or when a nil node is reached. Insertion without replacement can
// specified by ensuring that e.Compare() never returns 0. If insert without
// replacement is performed, a distinct query Comparable must be used that
// can return 0 with a Compare() call.
func (t *Tree) Insert(e Comparable) {
	var d int
	t.Root, d = t.Root.insert(e)
	t.Count += d
	t.Root.Color = Black
}

func (n *Node) insert(e Comparable) (root *Node, d int) {
	if n == nil {
		return &Node{Elem: e}, 1
	} else if n.Elem == nil {
		n.Elem = e
		return n, 1
	}

	if Mode == TD234 {
		if n.Left.color() == Red && n.Right.color() == Red {
			n.flipColors()
		}
	}

	switch c := e.Compare(n.Elem); {
	case c == 0:
		n.Elem = e
	case c < 0:
		n.Left, d = n.Left.insert(e)
	default:
		n.Right, d = n.Right.insert(e)
	}

	if n.Right.color() == Red && n.Left.color() == Black {
		n = n.rotateLeft()
	}
	if n.Left.color() == Red && n.Left.Left.color() == Red {
		n = n.rotateRight()
	}

	if Mode == BU23 {
		if n.Left.color() == Red && n.Right.color() == Red {
			n.flipColors()
		}
	}

	root = n

	return
}

// DeleteMin deletes the node with the minimum value in the tree. If insertion without
// replacement has been used, the left-most minimum will be deleted.
func (t *Tree) DeleteMin() {
	if t.Root == nil {
		return
	}
	var d int
	t.Root, d = t.Root.deleteMin()
	t.Count += d
	if t.Root == nil {
		return
	}
	t.Root.Color = Black
}

func (n *Node) deleteMin() (root *Node, d int) {
	if n.Left == nil {
		return nil, -1
	}
	if n.Left.color() == Black && n.Left.Left.color() == Black {
		n = n.moveRedLeft()
	}
	n.Left, d = n.Left.deleteMin()

	root = n.fixUp()

	return
}

// DeleteMax deletes the node with the maximum value in the tree. If insertion without
// replacement has been used, the right-most maximum will be deleted.
func (t *Tree) DeleteMax() {
	if t.Root == nil {
		return
	}
	var d int
	t.Root, d = t.Root.deleteMax()
	t.Count += d
	if t.Root == nil {
		return
	}
	t.Root.Color = Black
}

func (n *Node) deleteMax() (root *Node, d int) {
	if n.Left != nil && n.Left.color() == Red {
		n = n.rotateRight()
	}
	if n.Right == nil {
		return nil, -1
	}
	if n.Right.color() == Black && n.Right.Left.color() == Black {
		n = n.moveRedRight()
	}
	n.Right, d = n.Right.deleteMax()

	root = n.fixUp()

	return
}

// Delete deletes the node that matches e according to Compare(). Note that Compare must
// identify the target node uniquely and in cases where non-unique keys are used,
// attributes used to break ties must be used to determine tree ordering during insertion.
func (t *Tree) Delete(e Comparable) {
	if t.Root == nil {
		return
	}
	var d int
	t.Root, d = t.Root.delete(e)
	t.Count += d
	if t.Root == nil {
		return
	}
	t.Root.Color = Black
}

func (n *Node) delete(e Comparable) (root *Node, d int) {
	if e.Compare(n.Elem) < 0 {
		if n.Left != nil {
			if n.Left.color() == Black && n.Left.Left.color() == Black {
				n = n.moveRedLeft()
			}
			n.Left, d = n.Left.delete(e)
		}
	} else {
		if n.Left.color() == Red {
			n = n.rotateRight()
		}
		if n.Right == nil && e.Compare(n.Elem) == 0 {
			return nil, -1
		}
		if n.Right != nil {
			if n.Right.color() == Black && n.Right.Left.color() == Black {
				n = n.moveRedRight()
			}
			if e.Compare(n.Elem) == 0 {
				n.Elem = n.Right.min().Elem
				n.Right, d = n.Right.deleteMin()
			} else {
				n.Right, d = n.Right.delete(e)
			}
		}
	}

	root = n.fixUp()

	return
}

// Return the minimum value stored in the tree. This will be the left-most minimum value if
// insertion without replacement has been used.
func (t *Tree) Min() Comparable {
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

// Return the maximum value stored in the tree. This will be the right-most maximum value if
// insertion without replacement has been used.
func (t *Tree) Max() Comparable {
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

// Floor returns the greatest value equal to or less than the query q according to q.Compare().
func (t *Tree) Floor(q Comparable) Comparable {
	if t.Root == nil {
		return nil
	}
	n := t.Root.floor(q)
	if n == nil {
		return nil
	}
	return n.Elem
}

func (n *Node) floor(q Comparable) *Node {
	if n == nil {
		return nil
	}
	switch c := q.Compare(n.Elem); {
	case c == 0:
		return n
	case c < 0:
		return n.Left.floor(q)
	default:
		if r := n.Right.floor(q); r != nil {
			return r
		}
	}
	return n
}

// Ceil returns the smallest value equal to or greater than the query q according to q.Compare().
func (t *Tree) Ceil(q Comparable) Comparable {
	if t.Root == nil {
		return nil
	}
	n := t.Root.ceil(q)
	if n == nil {
		return nil
	}
	return n.Elem
}

func (n *Node) ceil(q Comparable) *Node {
	if n == nil {
		return nil
	}
	switch c := q.Compare(n.Elem); {
	case c == 0:
		return n
	case c > 0:
		return n.Right.ceil(q)
	default:
		if l := n.Left.ceil(q); l != nil {
			return l
		}
	}
	return n
}

// An Operation is a function that operates on a Comparable. If done is returned true, the
// Operation is indicating that no further work needs to be done and so the Do function should
// traverse no further.
type Operation func(Comparable) (done bool)

// Do performs fn on all values stored in the tree. A boolean is returned indicating whether the
// Do traversal was interrupted by an Operation returning true. If fn alters stored values' sort
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

// DoReverse performs fn on all values stored in the tree, but in reverse of sort order. A boolean
// is returned indicating whether the Do traversal was interrupted by an Operation returning true.
// If fn alters stored values' sort relationships, future tree operation behaviors are undefined.
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

// DoRange performs fn on all values stored in the tree over the interval [from, to) from left
// to right. If to is less than from DoRange will panic. A boolean is returned indicating whether
// the Do traversal was interrupted by an Operation returning true. If fn alters stored values'
// sort relationships future tree operation behaviors are undefined.
func (t *Tree) DoRange(fn Operation, from, to Comparable) bool {
	if t.Root == nil {
		return false
	}
	if from.Compare(to) > 0 {
		panic("llrb: inverted range")
	}
	return t.Root.doRange(fn, from, to)
}

func (n *Node) doRange(fn Operation, lo, hi Comparable) (done bool) {
	lc, hc := lo.Compare(n.Elem), hi.Compare(n.Elem)
	if lc <= 0 && n.Left != nil {
		done = n.Left.doRange(fn, lo, hi)
		if done {
			return
		}
	}
	if lc <= 0 && hc > 0 {
		done = fn(n.Elem)
		if done {
			return
		}
	}
	if hc > 0 && n.Right != nil {
		done = n.Right.doRange(fn, lo, hi)
	}
	return
}

// DoRangeReverse performs fn on all values stored in the tree over the interval (to, from] from
// right to left. If from is less than to DoRange will panic. A boolean is returned indicating
// whether the Do traversal was interrupted by an Operation returning true. If fn alters stored
// values' sort relationships future tree operation behaviors are undefined.
func (t *Tree) DoRangeReverse(fn Operation, from, to Comparable) bool {
	if t.Root == nil {
		return false
	}
	if from.Compare(to) < 0 {
		panic("llrb: inverted range")
	}
	return t.Root.doRangeReverse(fn, from, to)
}

func (n *Node) doRangeReverse(fn Operation, hi, lo Comparable) (done bool) {
	lc, hc := lo.Compare(n.Elem), hi.Compare(n.Elem)
	if hc > 0 && n.Right != nil {
		done = n.Right.doRangeReverse(fn, hi, lo)
		if done {
			return
		}
	}
	if lc <= 0 && hc > 0 {
		done = fn(n.Elem)
		if done {
			return
		}
	}
	if lc <= 0 && n.Left != nil {
		done = n.Left.doRangeReverse(fn, hi, lo)
	}
	return
}

// DoMatch performs fn on all values stored in the tree that match q according to Compare, with
// q.Compare() used to guide tree traversal, so DoMatching() will out perform Do() with a called
// conditional function if the condition is based on sort order, but can not be reliably used if
// the condition is independent of sort order. A boolean is returned indicating whether the Do
// traversal was interrupted by an Operation returning true. If fn alters stored values' sort
// relationships, future tree operation behaviors are undefined.
func (t *Tree) DoMatching(fn Operation, q Comparable) bool {
	if t.Root == nil {
		return false
	}
	return t.Root.doMatch(fn, q)
}

func (n *Node) doMatch(fn Operation, q Comparable) (done bool) {
	c := q.Compare(n.Elem)
	if c <= 0 && n.Left != nil {
		done = n.Left.doMatch(fn, q)
		if done {
			return
		}
	}
	if c == 0 {
		done = fn(n.Elem)
		if done {
			return
		}
	}
	if c >= 0 && n.Right != nil {
		done = n.Right.doMatch(fn, q)
	}
	return
}
