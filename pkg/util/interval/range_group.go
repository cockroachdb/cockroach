// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package interval

import (
	"bytes"
	"container/list"
	"fmt"
)

// RangeGroup represents a set of possibly disjointed Ranges. The
// interface exposes methods to manipulate the group by adding and
// subtracting Ranges. All methods requiring a Range will panic
// if the provided range is inverted or empty.
//
// One use case of the interface is to add ranges to the group and
// observe whether the addition increases the size of the group or
// not, indicating whether the new range's interval is redundant, or
// if it is needed for the full composition of the group. Because
// the RangeGroup builds as more ranges are added, insertion order of
// the ranges is critical. For instance, if two identical ranges are
// added, only the first to be added with Add will return true, as it
// will be the only one to expand the group.
//
// Another use case of the interface is to add and subtract ranges as
// needed to the group, allowing the internals of the implementation
// to coalesce and split ranges when needed to factor the group to
// its minimum number of disjoint ranges.
type RangeGroup interface {
	// Add will attempt to add the provided Range to the RangeGroup,
	// returning whether the addition increased the range of the group
	// or not.
	Add(Range) bool
	// Sub will attempt to remove the provided Range from the RangeGroup,
	// returning whether the subtraction reduced the range of the group
	// or not.
	Sub(Range) bool
	// Clear clears all ranges from the RangeGroup, resetting it to be
	// used again.
	Clear()
	// Overlaps returns whether the provided Range is partially contained
	// within the group of Ranges in the RangeGroup.
	Overlaps(Range) bool
	// Encloses returns whether the provided Range is fully contained
	// within the group of Ranges in the RangeGroup.
	Encloses(Range) bool
	// ForEach calls the provided function with each Range stored in
	// the group. An error is returned indicating whether the callback
	// function saw an error, whereupon the Range iteration will halt
	// (potentially prematurely) and the error will be returned from ForEach
	// itself. If no error is returned from the callback, the method
	// will visit all Ranges in the group before returning a nil error.
	ForEach(func(Range) error) error
	// Iterator returns an iterator to visit each Range stored in the
	// group, in-order. It is not safe to mutate the RangeGroup while
	// iteration is being performed.
	Iterator() RangeGroupIterator
	// Len returns the number of Ranges currently within the RangeGroup.
	// This will always be equal to or less than the number of ranges added,
	// as ranges that overlap will merge to produce a single larger range.
	Len() int
	fmt.Stringer
}

// RangeGroupIterator is an iterator that walks in-order over a RangeGroup.
type RangeGroupIterator interface {
	// Next returns the next Range in the RangeGroup. It returns false
	// if there are no more Ranges.
	Next() (Range, bool)
}

const rangeListNodeBucketSize = 8

type rangeListNode struct {
	slots [rangeListNodeBucketSize]Range // ordered, non-overlapping
	len   int
}

func newRangeListNodeWithRange(r Range) *rangeListNode {
	var n rangeListNode
	n.push(r)
	return &n
}

func (n *rangeListNode) push(r Range) {
	n.slots[n.len] = r
	n.len++
}

func (n *rangeListNode) full() bool {
	return n.len == len(n.slots)
}

func (n *rangeListNode) min() Comparable {
	return n.slots[0].Start
}

func (n *rangeListNode) max() Comparable {
	return n.slots[n.len-1].End
}

// findIdx finds the upper-bound slot index that the provided range should fit
// in the rangeListNode. It also returns whether the slot is currently occupied
// by an overlapping range.
func (n *rangeListNode) findIdx(r Range, inclusive bool) (int, bool) {
	overlapFn := overlapsExclusive
	passedCmp := 0
	if inclusive {
		overlapFn = overlapsInclusive
		passedCmp = -1
	}
	for i, nr := range n.slots[:n.len] {
		switch {
		case overlapFn(nr, r):
			return i, true
		case r.End.Compare(nr.Start) <= passedCmp:
			// Past where overlapping ranges would be.
			return i, false
		}
	}
	return n.len, false
}

// rangeList is an implementation of a RangeGroup using a bucketted linked list
// to sequentially order non-overlapping ranges.
//
// rangeList is not safe for concurrent use by multiple goroutines.
type rangeList struct {
	ll  list.List
	len int
}

// NewRangeList constructs a linked-list backed RangeGroup.
func NewRangeList() RangeGroup {
	var rl rangeList
	rl.ll.Init()
	rl.ll.PushFront(&rangeListNode{})
	return &rl
}

// findNode returns the upper-bound node that the range would be bucketted in,
// along with that node's previous element. It also returns whether the range
// overlaps with the bounds of the node.
func (rl *rangeList) findNode(r Range, inclusive bool) (prev, cur *list.Element, inCur bool) {
	if err := rangeError(r); err != nil {
		panic(err)
	}
	reachedCmp := 1
	passedCmp := 0
	if inclusive {
		reachedCmp = 0
		passedCmp = -1
	}
	for e := rl.ll.Front(); e != nil; e = e.Next() {
		n := e.Value.(*rangeListNode)
		if n.len == 0 {
			// The node is empty. This must be the last node in the list.
			return prev, e, false
		}
		// Check if the range starts at a value less than (or equal to, for
		// inclusive) the maximum value in this node. This is what we want.
		if n.max().Compare(r.Start) >= reachedCmp {
			// Determine whether the range overlap the node's bounds.
			inCur := n.min().Compare(r.End) <= passedCmp
			return prev, e, inCur
		}
		prev = e
	}
	return prev, nil, false
}

// insertAtIdx inserts the provided range at the specified index in the node.
// It performs any necessary slot movement to keep the ranges ordered.
//
// Note: e.Value is expected to be n, but we want to avoid repeated type
// assertions so both are taken as arguments.
func (rl *rangeList) insertAtIdx(e *list.Element, n *rangeListNode, r Range, i int) {
	if n.full() {
		// If the current node is full, we're going to need to shift off a range
		// from one of the slots into a different node. If i is not pointing
		// past the end of the range, we need to shift off the range currently
		// in the last slot in this node. If i is pointing past the end of the
		// range, we can just shift the new range to a different node without
		// making any changes to this node.
		toShift := n.slots[n.len-1]
		noLocalChanges := false
		if i == n.len {
			toShift = r

			// We're going to add range r to a different node, so there will be
			// no local changes to this node.
			noLocalChanges = true
		}

		// Check if the next node has room. Note that we only call insertAtIdx
		// recursively on the next node if it is not full. We don't want to
		// shift recursively all the way down the list. Instead, we'll just pay
		// the constant cost below of inserting a fresh node in-between the
		// current and next node.
		next := e.Next()
		insertedInNext := false
		if next != nil {
			nextN := next.Value.(*rangeListNode)
			if !nextN.full() {
				rl.insertAtIdx(next, nextN, toShift, 0)
				insertedInNext = true
			}
		}
		if !insertedInNext {
			newN := newRangeListNodeWithRange(toShift)
			rl.ll.InsertAfter(newN, e)
			rl.len++
		}

		if noLocalChanges {
			return
		}
	} else {
		n.len++
		rl.len++
	}
	// Shift all others over and copy the new range in. Because of
	// the n.full check, we know that we'll have at least one free
	// slot open at the end.
	copy(n.slots[i+1:n.len], n.slots[i:n.len-1])
	n.slots[i] = r
}

// Add implements RangeGroup. It iterates over the current ranges in the
// rangeList to find which overlap with the new range. If there is no
// overlap, the new range will be added and the function will return true.
// If there is some overlap, the function will return true if the new
// range increases the range of the rangeList, in which case it will be
// added to the list, and false if it does not increase the range, in which
// case it won't be added. If the range is added, the function will also attempt
// to merge any ranges within the list that now overlap.
func (rl *rangeList) Add(r Range) bool {
	prev, cur, inCur := rl.findNode(r, true /* inclusive */)

	var prevN *rangeListNode
	if prev != nil {
		prevN = prev.Value.(*rangeListNode)
	}
	if !inCur && prevN != nil && !prevN.full() {
		// There is a previous node. Add the range to the end of that node.
		prevN.push(r)
		rl.len++
		return true
	}

	if cur != nil {
		n := cur.Value.(*rangeListNode)
		i, ok := n.findIdx(r, true /* inclusive */)
		if !ok {
			// The range can't be merged with any existing ranges, but should
			// instead be inserted at slot i. This may force us to shift over
			// other slots.
			rl.insertAtIdx(cur, n, r, i)
			return true
		}

		// If a current range fully contains the new range, no need to add it.
		nr := n.slots[i]
		if contains(nr, r) {
			return false
		}

		// Merge as many ranges as possible and replace old range. All merges
		// will be made into n.slots[i] because adding a range to the RangeGroup
		// can result in only at most one group of ranges being merged.
		//
		// For example:
		//  existing ranges :  ---- -----    ----  ----  ----
		//  new range       :          --------------
		//  resulting ranges:  ---- -------------------  ----
		//
		// In this example, n.slots[i] is the first existing range that overlaps
		// with the new range.
		newR := merge(nr, r)
		n.slots[i] = newR

		// Each iteration attempts to merge all of the ranges in a rangeListNode.
		mergeElem := cur
		origNode := true
		for {
			mergeN := mergeElem.Value.(*rangeListNode)
			origLen := mergeN.len

			// mergeState is the slot index to begin merging from
			mergeStart := 0
			if origNode {
				mergeStart = i + 1
			}

			// Each iteration attempts to merge a single range into the current
			// merge batch.
			j := mergeStart
			for ; j < origLen; j++ {
				mergeR := mergeN.slots[j]

				if overlapsInclusive(newR, mergeR) {
					newR = merge(newR, mergeR)
					n.slots[i] = newR
					mergeN.len--
					rl.len--
				} else {
					// If the ranges don't overlap, that means index j and up
					// are still needed in the current node. Shift these over.
					copy(mergeN.slots[mergeStart:mergeStart+origLen-j], mergeN.slots[j:origLen])
					return true
				}
			}

			// If we didn't break, that means that all of the slots including
			// and after mergeStart in the current node were merged. Continue
			// onto the next node.
			nextE := mergeElem.Next()
			if !origNode {
				// If this is not the current node, we can delete it
				// completely.
				rl.ll.Remove(mergeElem)
			}
			if nextE == nil {
				return true
			}
			mergeElem = nextE
			origNode = false
		}
	}

	// The new range couldn't be added to the previous or the current node.
	// We'll have to create a new node for the range.
	n := newRangeListNodeWithRange(r)
	if prevN != nil {
		// There is a previous node and it is full.
		rl.ll.InsertAfter(n, prev)
	} else {
		// There is no previous node. Add the range to a new node in the front
		// of the list.
		rl.ll.PushFront(n)
	}
	rl.len++
	return true
}

// Sub implements RangeGroup. It iterates over the current ranges in the
// rangeList to find which overlap with the range to subtract. For all
// ranges that overlap with the provided range, the overlapping segment of
// the range is removed. If the provided range fully contains a range in
// the rangeList, the range in the rangeList will be removed. The method
// returns whether the subtraction resulted in any decrease to the size
// of the RangeGroup.
func (rl *rangeList) Sub(r Range) bool {
	_, cur, inCur := rl.findNode(r, false /* inclusive */)
	if !inCur {
		// The range does not overlap any nodes. Nothing to do.
		return false
	}

	n := cur.Value.(*rangeListNode)
	i, ok := n.findIdx(r, false /* inclusive */)
	if !ok {
		// The range does not overlap any ranges in the node. Nothing to do.
		return false
	}

	for {
		nr := n.slots[i]
		if !overlapsExclusive(nr, r) {
			// The range does not overlap nr so stop trying to subtract. The
			// findIdx check above guarantees that this will never be the case
			// for the first iteration of this loop.
			return true
		}

		sCmp := nr.Start.Compare(r.Start)
		eCmp := nr.End.Compare(r.End)
		delStart := sCmp >= 0
		delEnd := eCmp <= 0

		switch {
		case delStart && delEnd:
			// Remove the entire range.
			n.len--
			rl.len--
			if n.len == 0 {
				// Move to the next node, removing the current node as long as
				// it's not the only one in the list.
				i = 0
				next := cur.Next()
				if rl.len > 0 {
					rl.ll.Remove(cur)
				}
				if next == nil {
					return true
				}
				cur = next
				n = cur.Value.(*rangeListNode)
				continue
			} else {
				// Replace the current Range.
				copy(n.slots[i:n.len], n.slots[i+1:n.len+1])
			}
			// Don't increment i.
		case delStart:
			// Remove the start of the range by truncating. Can return after.
			n.slots[i].Start = r.End
			return true
		case delEnd:
			// Remove the end of the range by truncating.
			n.slots[i].End = r.Start
			i++
		default:
			// Remove the middle of the range by splitting and truncating.
			oldEnd := nr.End
			n.slots[i].End = r.Start

			// Create right side of split. Can return after.
			rSplit := Range{Start: r.End, End: oldEnd}
			rl.insertAtIdx(cur, n, rSplit, i+1)
			return true
		}

		// Move to the next node, if necessary.
		if i >= n.len {
			i = 0
			cur = cur.Next()
			if cur == nil {
				return true
			}
			n = cur.Value.(*rangeListNode)
		}
	}
}

// Clear implements RangeGroup. It clears all ranges from the
// rangeList.
func (rl *rangeList) Clear() {
	// Empty the first node, but keep it in the list.
	f := rl.ll.Front().Value.(*rangeListNode)
	*f = rangeListNode{}

	// If the list has more than one node in it, remove all but the first.
	if rl.ll.Len() > 1 {
		rl.ll.Init()
		rl.ll.PushBack(f)
	}
	rl.len = 0
}

// Overlaps implements RangeGroup. It returns whether the provided
// Range is partially contained within the group of Ranges in the rangeList.
func (rl *rangeList) Overlaps(r Range) bool {
	if _, cur, inCur := rl.findNode(r, false /* inclusive */); inCur {
		n := cur.Value.(*rangeListNode)
		if _, ok := n.findIdx(r, false /* inclusive */); ok {
			return true
		}
	}
	return false
}

// Encloses implements RangeGroup. It returns whether the provided
// Range is fully contained within the group of Ranges in the rangeList.
func (rl *rangeList) Encloses(r Range) bool {
	if _, cur, inCur := rl.findNode(r, false /* inclusive */); inCur {
		n := cur.Value.(*rangeListNode)
		if i, ok := n.findIdx(r, false /* inclusive */); ok {
			return contains(n.slots[i], r)
		}
	}
	return false
}

// ForEach implements RangeGroup. It calls the provided function f
// with each Range stored in the rangeList.
func (rl *rangeList) ForEach(f func(Range) error) error {
	it := rangeListIterator{e: rl.ll.Front()}
	for r, ok := it.Next(); ok; r, ok = it.Next() {
		if err := f(r); err != nil {
			return err
		}
	}
	return nil
}

// rangeListIterator is an in-order iterator operating over a rangeList.
type rangeListIterator struct {
	e   *list.Element
	idx int // next slot index
}

// Next implements RangeGroupIterator. It returns the next Range in the
// rangeList, or false.
func (rli *rangeListIterator) Next() (r Range, ok bool) {
	if rli.e != nil {
		n := rli.e.Value.(*rangeListNode)

		// Get current index, return if invalid.
		curIdx := rli.idx
		if curIdx >= n.len {
			return Range{}, false
		}

		// Move index and Element pointer forwards.
		rli.idx = curIdx + 1
		if rli.idx >= n.len {
			rli.idx = 0
			rli.e = rli.e.Next()
		}

		return n.slots[curIdx], true
	}
	return Range{}, false
}

// Iterator implements RangeGroup. It returns an iterator to iterate over
// the group of ranges.
func (rl *rangeList) Iterator() RangeGroupIterator {
	return &rangeListIterator{e: rl.ll.Front()}
}

// Len implements RangeGroup. It returns the number of ranges in
// the rangeList.
func (rl *rangeList) Len() int {
	return rl.len
}

func (rl *rangeList) String() string {
	return rgString(rl)
}

// rangeTree is an implementation of a RangeGroup using an interval
// tree to efficiently store and search for non-overlapping ranges.
//
// rangeTree is not safe for concurrent use by multiple goroutines.
type rangeTree struct {
	t       Tree
	idCount uintptr
}

// NewRangeTree constructs an interval tree backed RangeGroup.
func NewRangeTree() RangeGroup {
	return &rangeTree{
		t: NewTree(InclusiveOverlapper),
	}
}

// rangeKey implements Interface and can be inserted into a Tree. It
// provides uniqueness as well as a key interval.
type rangeKey struct {
	r  Range
	id uintptr
}

var _ Interface = rangeKey{}

// makeKey creates a new rangeKey defined by the provided range.
func (rt *rangeTree) makeKey(r Range) rangeKey {
	rt.idCount++
	return rangeKey{
		r:  r,
		id: rt.idCount,
	}
}

// Range implements Interface.
func (rk rangeKey) Range() Range {
	return rk.r
}

// ID implements Interface.
func (rk rangeKey) ID() uintptr {
	return rk.id
}

func (rk rangeKey) String() string {
	return fmt.Sprintf("%d: %q-%q", rk.id, rk.r.Start, rk.r.End)
}

// Add implements RangeGroup. It first uses the interval tree to lookup
// the current ranges which overlap with the new range. If there is no
// overlap, the new range will be added and the function will return true.
// If there is some overlap, the function will return true if the new
// range increases the range of the rangeTree, in which case it will be
// added to the tree, and false if it does not increase the range, in which
// case it won't be added. If the range is added, the function will also attempt
// to merge any ranges within the tree that now overlap.
func (rt *rangeTree) Add(r Range) bool {
	if err := rangeError(r); err != nil {
		panic(err)
	}
	overlaps := rt.t.Get(r)
	if len(overlaps) == 0 {
		key := rt.makeKey(r)
		if err := rt.t.Insert(&key, false /* fast */); err != nil {
			panic(err)
		}
		return true
	}
	first := overlaps[0].(*rangeKey)

	// If a current range fully contains the new range, no
	// need to add it.
	if contains(first.r, r) {
		return false
	}

	// Merge as many ranges as possible, and replace old range.
	first.r = merge(first.r, r)
	for _, o := range overlaps[1:] {
		other := o.(*rangeKey)
		first.r = merge(first.r, other.r)
		if err := rt.t.Delete(o, true /* fast */); err != nil {
			panic(err)
		}
	}
	rt.t.AdjustRanges()
	return true
}

// Sub implements RangeGroup. It first uses the interval tree to lookup
// the current ranges which overlap with the range to subtract. For all
// ranges that overlap with the provided range, the overlapping segment of
// the range is removed. If the provided range fully contains a range in
// the rangeTree, the range in the rangeTree will be removed. The method
// returns whether the subtraction resulted in any decrease to the size
// of the RangeGroup.
func (rt *rangeTree) Sub(r Range) bool {
	if err := rangeError(r); err != nil {
		panic(err)
	}
	overlaps := rt.t.GetWithOverlapper(r, ExclusiveOverlapper)
	if len(overlaps) == 0 {
		return false
	}

	for _, o := range overlaps {
		rk := o.(*rangeKey)
		sCmp := rk.r.Start.Compare(r.Start)
		eCmp := rk.r.End.Compare(r.End)

		delStart := sCmp >= 0
		delEnd := eCmp <= 0

		switch {
		case delStart && delEnd:
			// Remove the entire range.
			if err := rt.t.Delete(o, true /* fast */); err != nil {
				panic(err)
			}
		case delStart:
			// Remove the start of the range by truncating.
			rk.r.Start = r.End
		case delEnd:
			// Remove the end of the range by truncating.
			rk.r.End = r.Start
		default:
			// Remove the middle of the range by splitting.
			oldEnd := rk.r.End
			rk.r.End = r.Start

			rSplit := Range{Start: r.End, End: oldEnd}
			rKey := rt.makeKey(rSplit)
			if err := rt.t.Insert(&rKey, true /* fast */); err != nil {
				panic(err)
			}
		}
	}
	rt.t.AdjustRanges()
	return true
}

// Clear implements RangeGroup. It clears all rangeKeys from the rangeTree.
func (rt *rangeTree) Clear() {
	rt.t.Clear()
}

// Overlaps implements RangeGroup. It returns whether the provided
// Range is partially contained within the group of Ranges in the rangeTree.
func (rt *rangeTree) Overlaps(r Range) bool {
	if err := rangeError(r); err != nil {
		panic(err)
	}
	overlaps := rt.t.GetWithOverlapper(r, ExclusiveOverlapper)
	return len(overlaps) > 0
}

// Encloses implements RangeGroup. It returns whether the provided
// Range is fully contained within the group of Ranges in the rangeTree.
func (rt *rangeTree) Encloses(r Range) bool {
	if err := rangeError(r); err != nil {
		panic(err)
	}
	overlaps := rt.t.GetWithOverlapper(r, ExclusiveOverlapper)
	if len(overlaps) != 1 {
		return false
	}
	first := overlaps[0].(*rangeKey)
	return contains(first.r, r)
}

// ForEach implements RangeGroup. It calls the provided function f
// with each Range stored in the rangeTree.
func (rt *rangeTree) ForEach(f func(Range) error) error {
	var err error
	rt.t.Do(func(i Interface) bool {
		err = f(i.Range())
		return err != nil
	})
	return err
}

// rangeListIterator is an in-order iterator operating over a rangeTree.
type rangeTreeIterator struct {
	it TreeIterator
}

// Next implements RangeGroupIterator. It returns the next Range in the
// rangeTree, or false.
func (rti *rangeTreeIterator) Next() (r Range, ok bool) {
	i, ok := rti.it.Next()
	if !ok {
		return Range{}, false
	}
	return i.Range(), true
}

// Iterator implements RangeGroup. It returns an iterator to iterate over
// the group of ranges.
func (rt *rangeTree) Iterator() RangeGroupIterator {
	return &rangeTreeIterator{it: rt.t.Iterator()}
}

// Len implements RangeGroup. It returns the number of rangeKeys in
// the rangeTree.
func (rt *rangeTree) Len() int {
	return rt.t.Len()
}

func (rt *rangeTree) String() string {
	return rgString(rt)
}

// contains returns if the range in the out range fully contains the
// in range.
func contains(out, in Range) bool {
	return in.Start.Compare(out.Start) >= 0 && out.End.Compare(in.End) >= 0
}

// merge merges the provided ranges together into their union range. The
// ranges must overlap or the function will not produce the correct output.
func merge(l, r Range) Range {
	start := l.Start
	if r.Start.Compare(start) < 0 {
		start = r.Start
	}
	end := l.End
	if r.End.Compare(end) > 0 {
		end = r.End
	}
	return Range{Start: start, End: end}
}

// rgString returns a string representation of the ranges in a RangeGroup.
func rgString(rg RangeGroup) string {
	var buffer bytes.Buffer
	buffer.WriteRune('[')
	space := false
	if err := rg.ForEach(func(r Range) error {
		if space {
			buffer.WriteRune(' ')
		}
		buffer.WriteString(r.String())
		space = true
		return nil
	}); err != nil {
		panic(err)
	}
	buffer.WriteRune(']')
	return buffer.String()
}

// RangeGroupsOverlap determines if two RangeGroups contain any overlapping
// Ranges or if they are fully disjoint. It does so by iterating over the
// RangeGroups together and comparing subsequent ranges.
func RangeGroupsOverlap(rg1, rg2 RangeGroup) bool {
	it1, it2 := rg1.Iterator(), rg2.Iterator()
	r1, ok1 := it1.Next()
	r2, ok2 := it2.Next()
	if !ok1 || !ok2 {
		return false
	}
	for {
		// Check if the current pair of Ranges overlap.
		if overlapsExclusive(r1, r2) {
			return true
		}

		// If not, advance the Range further behind.
		var ok bool
		if r1.Start.Compare(r2.Start) < 0 {
			r1, ok = it1.Next()
		} else {
			r2, ok = it2.Next()
		}
		if !ok {
			return false
		}
	}
}
