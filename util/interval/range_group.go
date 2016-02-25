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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package interval

import (
	"container/list"
	"fmt"
)

// RangeGroup represents a set of possibly disjointed Ranges. The
// primary use case of the interface is to add ranges to the group
// and observe whether the addition increases the size of the group
// or not, indicating whether the new range's interval is redundant,
// or if it is needed for the full composition of the group. Because
// the RangeGroup builds as more ranges are added, insertion order of
// the ranges is critical. For instance, if two identical ranges are
// added, only the first to be added with Add will return true, as it
// will be the only one to expand the group.
type RangeGroup interface {
	// Add will attempt to add the provided Range to the RangeGroup,
	// returning whether the addition increased the range of the group
	// or not. The method will panic if the provided range is inverted
	// or empty.
	Add(Range) bool
	// Len returns the number of Ranges currently within the RangeGroup.
	// This will always be equal to or less than the number of ranges added,
	// as ranges that overlap will merge to produce a single larger range.
	Len() int
	// Clear clears all ranges from the RangeGroup, resetting it to be
	// used again.
	Clear()
}

// rangeList is an implementation of a RangeGroup using a linked
// list to sequentially order non-overlapping ranges.
//
// rangeList is not safe for concurrent use by multiple goroutines.
type rangeList struct {
	ll list.List
}

// NewRangeList constructs a linked-list backed RangeGroup.
func NewRangeList() RangeGroup {
	var r rangeList
	r.ll.Init()
	return &r
}

// Add implements RangeGroup. It iterates over the current ranges in the
// rangeList to find which overlap with the new range. If there is no
// overlap, the new range will be added and the function will return true.
// If there is some overlap, the function will return true if the new
// range increases the range of the rangeList, in which case it will be
// added to the list, and false if it does not increase the range, in which
// case it won't be added. If the range is added, the function will also attempt
// to merge any ranges within the list that now overlap.
func (rg *rangeList) Add(r Range) bool {
	if err := rangeError(r); err != nil {
		panic(err)
	}
	for e := rg.ll.Front(); e != nil; e = e.Next() {
		er := e.Value.(Range)
		switch {
		case er.OverlapInclusive(r):
			// If a current range fully contains the new range, no
			// need to add it.
			if contains(er, r) {
				return false
			}

			// Merge as many ranges as possible, and replace old range.
			newR := merge(er, r)
			for p := e.Next(); p != nil; {
				pr := p.Value.(Range)
				if newR.OverlapInclusive(pr) {
					newR = merge(newR, pr)

					nextP := p.Next()
					rg.ll.Remove(p)
					p = nextP
				} else {
					break
				}
			}
			e.Value = newR
			return true
		case r.End.Compare(er.Start) < 0:
			rg.ll.InsertBefore(r, e)
			return true
		}
	}
	rg.ll.PushBack(r)
	return true
}

// Len implements RangeGroup. It returns the number of ranges in
// the rangeList.
func (rg *rangeList) Len() int {
	return rg.ll.Len()
}

// Clear implements RangeGroup. It clears all ranges from the
// rangeList.
func (rg *rangeList) Clear() {
	rg.ll.Init()
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
		t: Tree{Overlapper: Range.OverlapInclusive},
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

func (rk rangeKey) Contains(lk rangeKey) bool {
	return contains(rk.r, lk.r)
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
	key := rt.makeKey(r)
	overlaps := rt.t.Get(key.r)
	if len(overlaps) == 0 {
		if err := rt.insertKey(key); err != nil {
			panic(err)
		}
		return true
	}
	first := overlaps[0].(*rangeKey)

	// If a current range fully contains the new range, no
	// need to add it.
	if first.Contains(key) {
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

// insertKey inserts the rangeKey into the rangeTree's interval tree.
// This is split into a helper method so that key will only escape to
// the heap when the insertion case is needed and Insert is called.
func (rt *rangeTree) insertKey(key rangeKey) error {
	return rt.t.Insert(&key, false /* !fast */)
}

// Len implements RangeGroup. It returns the number of rangeKeys in
// the rangeTree.
func (rt *rangeTree) Len() int {
	return rt.t.Len()
}

// Clear implements RangeGroup. It clears all rangeKeys from the rangeTree.
func (rt *rangeTree) Clear() {
	rt.t = Tree{Overlapper: Range.OverlapInclusive}
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
