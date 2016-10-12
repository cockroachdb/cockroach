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
	// Len returns the number of Ranges currently within the RangeGroup.
	// This will always be equal to or less than the number of ranges added,
	// as ranges that overlap will merge to produce a single larger range.
	Len() int
	fmt.Stringer
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
			// Past where inclusive overlapping ranges would be.
			rg.ll.InsertBefore(r, e)
			return true
		}
	}
	rg.ll.PushBack(r)
	return true
}

// Sub implements RangeGroup. It iterates over the current ranges in the
// rangeList to find which overlap with the range to subtract. For all
// ranges that overlap with the provided range, the overlapping segment of
// the range is removed. If the provided range fully contains a range in
// the rangeList, the range in the rangeList will be removed. The method
// returns whether the subtraction resulted in any decrease to the size
// of the RangeGroup.
func (rg *rangeList) Sub(r Range) bool {
	if err := rangeError(r); err != nil {
		panic(err)
	}
	dec := false
	for e := rg.ll.Front(); e != nil; {
		er := e.Value.(Range)
		switch {
		case er.OverlapExclusive(r):
			sCmp := er.Start.Compare(r.Start)
			eCmp := er.End.Compare(r.End)

			delStart := sCmp >= 0
			delEnd := eCmp <= 0
			cont := eCmp < 0

			switch {
			case delStart && delEnd:
				// Remove the entire range.
				nextE := e.Next()
				rg.ll.Remove(e)
				e = nextE
			case delStart:
				// Remove the start of the range by truncating.
				er.Start = r.End
				e.Value = er
				e = e.Next()
			case delEnd:
				// Remove the end of the range by truncating.
				er.End = r.Start
				e.Value = er
				e = e.Next()
			default:
				// Remove the middle of the range by splitting and truncating.
				oldEnd := er.End
				er.End = r.Start
				e.Value = er

				rSplit := Range{Start: r.End, End: oldEnd}
				newE := rg.ll.InsertAfter(rSplit, e)
				e = newE.Next()
			}

			dec = true
			if !cont {
				return dec
			}
		case r.End.Compare(er.Start) <= 0:
			// Past where exclusive overlapping ranges would be.
			return dec
		default:
			e = e.Next()
		}
	}
	return dec
}

// Clear implements RangeGroup. It clears all ranges from the
// rangeList.
func (rg *rangeList) Clear() {
	rg.ll.Init()
}

// Overlaps implements RangeGroup. It returns whether the provided
// Range is partially contained within the group of Ranges in the rangeList.
func (rg *rangeList) Overlaps(r Range) bool {
	if err := rangeError(r); err != nil {
		panic(err)
	}
	for e := rg.ll.Front(); e != nil; e = e.Next() {
		er := e.Value.(Range)
		switch {
		case er.OverlapExclusive(r):
			return true
		case r.End.Compare(er.Start) <= 0:
			// Past where exclusive overlapping ranges would be.
			return false
		}
	}
	return false
}

// Encloses implements RangeGroup. It returns whether the provided
// Range is fully contained within the group of Ranges in the rangeList.
func (rg *rangeList) Encloses(r Range) bool {
	if err := rangeError(r); err != nil {
		panic(err)
	}
	for e := rg.ll.Front(); e != nil; e = e.Next() {
		er := e.Value.(Range)
		switch {
		case contains(er, r):
			return true
		case r.End.Compare(er.Start) <= 0:
			// Past where exclusive overlapping ranges would be.
			return false
		}
	}
	return false
}

// ForEach implements RangeGroup. It calls the provided function f
// with each Range stored in the rangeList.
func (rg *rangeList) ForEach(f func(Range) error) error {
	for e := rg.ll.Front(); e != nil; e = e.Next() {
		if err := f(e.Value.(Range)); err != nil {
			return err
		}
	}
	return nil
}

// Len implements RangeGroup. It returns the number of ranges in
// the rangeList.
func (rg *rangeList) Len() int {
	return rg.ll.Len()
}

func (rg *rangeList) String() string {
	return rgString(rg)
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
		if err := rt.t.Insert(&key, false /* !fast */); err != nil {
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
	overlaps := rt.t.GetWithOverlapper(r, Range.OverlapExclusive)
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
	rt.t = Tree{Overlapper: Range.OverlapInclusive}
}

// Overlaps implements RangeGroup. It returns whether the provided
// Range is partially contained within the group of Ranges in the rangeTree.
func (rt *rangeTree) Overlaps(r Range) bool {
	if err := rangeError(r); err != nil {
		panic(err)
	}
	overlaps := rt.t.GetWithOverlapper(r, Range.OverlapExclusive)
	return len(overlaps) > 0
}

// Encloses implements RangeGroup. It returns whether the provided
// Range is fully contained within the group of Ranges in the rangeTree.
func (rt *rangeTree) Encloses(r Range) bool {
	if err := rangeError(r); err != nil {
		panic(err)
	}
	overlaps := rt.t.GetWithOverlapper(r, Range.OverlapExclusive)
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
