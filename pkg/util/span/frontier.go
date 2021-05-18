// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package span

import (
	"container/heap"
	"fmt"
	"strings"

	// Needed for roachpb.Span.String().
	_ "github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
)

// frontierEntry represents a timestamped span. It is used as the nodes in both
// the interval tree and heap needed to keep the Frontier.
type frontierEntry struct {
	id   int64
	keys interval.Range
	span roachpb.Span
	ts   hlc.Timestamp

	// The index of the item in the frontierHeap, maintained by the
	// heap.Interface methods.
	index int
}

// ID implements interval.Interface.
func (s *frontierEntry) ID() uintptr {
	return uintptr(s.id)
}

// Range implements interval.Interface.
func (s *frontierEntry) Range() interval.Range {
	return s.keys
}

func (s *frontierEntry) String() string {
	return fmt.Sprintf("[%s @ %s]", s.span, s.ts)
}

// frontierHeap implements heap.Interface and holds `frontierEntry`s. Entries
// are sorted based on their timestamp such that the oldest will rise to the top
// of the heap.
type frontierHeap []*frontierEntry

// Len implements heap.Interface.
func (h frontierHeap) Len() int { return len(h) }

// Less implements heap.Interface.
func (h frontierHeap) Less(i, j int) bool {
	if h[i].ts.EqOrdering(h[j].ts) {
		return h[i].span.Key.Compare(h[j].span.Key) < 0
	}
	return h[i].ts.Less(h[j].ts)
}

// Swap implements heap.Interface.
func (h frontierHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

// Push implements heap.Interface.
func (h *frontierHeap) Push(x interface{}) {
	n := len(*h)
	entry := x.(*frontierEntry)
	entry.index = n
	*h = append(*h, entry)
}

// Pop implements heap.Interface.
func (h *frontierHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	entry.index = -1 // for safety
	old[n-1] = nil   // for gc
	*h = old[0 : n-1]
	return entry
}

// Frontier tracks the minimum timestamp of a set of spans.
type Frontier struct {
	// tree contains `*frontierEntry` items for the entire current tracked
	// span set. Any tracked spans that have never been `Forward`ed will have a
	// zero timestamp. If any entries needed to be split along a tracking
	// boundary, this has already been done by `insert` before it entered the
	// tree.
	tree interval.Tree
	// minHeap contains the same `*frontierEntry` items as `tree`. Entries
	// in the heap are sorted first by minimum timestamp and then by lesser
	// start key.
	minHeap frontierHeap

	idAlloc int64
}

// makeSpan copies intervals start/end points and returns a span.
// Whenever we store user provided span objects inside frontier
// datastructures, we must make a copy lest the user later mutates
// underlying start/end []byte slices in the range.
func makeSpan(r interval.Range) (res roachpb.Span) {
	res.Key = append(res.Key, r.Start...)
	res.EndKey = append(res.EndKey, r.End...)
	return
}

// MakeFrontier returns a Frontier that tracks the given set of spans.
func MakeFrontier(spans ...roachpb.Span) (*Frontier, error) {
	f := &Frontier{tree: interval.NewTree(interval.ExclusiveOverlapper)}
	for _, s := range spans {
		span := makeSpan(s.AsRange())
		e := &frontierEntry{
			id:   f.idAlloc,
			keys: span.AsRange(),
			span: span,
			ts:   hlc.Timestamp{},
		}
		f.idAlloc++
		if err := f.tree.Insert(e, true /* fast */); err != nil {
			return nil, err
		}
		heap.Push(&f.minHeap, e)
	}
	f.tree.AdjustRanges()
	return f, nil
}

// Frontier returns the minimum timestamp being tracked.
func (f *Frontier) Frontier() hlc.Timestamp {
	if f.minHeap.Len() == 0 {
		return hlc.Timestamp{}
	}
	return f.minHeap[0].ts
}

// PeekFrontierSpan returns one of the spans at the Frontier.
func (f *Frontier) PeekFrontierSpan() roachpb.Span {
	if f.minHeap.Len() == 0 {
		return roachpb.Span{}
	}
	return f.minHeap[0].span
}

// Forward advances the timestamp for a span. Any part of the span that doesn't
// overlap the tracked span set will be ignored. True is returned if the
// frontier advanced as a result.
//
// Note that internally, it may be necessary to use multiple entries to
// represent this timestamped span (e.g. if it overlaps with the tracked span
// set boundary). Similarly, an entry created by a previous Forward may be
// partially overlapped and have to be split into two entries.
func (f *Frontier) Forward(span roachpb.Span, ts hlc.Timestamp) (bool, error) {
	prevFrontier := f.Frontier()
	if err := f.insert(span, ts); err != nil {
		return false, err
	}
	return prevFrontier.Less(f.Frontier()), nil
}

// extendRangeToTheLeft extends the range to the left of the range, provided those
// ranges all have specified timestamp.
// Updates provided range with the new starting position.
// Returns the list of frontier entries covered by the updated range.
func extendRangeToTheLeft(
	t interval.Tree, r *interval.Range, ts hlc.Timestamp,
) (covered []*frontierEntry) {
	for {
		// Get the range to the left of the range.
		// Since we request an inclusive overlap of the range containing exactly
		// 1 key, we expect to get two extensions if there is anything to the left:
		// the range (r) itself, and the one to the left of r.
		left := t.GetWithOverlapper(
			interval.Range{Start: r.Start, End: r.Start},
			interval.InclusiveOverlapper,
		)
		if len(left) == 2 && left[0].(*frontierEntry).ts.Equal(ts) {
			e := left[0].(*frontierEntry)
			covered = append(covered, e)
			r.Start = e.keys.Start
		} else {
			return
		}
	}
}

// extendRangeToTheRight extends the range to the right of the range, provided those
// ranges all have specified timestamp.
// Updates provided range with the new ending position.
// Returns the list of frontier entries covered by the updated range.
func extendRangeToTheRight(
	t interval.Tree, r *interval.Range, ts hlc.Timestamp,
) (covered []*frontierEntry) {
	for {
		// Get the range to the right of the range.
		// Since we request an exclusive overlap of the range containing exactly
		// 1 key, we expect to get exactly 1 extensions if there is anything to the right of the span.
		endKey := roachpb.Key(r.End)
		rightSpan := roachpb.Span{Key: endKey, EndKey: endKey.Next()}
		right := t.GetWithOverlapper(rightSpan.AsRange(), interval.ExclusiveOverlapper)
		if len(right) == 1 && right[0].(*frontierEntry).ts.Equal(ts) {
			e := right[0].(*frontierEntry)
			covered = append(covered, e)
			r.End = e.keys.End
		} else {
			return
		}
	}
}

func (f *Frontier) insert(span roachpb.Span, insertTS hlc.Timestamp) error {
	const continueMatch = false

	// Set of frontier entries to add and remove.
	var toAdd, toRemove []*frontierEntry

	// addEntry adds frontier entry to the toAdd list.
	addEntry := func(r interval.Range, ts hlc.Timestamp) {
		sp := makeSpan(r)
		toAdd = append(toAdd, &frontierEntry{
			id:   f.idAlloc,
			span: sp,
			keys: sp.AsRange(),
			ts:   ts,
		})
		f.idAlloc++
	}

	// todoRange is the range we're adding. It gets updated as we process the range.
	todoRange := span.AsRange()

	// mergedSpan (if not empty) is the span of multiple overlap intervals
	// we'll merge together (because all of those intervals have timestamp lower
	// than insertTS).
	var mergedSpan interval.Range

	// consumePrefix consumes todoRange prefix ending at 'end' and moves
	// that prefix into mergedSpan.
	consumePrefix := func(end interval.Comparable) {
		if mergedSpan.Start == nil {
			mergedSpan.Start = todoRange.Start
		}
		todoRange.Start = end
		mergedSpan.End = end
	}

	extendLeft := true // can the merged span be extended to the left?

	// addMerged adds frontier entry for the mergedSpan if it's non-empty, and resets it.
	addMerged := func() {
		if !mergedSpan.Start.Equal(mergedSpan.End) {
			if extendLeft {
				toRemove = append(toRemove, extendRangeToTheLeft(f.tree, &mergedSpan, insertTS)...)
			}
			addEntry(mergedSpan, insertTS)
		}

		mergedSpan.Start = nil
		mergedSpan.End = nil
		extendLeft = true
	}

	// Main work: start iterating through all ranges that overlap our span.
	f.tree.DoMatching(func(k interval.Interface) (done bool) {
		overlap := k.(*frontierEntry)

		// If overlap does not start immediately after our mergedSpan,
		// then add and reset pending.
		if !overlap.span.Key.Equal(roachpb.Key(mergedSpan.End)) {
			addMerged()
		}

		// Trim todoRange if it falls outside the span(s) tracked by this frontier.
		// This establishes the invariant that overlap start must be at or before todoRange start.
		if todoRange.Start.Compare(overlap.keys.Start) < 0 {
			todoRange.Start = overlap.keys.Start
		}

		// Fast case: we already recorded higher timestamp for this overlap.
		if insertTS.LessEq(overlap.ts) {
			todoRange.Start = overlap.keys.End
			return continueMatch
		}

		// At this point, we know that overlap timestamp is not ahead of the insertTS
		// (otherwise we'd hit fast case above).
		// We need split overlap range, so mark overlap for removal.
		toRemove = append(toRemove, overlap)

		// We need to split overlap range into multiple parts.
		// 1. Possibly empty part before todoRange.Start
		if overlap.keys.Start.Compare(todoRange.Start) < 0 {
			extendLeft = false
			addEntry(interval.Range{Start: overlap.keys.Start, End: todoRange.Start}, overlap.ts)
		}

		// 2. Middle part (with updated timestamp), and...
		// 3. Possibly empty part after todoRange end.
		if cmp := todoRange.End.Compare(overlap.keys.End); cmp <= 0 {
			// Our todoRange ends before the overlap ends, so consume all of it.
			consumePrefix(todoRange.End)

			if cmp < 0 && overlap.ts != insertTS {
				// Add the rest of the overlap.
				addEntry(interval.Range{Start: todoRange.End, End: overlap.keys.End}, overlap.ts)
			} else {
				// We can consume all the way until the end of the overlap
				// since overlap extends to the end of todoRange or it has the same timestamp as insertTS.
				consumePrefix(overlap.keys.End)
				// We can also attempt to merge more ranges with the same timestamp to the right
				// of overlap.
				toRemove = append(toRemove, extendRangeToTheRight(f.tree, &mergedSpan, insertTS)...)
			}
		} else {
			// Our todoRange extends beyond overlap: consume until the end of the overlap.
			consumePrefix(overlap.keys.End)
		}

		return continueMatch
	}, span.AsRange())

	// Add remaining merge range.
	addMerged()

	const withRangeAdjust = false
	for _, e := range toRemove {
		if err := f.tree.Delete(e, withRangeAdjust); err != nil {
			return err
		}
		heap.Remove(&f.minHeap, e.index)
	}

	for _, e := range toAdd {
		if err := f.tree.Insert(e, withRangeAdjust); err != nil {
			return err
		}
		heap.Push(&f.minHeap, e)
	}
	return nil
}

// Entries invokes the given callback with the current timestamp for each
// component span in the tracked span set.
func (f *Frontier) Entries(fn func(roachpb.Span, hlc.Timestamp)) {
	f.tree.Do(func(i interval.Interface) bool {
		spe := i.(*frontierEntry)
		fn(spe.span, spe.ts)
		return false
	})
}

func (f *Frontier) String() string {
	var buf strings.Builder
	f.tree.Do(func(i interval.Interface) bool {
		if buf.Len() != 0 {
			buf.WriteString(` `)
		}
		buf.WriteString(i.(*frontierEntry).String())
		return false
	})
	return buf.String()
}
