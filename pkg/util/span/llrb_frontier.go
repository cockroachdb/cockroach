// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// llrbFrontier is a legacy span btreeFrontier implementation using LLRB tree.

// llrbFrontierEntry represents a timestamped span. It is used as the nodes in both
// the interval tree and heap needed to keep the llrbFrontier.
type llrbFrontierEntry struct {
	id   int64
	keys interval.Range
	span roachpb.Span
	ts   hlc.Timestamp
	// The index of the item in the llrbFrontierHeap, maintained by the
	// heap.Interface methods.
	index int
}

// ID implements interval.Interface.
func (s *llrbFrontierEntry) ID() uintptr {
	return uintptr(s.id)
}

// Range implements interval.Interface.
func (s *llrbFrontierEntry) Range() interval.Range {
	return s.keys
}

func (s *llrbFrontierEntry) String() string {
	return fmt.Sprintf("[%s @ %s]", s.span, s.ts)
}

// llrbFrontierHeap implements heap.Interface and holds `llrbFrontierEntry`s. Entries
// are sorted based on their timestamp such that the oldest will rise to the top
// of the heap.
type llrbFrontierHeap []*llrbFrontierEntry

// Len implements heap.Interface.
func (h llrbFrontierHeap) Len() int { return len(h) }

// Less implements heap.Interface.
func (h llrbFrontierHeap) Less(i, j int) bool {
	if h[i].ts.EqOrdering(h[j].ts) {
		return h[i].span.Key.Compare(h[j].span.Key) < 0
	}
	return h[i].ts.Less(h[j].ts)
}

// Swap implements heap.Interface.
func (h llrbFrontierHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

// Push implements heap.Interface.
func (h *llrbFrontierHeap) Push(x interface{}) {
	n := len(*h)
	entry := x.(*llrbFrontierEntry)
	entry.index = n
	*h = append(*h, entry)
}

// Pop implements heap.Interface.
func (h *llrbFrontierHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	entry.index = -1 // for safety
	old[n-1] = nil   // for gc
	*h = old[0 : n-1]
	return entry
}

// llrbFrontier tracks the minimum timestamp of a set of spans.
type llrbFrontier struct {
	// tree contains `*llrbFrontierEntry` items for the entire current tracked
	// span set. Any tracked spans that have never been `Forward`ed will have a
	// zero timestamp. If any entries needed to be split along a tracking
	// boundary, this has already been done by `insert` before it entered the
	// tree.
	tree interval.Tree
	// minHeap contains the same `*llrbFrontierEntry` items as `tree`. Entries
	// in the heap are sorted first by minimum timestamp and then by lesser
	// start key.
	minHeap llrbFrontierHeap

	idAlloc int64
}

// copyRangeToSpan copies intervals start/end points and returns a span.
// Whenever we store user provided span objects inside btreeFrontier
// datastructures, we must make a copy lest the user later mutates
// underlying start/end []byte slices in the range.
func copyRangeToSpan(r interval.Range) (res roachpb.Span) {
	res.Key = append(res.Key, r.Start...)
	res.EndKey = append(res.EndKey, r.End...)
	return
}

// Release implements Frontier interface.
func (f *llrbFrontier) Release() {}

// AddSpansAt adds the provided spans to the llrbFrontier at the provided timestamp.
func (f *llrbFrontier) AddSpansAt(startAt hlc.Timestamp, spans ...roachpb.Span) error {

	for _, toAdd := range spans {
		toAdd = copyRangeToSpan(toAdd.AsRange())

		// Add toAdd sub-spans that do not overlap this frontier. To ensure that
		// adjacent spans are merged, sub-spans are added in two steps: first,
		// non-overlapping spans are added with 0 timestamp; then the timestamp for
		// the entire toAdd span is forwarded.
		for _, span := range spanDifference(toAdd, f) {
			e := &llrbFrontierEntry{
				id:   f.idAlloc,
				keys: span.AsRange(),
				span: span,
				ts:   hlc.Timestamp{},
			}
			f.idAlloc++
			if err := f.tree.Insert(e, false /* fast */); err != nil {
				return err
			}
			heap.Push(&f.minHeap, e)
		}
		if err := f.insert(toAdd, startAt); err != nil {
			return err
		}
	}
	f.tree.AdjustRanges()
	return nil
}

// Frontier returns the minimum timestamp being tracked.
func (f *llrbFrontier) Frontier() hlc.Timestamp {
	if f.minHeap.Len() == 0 {
		return hlc.Timestamp{}
	}
	return f.minHeap[0].ts
}

// PeekFrontierSpan returns one of the spans at the llrbFrontier.
func (f *llrbFrontier) PeekFrontierSpan() roachpb.Span {
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
func (f *llrbFrontier) Forward(span roachpb.Span, ts hlc.Timestamp) (bool, error) {
	prevFrontier := f.Frontier()
	if err := f.insert(span, ts); err != nil {
		return false, err
	}
	return prevFrontier.Less(f.Frontier()), nil
}

// extendRangeToTheLeft extends the range to the left of the range, provided those
// ranges all have specified timestamp.
// Updates provided range with the new starting position.
// Returns the list of llrbFrontierEntries covered by the updated range; the caller
// is expected to remove those covered ranges from the tree.
func extendRangeToTheLeft(
	t interval.Tree, r *interval.Range, ts hlc.Timestamp,
) (covered []*llrbFrontierEntry) {
	for {
		// Get the range to the left of the range.
		// Since we request an inclusive overlap of the range containing exactly
		// 1 key, we expect to get two extensions if there is anything to the left:
		// the range (r) itself, and the one to the left of r.
		left := t.GetWithOverlapper(
			interval.Range{Start: r.Start, End: r.Start},
			interval.InclusiveOverlapper,
		)
		if len(left) == 2 && left[0].(*llrbFrontierEntry).ts.Equal(ts) {
			e := left[0].(*llrbFrontierEntry)
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
// Returns the list of llrbFrontierEntries covered by the updated range; the caller
// is expected to remove those covered ranges from the tree.
func extendRangeToTheRight(
	t interval.Tree, r *interval.Range, ts hlc.Timestamp,
) (covered []*llrbFrontierEntry) {
	for {
		// Get the range to the right of the range.
		// Since we request an exclusive overlap of the range containing exactly
		// 1 key, we expect to get exactly 1 extensions if there is anything to the right of the span.
		endKey := roachpb.Key(r.End)
		rightSpan := roachpb.Span{Key: endKey, EndKey: endKey.Next()}
		right := t.GetWithOverlapper(rightSpan.AsRange(), interval.ExclusiveOverlapper)
		if len(right) == 1 && right[0].(*llrbFrontierEntry).ts.Equal(ts) {
			e := right[0].(*llrbFrontierEntry)
			covered = append(covered, e)
			r.End = e.keys.End
		} else {
			return
		}
	}
}

func (f *llrbFrontier) insert(span roachpb.Span, insertTS hlc.Timestamp) error {
	if err := checkSpan(span); err != nil {
		return err
	}

	// Set of llrbFrontierEntries to add and remove.
	var toAdd, toRemove []*llrbFrontierEntry

	// addEntry adds llrbFrontierEntry to the toAdd list.
	addEntry := func(r interval.Range, ts hlc.Timestamp) {
		sp := copyRangeToSpan(r)
		toAdd = append(toAdd, &llrbFrontierEntry{
			id:   f.idAlloc,
			span: sp,
			keys: sp.AsRange(),
			ts:   ts,
		})
		f.idAlloc++
	}

	// todoRange is the range we're adding. It gets updated as we process the range.
	todoRange := span.AsRange()

	// pendingSpan (if not empty) is the span of multiple overlap intervals
	// we'll merge together (because all of those intervals have timestamp lower
	// than insertTS).
	var pendingSpan interval.Range

	// consumePrefix consumes todoRange prefix ending at 'end' and moves
	// that prefix into pendingSpan.
	consumePrefix := func(end interval.Comparable) {
		if pendingSpan.Start == nil {
			pendingSpan.Start = todoRange.Start
		}
		todoRange.Start = end
		pendingSpan.End = end
	}

	extendLeft := true // can the merged span be extended to the left?

	// addPending adds llrbFrontierEntry for the pendingSpan if it's non-empty, and resets it.
	addPending := func() {
		if !pendingSpan.Start.Equal(pendingSpan.End) {
			if extendLeft {
				toRemove = append(toRemove, extendRangeToTheLeft(f.tree, &pendingSpan, insertTS)...)
			}
			addEntry(pendingSpan, insertTS)
		}

		pendingSpan.Start = nil
		pendingSpan.End = nil
		extendLeft = true
	}

	// Main work: start iterating through all ranges that overlap our span.
	f.tree.DoMatching(func(k interval.Interface) (done bool) {
		overlap := k.(*llrbFrontierEntry)

		// If overlap does not start immediately after our pendingSpan,
		// then add and reset pending.
		if !overlap.span.Key.Equal(roachpb.Key(pendingSpan.End)) {
			addPending()
		}

		// Trim todoRange if it falls outside the span(s) tracked by this frontier.
		// This establishes the invariant that overlap start must be at or before todoRange start.
		if todoRange.Start.Compare(overlap.keys.Start) < 0 {
			todoRange.Start = overlap.keys.Start
		}

		// Fast case: we already recorded higher timestamp for this overlap
		if insertTS.Less(overlap.ts) {
			todoRange.Start = overlap.keys.End
			return ContinueMatch.asBool()
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
				// of overlap.  Extending range to the right adjusts pendingSpan.End and returns the
				// list of extended ranges, which we remove because they are subsumed by pendingSpan.
				// Note also, that at this point, we know that this is the last overlap entry, and that
				// we will exit DoMatching, at which point we add whatever range was accumulated
				// in the pendingRange.
				toRemove = append(toRemove, extendRangeToTheRight(f.tree, &pendingSpan, insertTS)...)
			}
		} else {
			// Our todoRange extends beyond overlap: consume until the end of the overlap.
			consumePrefix(overlap.keys.End)
		}

		return ContinueMatch.asBool()
	}, span.AsRange())

	// Add remaining pending range.
	addPending()

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
func (f *llrbFrontier) Entries(fn Operation) {
	f.tree.Do(func(i interval.Interface) bool {
		spe := i.(*llrbFrontierEntry)
		return fn(spe.span, spe.ts).asBool()
	})
}

// SpanEntries invokes op for each sub-span of the specified span with the
// timestamp as observed by this frontier.
//
// Time
// 5|      .b__c               .
// 4|      .             h__k  .
// 3|      .      e__f         .
// 1 ---a----------------------m---q-- llrbFrontier
//
//	|___________span___________|
//
// In the above example, frontier tracks [b, m) and the current frontier
// timestamp is 1.  SpanEntries for span [a-q) will invoke op with:
//
//	([b-c), 5), ([c-e), 1), ([e-f), 3], ([f, h], 1) ([h, k), 4), ([k, m), 1).
//
// Note: neither [a-b) nor [m, q) will be emitted since they do not intersect with the spans
// tracked by this frontier.
func (f *llrbFrontier) SpanEntries(span roachpb.Span, op Operation) {
	todoRange := span.AsRange()

	f.tree.DoMatching(func(i interval.Interface) bool {
		e := i.(*llrbFrontierEntry)

		// Skip untracked portion.
		if todoRange.Start.Compare(e.keys.Start) < 0 {
			todoRange.Start = e.keys.Start
		}

		end := e.keys.End
		if e.keys.End.Compare(todoRange.End) > 0 {
			end = todoRange.End
		}

		if op(roachpb.Span{Key: roachpb.Key(todoRange.Start), EndKey: roachpb.Key(end)}, e.ts) == StopMatch {
			return StopMatch.asBool()
		}
		todoRange.Start = end
		return ContinueMatch.asBool()
	}, span.AsRange())
}

// String implements Stringer.
func (f *llrbFrontier) String() string {
	var buf strings.Builder
	f.tree.Do(func(i interval.Interface) bool {
		if buf.Len() != 0 {
			buf.WriteString(` `)
		}
		buf.WriteString(i.(*llrbFrontierEntry).String())
		return false
	})
	return buf.String()
}

// Len implements Frontier.
func (f *llrbFrontier) Len() int {
	return f.tree.Len()
}
