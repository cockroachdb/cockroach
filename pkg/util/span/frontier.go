// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package span

import (
	"container/heap"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	// Needed for roachpb.Span.String().
	_ "github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Frontier tracks the minimum timestamp of a set of spans.
// Frontier is not safe for concurrent modification, but MakeConcurrentFrontier
// can be used to make thread safe frontier.
type Frontier interface {
	// AddSpansAt adds the provided spans to the frontier at the provided timestamp.
	// If the span overlaps any spans already tracked by the frontier, the tree is adjusted
	// to hold union of the span and the overlaps, with all entries assigned startAt starting
	// timestamp.
	AddSpansAt(startAt hlc.Timestamp, spans ...roachpb.Span) error

	// Frontier returns the minimum timestamp being tracked.
	Frontier() hlc.Timestamp

	// PeekFrontierSpan returns one of the spans at the Frontier.
	PeekFrontierSpan() roachpb.Span

	// Forward advances the timestamp for a span. Any part of the span that doesn't
	// overlap the tracked span set will be ignored. True is returned if the
	// frontier advanced as a result.
	Forward(span roachpb.Span, ts hlc.Timestamp) (bool, error)

	// Release removes all items from the frontier. In doing so, it allows memory
	// held by the frontier to be recycled. Failure to call this method before
	// letting a frontier be GCed is safe in that it won't cause a memory leak,
	// but it will prevent frontier nodes from being efficiently re-used.
	Release()

	// Entries invokes the given callback with the current timestamp for each
	// component span in the tracked span set.
	// The fn may not mutate this frontier while iterating.
	Entries(fn Operation)

	// SpanEntries invokes op for each sub-span of the specified span with the
	// timestamp as observed by this frontier.
	//
	// Time
	// 5|      .b__c               .
	// 4|      .             h__k  .
	// 3|      .      e__f         .
	// 1 ---a----------------------m---q-- Frontier
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
	// The fn may not mutate this frontier while iterating.
	SpanEntries(span roachpb.Span, op Operation)

	// Len returns the number of spans tracked by the frontier.
	Len() int

	// String returns string representation of this fFrontier.
	String() string
}

// OpResult is the result of the Operation callback.
type OpResult bool

const (
	// ContinueMatch signals DoMatching should continue.
	ContinueMatch OpResult = false
	// StopMatch signals DoMatching should stop.
	StopMatch OpResult = true
)

// An Operation is a function that operates on a frontier spans. If done is returned true, the
// Operation is indicating that no further work needs to be done and so the DoMatching function
// should traverse no further.
type Operation func(roachpb.Span, hlc.Timestamp) (done OpResult)

func newBtreeFrontier() Frontier {
	return &btreeFrontier{}
}

func newFrontier() Frontier {
	return newBtreeFrontier()
}

// MakeFrontier returns a Frontier that tracks the given set of spans.
// Each span timestamp initialized at 0.
func MakeFrontier(spans ...roachpb.Span) (Frontier, error) {
	return MakeFrontierAt(hlc.Timestamp{}, spans...)
}

// MakeFrontierAt returns a Frontier that tracks the given set of spans.
// Each span timestamp initialized at specified start time.
func MakeFrontierAt(startAt hlc.Timestamp, spans ...roachpb.Span) (Frontier, error) {
	f := newFrontier()
	if err := f.AddSpansAt(startAt, spans...); err != nil {
		f.Release() // release whatever was allocated.
		return nil, err
	}
	return f, nil
}

// MakeConcurrentFrontier wraps provided frontier to make it safe to use concurrently.
func MakeConcurrentFrontier(f Frontier) Frontier {
	return &concurrentFrontier{f: f}
}

// btreeFrontier is a btree based implementation of Frontier.
type btreeFrontier struct {
	// tree contains `*btreeFrontierEntry` items for the entire currently tracked
	// span set. Any tracked spans that have never been `Forward`ed will have a
	// zero timestamp. If any entries needed to be split along a tracking
	// boundary, this has already been done by `forward` before it entered the
	// tree.
	tree btree
	// minHeap contains the same `*btreeFrontierEntry` items as `tree`. Entries
	// in the heap are sorted first by minimum timestamp and then by lesser
	// start key.
	minHeap frontierHeap

	idAlloc uint64

	mergeAlloc []*btreeFrontierEntry // Amortize allocations.

	// disallowMutationWhileIterating is set when iterating
	// over frontier entries.  Attempts to mutate this frontier
	// will panic under the test or return an error.
	disallowMutationWhileIterating atomic.Bool
}

// btreeFrontierEntry represents a timestamped span. It is used as the nodes in both
// the tree and heap needed to keep the Frontier.
// btreeFrontierEntry implements interval/generic interface.
type btreeFrontierEntry struct {
	Start, End roachpb.Key
	ts         hlc.Timestamp

	// id is a unique ID assigned to each frontier entry
	// (required by the underlying generic btree implementation).
	id uint64

	// The heapIdx of the item in the frontierHeap, maintained by the
	// heap.Interface methods.
	heapIdx int

	// spanCopy contains a copy of the user provided span.
	// This is used only under test to detect frontier mis-uses when
	// the caller mutates span keys after adding those spans to this frontier.
	spanCopy roachpb.Span
}

//go:generate ../interval/generic/gen.sh *btreeFrontierEntry span

// AddSpansAt adds the provided spans to the btreeFrontier at the provided timestamp.
// AddSpansAt deletes any overlapping spans already in the frontier.
//
// NB: It is *extremely* important for the caller to guarantee that the passed
// in spans (the underlying Key/EndKey []byte slices) are not modified in any
// way after this call. If modifications are made to the underlying key slices
// after the spans are added, the results are undefined -- anything from panic
// to infinite loops are possible. While this warning is scary, as it should be,
// the reality is that all callers so far, use the spans that come in from
// external source (an iterator, or RPC), and none of these callers ever modify
// the underlying keys.  If the caller has to modify underlying key slices, they
// must pass in the copy.
func (f *btreeFrontier) AddSpansAt(startAt hlc.Timestamp, spans ...roachpb.Span) (retErr error) {
	if err := f.checkDisallowedMutation(); err != nil {
		return err
	}

	if expensiveChecksEnabled() {
		defer func() {
			if err := f.checkUnsafeKeyModification(); err != nil {
				retErr = errors.CombineErrors(retErr, err)
			}
		}()
	}

	for _, toAdd := range spans {
		// Validate caller provided span.
		if err := checkSpan(toAdd); err != nil {
			return err
		}

		// Add toAdd sub-spans that do not overlap this frontier. To ensure that
		// adjacent spans are merged, sub-spans are added in two steps: first,
		// non-overlapping spans are added with 0 timestamp; then the timestamp for
		// the entire toAdd span is forwarded.
		for _, s := range spanDifference(toAdd, f) {
			e := newFrontierEntry(&f.idAlloc, s.Key, s.EndKey, hlc.Timestamp{})
			if err := f.setEntry(e); err != nil {
				putFrontierEntry(e)
				return err
			}
		}
		if err := f.forward(toAdd, startAt); err != nil {
			return err
		}
	}
	return nil
}

// Release removes all items from the btreeFrontier. In doing so, it allows memory
// held by the btreeFrontier to be recycled. Failure to call this method before
// letting a btreeFrontier be GCed is safe in that it won't cause a memory leak,
// but it will prevent btreeFrontier nodes from being efficiently re-used.
func (f *btreeFrontier) Release() {
	it := f.tree.MakeIter()
	for it.First(); it.Valid(); it.Next() {
		putFrontierEntry(it.Cur())
	}
	f.tree.Reset()
}

// Frontier returns the minimum timestamp being tracked.
func (f *btreeFrontier) Frontier() hlc.Timestamp {
	if f.minHeap.Len() == 0 {
		return hlc.Timestamp{}
	}
	return f.minHeap[0].ts
}

// PeekFrontierSpan returns one of the spans at the Frontier.
func (f *btreeFrontier) PeekFrontierSpan() roachpb.Span {
	if f.minHeap.Len() == 0 {
		return roachpb.Span{}
	}
	return f.minHeap[0].span()
}

// Forward advances the timestamp for a span. Any part of the span that doesn't
// overlap the tracked span set will be ignored. True is returned if the
// frontier advanced as a result.
//
// Note that internally, it may be necessary to use multiple entries to
// represent this timestamped span (e.g. if it overlaps with the tracked span
// set boundary). Similarly, an entry created by a previous Forward may be
// partially overlapped and have to be split into two entries.
//
// NB: it is unsafe for the caller to modify the keys in the provided span after this
// call returns.
func (f *btreeFrontier) Forward(
	span roachpb.Span, ts hlc.Timestamp,
) (forwarded bool, retErr error) {
	if err := f.checkDisallowedMutation(); err != nil {
		return false, err
	}

	// Validate caller provided span.
	if err := checkSpan(span); err != nil {
		return false, err
	}

	if expensiveChecksEnabled() {
		defer func() {
			if err := f.checkUnsafeKeyModification(); err != nil {
				retErr = errors.CombineErrors(retErr, err)
			}
		}()
	}

	prevFrontier := f.Frontier()
	if err := f.forward(span, ts); err != nil {
		return false, err
	}
	return prevFrontier.Less(f.Frontier()), nil
}

// clone augments generated iterStack code to support cloning.
func (is *iterStack) clone() iterStack {
	c := *is
	c.s = append([]iterFrame(nil), is.s...) // copy stack.
	return c
}

// clone augments generated iterator code to support cloning.
func (i *iterator) clone() iterator {
	c := *i
	c.s = i.s.clone() // copy stack.
	return c
}

// mergeEntries searches for the entries to the left and to the right
// of the input entry that are contiguous to the entry range and have the same timestamp.
// Updates btree to include single merged entry.
// Any existing tree iterators and the passed in entry should be considered invalid after this call.
// Returns btreeFrontierEntry that replaced passed in entry.
func (f *btreeFrontier) mergeEntries(e *btreeFrontierEntry) (*btreeFrontierEntry, error) {
	defer func() {
		f.mergeAlloc = f.mergeAlloc[:0]
	}()

	// First, position iterator at e.
	pos := f.tree.MakeIter()
	pos.SeekGE(e)
	if !pos.Valid() || pos.Cur() != e {
		return nil, errors.AssertionFailedf("failed to find entry %s in btree", e)
	}

	// Now, search for contiguous spans to the left of e.
	leftMost := e
	leftIter := pos.clone()
	for leftIter.Prev(); leftIter.Valid(); leftIter.Prev() {
		if !(leftIter.Cur().End.Equal(leftMost.Start) && leftIter.Cur().ts.Equal(e.ts)) {
			break
		}
		f.mergeAlloc = append(f.mergeAlloc, leftIter.Cur())
		leftMost = leftIter.Cur()
	}

	if leftMost != e {
		// We found ranges to the left of e that have the same timestamp.
		// That means that we'll merge entries into leftMost, and we will
		// also subsume e itself.  This assignment ensures that leftMost
		// entry is either an entry to the left of 'e' or the 'e' itself
		// and that leftMost is removed from the mergeAlloc so that it will
		// not be deleted below.
		f.mergeAlloc[len(f.mergeAlloc)-1] = e
	}

	// Now, continue to the right of e.
	end := e.End
	rightIter := pos.clone()
	for rightIter.Next(); rightIter.Valid(); rightIter.Next() {
		if !(rightIter.Cur().Start.Equal(end) && rightIter.Cur().ts.Equal(e.ts)) {
			break
		}
		end = rightIter.Cur().End
		f.mergeAlloc = append(f.mergeAlloc, rightIter.Cur())
	}

	// If there were no left or right merges, return without restructuring the
	// tree.
	if len(f.mergeAlloc) == 0 {
		return leftMost, nil
	}

	// Delete entries first, before updating leftMost boundaries since doing so
	// will mess up btree.
	for i, toRemove := range f.mergeAlloc {
		f.mergeAlloc[i] = nil
		if err := f.deleteEntry(toRemove); err != nil {
			return nil, err
		}
	}

	f.setEndKey(leftMost, end)

	return leftMost, nil
}

// setEntry adds entry to the tree and to the heap.
func (f *btreeFrontier) setEntry(e *btreeFrontierEntry) error {
	if expensiveChecksEnabled() {
		if err := checkSpan(e.span()); err != nil {
			return err
		}
	}

	f.tree.Set(e)
	heap.Push(&f.minHeap, e)
	return nil
}

// deleteEntry removes entry from the tree and the heap, and releases this entry
// into the pool.
func (f *btreeFrontier) deleteEntry(e *btreeFrontierEntry) error {
	defer putFrontierEntry(e)

	if expensiveChecksEnabled() {
		if err := checkSpan(e.span()); err != nil {
			return err
		}
	}

	heap.Remove(&f.minHeap, e.heapIdx)
	f.tree.Delete(e)
	return nil
}

// splitEntryAt splits entry at specified split point.
// Returns left and right entries.
// Any existing tree iterators are invalid after this call.
func (f *btreeFrontier) splitEntryAt(
	e *btreeFrontierEntry, split roachpb.Key,
) (left, right *btreeFrontierEntry, err error) {
	if expensiveChecksEnabled() {
		if !e.span().ContainsKey(split) {
			return nil, nil, errors.AssertionFailedf(
				"split key %s is not contained by %s", split, e.span())
		}
	}

	right = newFrontierEntry(&f.idAlloc, split, e.End, e.ts)

	// Adjust e boundary before we add right (so that there is no overlap in the
	// tree).
	f.setEndKey(e, split)

	if err := f.setEntry(right); err != nil {
		putFrontierEntry(right)
		return nil, nil, err
	}
	return e, right, nil
}

// setEndKey changes the end key assigned to the entry. setEndKey requires the
// entry to be in the tree.
func (f *btreeFrontier) setEndKey(e *btreeFrontierEntry, endKey roachpb.Key) {
	// The tree implementation expects the Start and End keys of a span to be
	// immutable. We remove the leftMost node before updating the End index in
	// order to avoid corrupting the `maxKey` that is inlined in the `node`.
	f.tree.Delete(e)
	e.End = endKey
	if expensiveChecksEnabled() {
		e.spanCopy.EndKey = append(roachpb.Key{}, endKey...)
	}
	f.tree.Set(e)
}

// forward is the work horse of the btreeFrontier.  It forwards the timestamp
// for the specified span, splitting, and merging btreeFrontierEntries as needed.
func (f *btreeFrontier) forward(span roachpb.Span, insertTS hlc.Timestamp) error {
	todoEntry := newSearchKey(span.Key, span.EndKey)
	defer putFrontierEntry(todoEntry)

	// forwardEntryTimestamp forwards timestamp to insertTS, and updates
	// tree to merge contiguous spans with the same timestamp (if possible).
	// NB: passed in entry and any existing iterators should be considered invalid
	// after this call.
	forwardEntryTimestamp := func(e *btreeFrontierEntry) (*btreeFrontierEntry, error) {
		e.ts = insertTS
		heap.Fix(&f.minHeap, e.heapIdx)
		return f.mergeEntries(e)
	}

	for !todoEntry.isEmptyRange() { // Keep going as long as there is work to be done.
		if expensiveChecksEnabled() {
			if err := checkSpan(todoEntry.span()); err != nil {
				return err
			}
		}

		// Seek to the first entry overlapping todoEntry.
		it := f.tree.MakeIter()
		it.FirstOverlap(todoEntry)
		if !it.Valid() {
			break
		}

		overlap := it.Cur()

		// Invariant (a): todoEntry.Start must be equal or after overlap.Start.
		// Trim todoEntry if it falls outside the span(s) tracked by this btreeFrontier.
		// This establishes the invariant that overlap start must be at or before todoEntry start.
		if todoEntry.Start.Compare(overlap.Start) < 0 {
			todoEntry.Start = overlap.Start
			if todoEntry.isEmptyRange() {
				break
			}
		}

		// Fast case: we already recorded higher timestamp for this overlap.
		if insertTS.LessEq(overlap.ts) {
			todoEntry.Start = overlap.End
			continue
		}

		// Fast case: we expect that most of the time, we forward timestamp for
		// stable ranges -- that is, we expect range split/merge are not that common.
		// As such, if the overlap range exactly matches todoEntry, we can simply
		// update overlap timestamp and be done.
		if overlap.span().Equal(todoEntry.span()) {
			if _, err := forwardEntryTimestamp(overlap); err != nil {
				return err
			}
			break
		}

		// At this point, we know that overlap timestamp is not ahead of the
		// insertTS (otherwise we'd hit fast case above).
		// We need to split overlap range into multiple parts.
		// 1. Possibly empty part before todoEntry.Start
		// 2. Middle part (with updated timestamp),
		// 3. Possibly empty part after todoEntry end.
		if overlap.Start.Compare(todoEntry.Start) < 0 {
			// Split overlap into 2 entries
			// [overlap.Start, todoEntry.Start) and [todoEntry.Start, overlap.End)
			// Invariant (b): after this step, overlap is split into 2 parts.  The right
			// part starts at todoEntry.Start.
			_, _, err := f.splitEntryAt(overlap, todoEntry.Start)
			if err != nil {
				return err
			}
			continue
		}

		// NB: overlap.Start must be equal to todoEntry.Start (established by Invariant (a) and (b) above).
		if expensiveChecksEnabled() && !overlap.Start.Equal(todoEntry.Start) {
			return errors.AssertionFailedf("expected overlap %s to start at %s", overlap, todoEntry)
		}

		switch cmp := todoEntry.End.Compare(overlap.End); {
		case cmp < 0:
			// Our todoEntry ends before the overlap ends.
			// Split overlap into 2 entries:
			// [overlap.Start, todoEntry.End) and [todoEntry.End, overlap.End)
			// Left entry can reuse overlap with insertTS.
			left, right, err := f.splitEntryAt(overlap, todoEntry.End)
			if err != nil {
				return err
			}
			todoEntry.Start = right.End
			// The left part advances its timestamp.
			if _, err := forwardEntryTimestamp(left); err != nil {
				return err
			}
		case cmp >= 0:
			// todoEntry ends at or beyond overlap.  Regardless, we can simply update overlap
			// and if needed, continue matching remaining todoEntry (if any).
			fwd, err := forwardEntryTimestamp(overlap)
			if err != nil {
				return err
			}
			todoEntry.Start = fwd.End
		}
	}
	return nil
}

func (f *btreeFrontier) disallowMutations() func() {
	f.disallowMutationWhileIterating.Store(true)
	return func() {
		f.disallowMutationWhileIterating.Store(false)
	}
}

// Entries invokes the given callback with the current timestamp for each
// component span in the tracked span set.
func (f *btreeFrontier) Entries(fn Operation) {
	defer f.disallowMutations()()

	it := f.tree.MakeIter()
	for it.First(); it.Valid(); it.Next() {
		if fn(it.Cur().span(), it.Cur().ts) == StopMatch {
			break
		}
	}
}

// SpanEntries invokes op for each sub-span of the specified span with the
// timestamp as observed by this frontier.
//
// Time
// 5|      .b__c               .
// 4|      .             h__k  .
// 3|      .      e__f         .
// 1 ---a----------------------m---q-- Frontier
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
func (f *btreeFrontier) SpanEntries(span roachpb.Span, op Operation) {
	defer f.disallowMutations()()

	todoRange := newSearchKey(span.Key, span.EndKey)
	defer putFrontierEntry(todoRange)

	it := f.tree.MakeIter()
	for it.FirstOverlap(todoRange); it.Valid(); it.NextOverlap(todoRange) {
		e := it.Cur()

		// Skip untracked portion.
		if todoRange.Start.Compare(e.Start) < 0 {
			todoRange.Start = e.Start
		}

		end := e.End
		if e.End.Compare(todoRange.End) > 0 {
			end = todoRange.End
		}

		if op(roachpb.Span{Key: todoRange.Start, EndKey: end}, e.ts) == StopMatch {
			return
		}
		todoRange.Start = end
	}
}

// String implements Stringer.
func (f *btreeFrontier) String() string {
	defer f.disallowMutations()()

	var buf strings.Builder
	it := f.tree.MakeIter()
	for it.First(); it.Valid(); it.Next() {
		if buf.Len() != 0 {
			buf.WriteString(` `)
		}
		buf.WriteString(it.Cur().String())
	}
	return buf.String()
}

// Len implements Frontier.
func (f *btreeFrontier) Len() int {
	return f.tree.Len()
}

func (e *btreeFrontierEntry) ID() uint64 {
	return e.id
}

func (e *btreeFrontierEntry) Key() []byte {
	return e.Start
}

func (e *btreeFrontierEntry) EndKey() []byte {
	return e.End
}

func (e *btreeFrontierEntry) New() *btreeFrontierEntry {
	return &btreeFrontierEntry{}
}

func (e *btreeFrontierEntry) SetID(id uint64) {
	e.id = id
}

func (e *btreeFrontierEntry) SetKey(k []byte) {
	e.Start = k
}

func (e *btreeFrontierEntry) SetEndKey(k []byte) {
	e.End = k
}

func (e *btreeFrontierEntry) String() string {
	return fmt.Sprintf("[%s@%s]", e.span(), e.ts)
}

func (e *btreeFrontierEntry) span() roachpb.Span {
	return roachpb.Span{Key: e.Start, EndKey: e.End}
}

// isEmptyRange returns true if btreeFrontier entry range is empty.
func (e *btreeFrontierEntry) isEmptyRange() bool {
	return e.Start.Compare(e.End) >= 0
}

// frontierHeap implements heap.Interface and holds `btreeFrontierEntry`s. Entries
// are sorted based on their timestamp such that the oldest will rise to the top
// of the heap.
type frontierHeap []*btreeFrontierEntry

// Len implements heap.Interface.
func (h frontierHeap) Len() int { return len(h) }

// Less implements heap.Interface.
func (h frontierHeap) Less(i, j int) bool {
	if h[i].ts == h[j].ts {
		return h[i].Start.Compare(h[j].Start) < 0
	}
	return h[i].ts.Less(h[j].ts)
}

// Swap implements heap.Interface.
func (h frontierHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx, h[j].heapIdx = i, j
}

// Push implements heap.Interface.
func (h *frontierHeap) Push(x interface{}) {
	n := len(*h)
	entry := x.(*btreeFrontierEntry)
	entry.heapIdx = n
	*h = append(*h, entry)
}

// Pop implements heap.Interface.
func (h *frontierHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	entry.heapIdx = -1 // for safety
	old[n-1] = nil     // for gc
	*h = old[0 : n-1]
	return entry
}

// newFrontierEntry/putFrontierEntry provide access to pooled *btreeFrontierEntry.
var newFrontierEntry, putFrontierEntry = func() (
	func(id *uint64, start, end roachpb.Key, ts hlc.Timestamp) *btreeFrontierEntry,
	func(e *btreeFrontierEntry),
) {
	entryPool := sync.Pool{New: func() any { return new(btreeFrontierEntry) }}

	newEntry := func(idAlloc *uint64, start, end roachpb.Key, ts hlc.Timestamp) *btreeFrontierEntry {
		e := entryPool.Get().(*btreeFrontierEntry)
		var id uint64
		if idAlloc != nil {
			id = *idAlloc
			*idAlloc++
		}
		*e = btreeFrontierEntry{
			Start:   start,
			End:     end,
			id:      id,
			ts:      ts,
			heapIdx: -1,
		}

		if expensiveChecksEnabled() {
			e.spanCopy.Key = append(e.spanCopy.Key, start...)
			e.spanCopy.EndKey = append(e.spanCopy.EndKey, end...)
		}

		return e
	}
	putEntry := func(e *btreeFrontierEntry) {
		e.Start = nil
		e.End = nil
		e.spanCopy.Key = nil
		e.spanCopy.EndKey = nil
		entryPool.Put(e)
	}
	return newEntry, putEntry
}()

// newSearchKey returns btreeFrontierEntry that can be used to search/seek
// in the btree.
var newSearchKey = func(start, end roachpb.Key) *btreeFrontierEntry {
	return newFrontierEntry(nil, start, end, hlc.Timestamp{})
}

// checkSpan validates span.
func checkSpan(s roachpb.Span) error {
	switch s.Key.Compare(s.EndKey) {
	case 1:
		return errors.Wrapf(interval.ErrInvertedRange, "inverted span %s", s)
	case 0:
		if len(s.Key) == 0 && len(s.EndKey) == 0 {
			return errors.Wrapf(interval.ErrNilRange, "nil span %s", s)
		}
		return errors.Wrapf(interval.ErrEmptyRange, "empty span %s", s)
	default:
		return nil
	}
}

// checkUnsafeKeyModification is an expensive check performed under tests
// to verify that the caller did not mutate span keys after adding/forwarding them.
func (f *btreeFrontier) checkUnsafeKeyModification() error {
	it := f.tree.MakeIter()
	for it.First(); it.Valid(); it.Next() {
		cur := it.Cur()
		if !cur.Start.Equal(cur.spanCopy.Key) || !cur.End.Equal(cur.spanCopy.EndKey) {
			return errors.Newf("unsafe span key modification: was %s, now %s", cur.spanCopy, cur.span())
		}
	}
	return nil
}

func (f *btreeFrontier) checkDisallowedMutation() error {
	if f.disallowMutationWhileIterating.Load() {
		err := errors.AssertionFailedWithDepthf(1, "attempt to mutate frontier while iterating")
		if buildutil.CrdbTestBuild {
			panic(err)
		}
		return err
	}
	return nil
}

var disableSanityChecksForBenchmark bool

func expensiveChecksEnabled() bool {
	return buildutil.CrdbTestBuild && !disableSanityChecksForBenchmark
}

// spanDifference subtracts frontier (spans) from this span, and
// returns set difference.
func spanDifference(s roachpb.Span, f Frontier) []roachpb.Span {
	var sg roachpb.SpanGroup
	sg.Add(s)

	f.SpanEntries(s, func(overlap roachpb.Span, ts hlc.Timestamp) (done OpResult) {
		sg.Sub(overlap)
		return false
	})

	return sg.Slice()
}

type concurrentFrontier struct {
	syncutil.Mutex
	f Frontier
}

var _ Frontier = (*concurrentFrontier)(nil)

// AddSpansAt implements Frontier.
func (f *concurrentFrontier) AddSpansAt(startAt hlc.Timestamp, spans ...roachpb.Span) error {
	f.Lock()
	defer f.Unlock()
	return f.f.AddSpansAt(startAt, spans...)
}

// Frontier implements Frontier.
func (f *concurrentFrontier) Frontier() hlc.Timestamp {
	f.Lock()
	defer f.Unlock()
	return f.f.Frontier()
}

// PeekFrontierSpan implements Frontier.
func (f *concurrentFrontier) PeekFrontierSpan() roachpb.Span {
	f.Lock()
	defer f.Unlock()
	return f.f.PeekFrontierSpan()
}

// Forward implements Frontier.
func (f *concurrentFrontier) Forward(span roachpb.Span, ts hlc.Timestamp) (bool, error) {
	f.Lock()
	defer f.Unlock()
	return f.f.Forward(span, ts)
}

// Release implements Frontier.
func (f *concurrentFrontier) Release() {
	f.Lock()
	defer f.Unlock()
	f.f.Release()
}

// Entries implements Frontier.
func (f *concurrentFrontier) Entries(fn Operation) {
	f.Lock()
	defer f.Unlock()
	f.f.Entries(fn)
}

// SpanEntries implements Frontier.
func (f *concurrentFrontier) SpanEntries(span roachpb.Span, op Operation) {
	f.Lock()
	defer f.Unlock()
	f.f.SpanEntries(span, op)
}

// Len implements Frontier.
func (f *concurrentFrontier) Len() int {
	f.Lock()
	defer f.Unlock()
	return f.f.Len()
}

// String implements Frontier.
func (f *concurrentFrontier) String() string {
	f.Lock()
	defer f.Unlock()
	return f.f.String()
}
