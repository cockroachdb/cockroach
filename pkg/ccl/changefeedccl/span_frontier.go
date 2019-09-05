// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"container/heap"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
)

// spanFrontierEntry represents a timestamped span. It is used as the nodes in
// both the interval tree and heap needed to keep the spanFrontier.
type spanFrontierEntry struct {
	id   int64
	keys interval.Range
	span roachpb.Span
	ts   hlc.Timestamp

	// The index of the item in the spanFrontierHeap, maintained by the
	// heap.Interface methods.
	index int
}

// ID implements interval.Interface.
func (s *spanFrontierEntry) ID() uintptr {
	return uintptr(s.id)
}

// Range implements interval.Interface.
func (s *spanFrontierEntry) Range() interval.Range {
	return s.keys
}

func (s *spanFrontierEntry) String() string {
	return fmt.Sprintf("[%s @ %s]", s.span, s.ts)
}

// spanFrontierHeap implements heap.Interface and holds `spanFrontierEntry`s.
// Entries are sorted based on their timestamp such that the oldest will rise to
// the top of the heap.
type spanFrontierHeap []*spanFrontierEntry

// Len implements heap.Interface.
func (h spanFrontierHeap) Len() int { return len(h) }

// Less implements heap.Interface.
func (h spanFrontierHeap) Less(i, j int) bool {
	if h[i].ts == h[j].ts {
		return h[i].span.Key.Compare(h[j].span.Key) < 0
	}
	return h[i].ts.Less(h[j].ts)
}

// Swap implements heap.Interface.
func (h spanFrontierHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

// Push implements heap.Interface.
func (h *spanFrontierHeap) Push(x interface{}) {
	n := len(*h)
	entry := x.(*spanFrontierEntry)
	entry.index = n
	*h = append(*h, entry)
}

// Pop implements heap.Interface.
func (h *spanFrontierHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	entry.index = -1 // for safety
	old[n-1] = nil   // for gc
	*h = old[0 : n-1]
	return entry
}

// spanFrontier tracks the minimum timestamp of a set of spans.
type spanFrontier struct {
	// tree contains `*spanFrontierEntry` items for the entire current tracked
	// span set. Any tracked spans that have never been `Forward`ed will have a
	// zero timestamp. If any entries needed to be split along a tracking
	// boundary, this has already been done by `insert` before it entered the
	// tree.
	tree interval.Tree
	// minHeap contains the same `*spanFrontierEntry` items as `tree`. Entries
	// in the heap are sorted first by minimum timestamp and then by lesser
	// start key.
	minHeap spanFrontierHeap

	idAlloc int64
}

func makeSpanFrontier(spans ...roachpb.Span) *spanFrontier {
	s := &spanFrontier{tree: interval.NewTree(interval.ExclusiveOverlapper)}
	for _, span := range spans {
		e := &spanFrontierEntry{
			id:   s.idAlloc,
			keys: span.AsRange(),
			span: span,
			ts:   hlc.Timestamp{},
		}
		s.idAlloc++
		if err := s.tree.Insert(e, true /* fast */); err != nil {
			panic(err)
		}
		heap.Push(&s.minHeap, e)
	}
	s.tree.AdjustRanges()
	return s
}

// Frontier returns the minimum timestamp being tracked.
func (s *spanFrontier) Frontier() hlc.Timestamp {
	if s.minHeap.Len() == 0 {
		return hlc.Timestamp{}
	}
	return s.minHeap[0].ts
}

func (s *spanFrontier) peekFrontierSpan() roachpb.Span {
	if s.minHeap.Len() == 0 {
		return roachpb.Span{}
	}
	return s.minHeap[0].span
}

// Forward advances the timestamp for a span. Any part of the span that doesn't
// overlap the tracked span set will be ignored. True is returned if the
// frontier advanced as a result.
//
// Note that internally, it may be necessary to use multiple entries to
// represent this timestamped span (e.g. if it overlaps with the tracked span
// set boundary). Similarly, an entry created by a previous Forward may be
// partially overlapped and have to be split into two entries.
func (s *spanFrontier) Forward(span roachpb.Span, ts hlc.Timestamp) bool {
	prevFrontier := s.Frontier()
	s.insert(span, ts)
	return prevFrontier.Less(s.Frontier())
}

func (s *spanFrontier) insert(span roachpb.Span, ts hlc.Timestamp) {
	entryKeys := span.AsRange()
	overlapping := s.tree.Get(entryKeys)

	// TODO(dan): OverlapCoveringMerge is overkill, do this without it. See
	// `tscache/treeImpl.Add` for inspiration.
	entryCov := covering.Covering{{Start: span.Key, End: span.EndKey, Payload: ts}}
	overlapCov := make(covering.Covering, len(overlapping))
	for i, o := range overlapping {
		spe := o.(*spanFrontierEntry)
		overlapCov[i] = covering.Range{
			Start: spe.span.Key, End: spe.span.EndKey, Payload: spe,
		}
	}
	merged := covering.OverlapCoveringMerge([]covering.Covering{entryCov, overlapCov})

	toInsert := make([]spanFrontierEntry, 0, len(merged))
	for _, m := range merged {
		// Compute the newest timestamp seen for this span and note whether it's
		// tracked. There will be either 1 or 2 payloads. If there's 2, it will
		// be the new span and the old entry. If it's 1 it could be either a new
		// span (which is untracked and should be ignored) or an old entry which
		// has been clipped.
		var mergedTs hlc.Timestamp
		var tracked bool
		for _, payload := range m.Payload.([]interface{}) {
			switch p := payload.(type) {
			case hlc.Timestamp:
				if mergedTs.Less(p) {
					mergedTs = p
				}
			case *spanFrontierEntry:
				tracked = true
				if mergedTs.Less(p.ts) {
					mergedTs = p.ts
				}
			}
		}
		// TODO(dan): Collapse span-adjacent entries with the same value for
		// timestamp and tracked to save space.
		if tracked {
			toInsert = append(toInsert, spanFrontierEntry{
				id:   s.idAlloc,
				keys: interval.Range{Start: m.Start, End: m.End},
				span: roachpb.Span{Key: m.Start, EndKey: m.End},
				ts:   mergedTs,
			})
			s.idAlloc++
		}
	}

	// All the entries in `overlapping` have been replaced by updated ones in
	// `toInsert`, so remove them all from the tree and heap.
	needAdjust := false
	if len(overlapping) == 1 {
		spe := overlapping[0].(*spanFrontierEntry)
		if err := s.tree.Delete(spe, false /* fast */); err != nil {
			panic(err)
		}
		heap.Remove(&s.minHeap, spe.index)
	} else {
		for i := range overlapping {
			spe := overlapping[i].(*spanFrontierEntry)
			if err := s.tree.Delete(spe, true /* fast */); err != nil {
				panic(err)
			}
			heap.Remove(&s.minHeap, spe.index)
		}
		needAdjust = true
	}
	// Then insert!
	if len(toInsert) == 1 {
		if err := s.tree.Insert(&toInsert[0], false /* fast */); err != nil {
			panic(err)
		}
		heap.Push(&s.minHeap, &toInsert[0])
	} else {
		for i := range toInsert {
			if err := s.tree.Insert(&toInsert[i], true /* fast */); err != nil {
				panic(err)
			}
			heap.Push(&s.minHeap, &toInsert[i])
		}
		needAdjust = true
	}
	if needAdjust {
		s.tree.AdjustRanges()
	}
}

// Entries invokes the given callback with the current timestamp for each
// component span in the tracked span set.
func (s *spanFrontier) Entries(fn func(roachpb.Span, hlc.Timestamp)) {
	s.tree.Do(func(i interval.Interface) bool {
		spe := i.(*spanFrontierEntry)
		fn(spe.span, spe.ts)
		return false
	})
}

func (s *spanFrontier) String() string {
	var buf strings.Builder
	s.tree.Do(func(i interval.Interface) bool {
		if buf.Len() != 0 {
			buf.WriteString(` `)
		}
		buf.WriteString(i.(*spanFrontierEntry).String())
		return false
	})
	return buf.String()
}
