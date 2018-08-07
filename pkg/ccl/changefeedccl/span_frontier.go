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

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/intervalccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

	// tracked is true when this entry is included in the set of tracked spans.
	tracked bool

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
	// tree contains `*spanFrontierEntry` items for all span `Forward`s.
	tree interval.Tree
	// minHeap contains `*spanFrontierEntry` items for only the current set of
	// tracked spans. If any entries needed to be split along a tracking
	// boundary, this has already been done by `insert` before it entered the
	// heap. Entries in the heap are sorted first by minimum timestamp and then
	// by lesser start key.
	minHeap spanFrontierHeap

	idAlloc int64
}

func makeSpanFrontier() *spanFrontier {
	return &spanFrontier{tree: interval.NewTree(interval.ExclusiveOverlapper)}
}

// ChangeTrackedSpans alters the set of spans used to calculate the Frontier. If
// the new span set is not a subset of the current one, the Frontier may recede.
func (s *spanFrontier) ChangeTrackedSpans(spans ...roachpb.Span) {
	// Reset the tree and heap.
	oldTree := s.tree
	s.tree = interval.NewTree(interval.ExclusiveOverlapper)
	s.minHeap = s.minHeap[:0]

	// Fill in entries for every tracked span, which is used by `insert` to
	// decide what to keep in the
	for _, span := range spans {
		entry := &spanFrontierEntry{
			id:      s.idAlloc,
			keys:    span.AsRange(),
			span:    span,
			ts:      hlc.Timestamp{},
			tracked: true,
		}
		s.idAlloc++
		if err := s.tree.Insert(entry, false /* fast */); err != nil {
			panic(err)
		}
		heap.Push(&s.minHeap, entry)
	}

	// Finally, re-insert the old progress.
	oldTree.Do(func(i interval.Interface) bool {
		ts := i.(*spanFrontierEntry)
		s.insert(ts.span, ts.ts)
		return false
	})
}

// Len returns the number of actively tracked span timestamps.
func (s *spanFrontier) Len() int {
	return s.minHeap.Len()
}

// Frontier returns the minimum timestamp being tracked.
func (s *spanFrontier) Frontier() hlc.Timestamp {
	if s.minHeap.Len() == 0 {
		return hlc.Timestamp{}
	}
	return s.minHeap[0].ts
}

// Forward advances the timestamp for a span. If the span doesn't overlap (or
// partially overlaps) the tracked span set, the currently untracked parts will
// be noted, but not used to calculate the Frontier. True is returned if the
// frontier advanced as a result.
//
// Note that internally, it may be necessary to use multiple entries to
// represent this timestamped span (e.g. if it overlaps with the tracked span
// set boundary). Similarly, an entry created by a previous Forward may be
// partially overlapped and have to be split into two entries. Thus, there is no
// easy relationship between this and the output of `Len`.
func (s *spanFrontier) Forward(span roachpb.Span, ts hlc.Timestamp) bool {
	prevFrontier := s.Frontier()
	s.insert(span, ts)
	return prevFrontier.Less(s.Frontier())
}

func (s *spanFrontier) insert(span roachpb.Span, ts hlc.Timestamp) {
	entryKeys := span.AsRange()
	overlapping := s.tree.Get(entryKeys)

	// TODO(dan): OverlapCoveringMerge is overkill, do this without it.
	entryCov := intervalccl.Covering{{Start: span.Key, End: span.EndKey, Payload: ts}}
	overlapCov := make(intervalccl.Covering, len(overlapping))
	for i, o := range overlapping {
		spe := o.(*spanFrontierEntry)
		overlapCov[i] = intervalccl.Range{
			Start: spe.span.Key, End: spe.span.EndKey, Payload: spe,
		}
	}
	merged := intervalccl.OverlapCoveringMerge([]intervalccl.Covering{entryCov, overlapCov})

	toInsert := make([]spanFrontierEntry, len(merged))
	for i, m := range merged {
		// Compute the newest timestamp seen for this span and note whether it's
		// tracked.
		var mergedTs hlc.Timestamp
		var tracked bool
		for _, payload := range m.Payload.([]interface{}) {
			switch p := payload.(type) {
			case hlc.Timestamp:
				if mergedTs.Less(p) {
					mergedTs = p
				}
			case *spanFrontierEntry:
				if mergedTs.Less(p.ts) {
					mergedTs = p.ts
				}
				tracked = tracked || p.tracked
			}
		}
		// TODO(dan): Collapse span-adjacent entries with the same value for
		// timestamp and tracked to save space.
		toInsert[i] = spanFrontierEntry{
			id:      s.idAlloc,
			keys:    interval.Range{Start: m.Start, End: m.End},
			span:    roachpb.Span{Key: m.Start, EndKey: m.End},
			ts:      mergedTs,
			tracked: tracked,
		}
		s.idAlloc++
	}

	// All the entries in `overlapping` have been replaced by updated ones in
	// `toInsert`, so remove them all from the tree. Remove only the tracked
	// ones from the heap (because only the tracked ones are in the heap).
	needAdjust := false
	if len(overlapping) == 1 {
		spe := overlapping[0].(*spanFrontierEntry)
		if err := s.tree.Delete(spe, false /* fast */); err != nil {
			panic(err)
		}
		if spe.tracked {
			heap.Remove(&s.minHeap, spe.index)
		}
	} else {
		for i := range overlapping {
			spe := overlapping[i].(*spanFrontierEntry)
			if err := s.tree.Delete(spe, true /* fast */); err != nil {
				panic(err)
			}
			if spe.tracked {
				heap.Remove(&s.minHeap, spe.index)
			}
		}
		needAdjust = true
	}
	// Then insert! As always, keep only the tracked entries in the heap.
	if len(toInsert) == 1 {
		if err := s.tree.Insert(&toInsert[0], false /* fast */); err != nil {
			panic(err)
		}
		if toInsert[0].tracked {
			heap.Push(&s.minHeap, &toInsert[0])
		}
	} else {
		for i := range toInsert {
			if err := s.tree.Insert(&toInsert[i], true /* fast */); err != nil {
				panic(err)
			}
			if toInsert[i].tracked {
				heap.Push(&s.minHeap, &toInsert[i])
			}
		}
		needAdjust = true
	}
	if needAdjust {
		s.tree.AdjustRanges()
	}
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
