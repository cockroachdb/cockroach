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
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
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
	if h[i].ts == h[j].ts {
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

// MakeFrontier returns a Frontier that tracks the given set of spans.
func MakeFrontier(spans ...roachpb.Span) *Frontier {
	s := &Frontier{tree: interval.NewTree(interval.ExclusiveOverlapper)}
	for _, span := range spans {
		e := &frontierEntry{
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
func (f *Frontier) Forward(span roachpb.Span, ts hlc.Timestamp) bool {
	prevFrontier := f.Frontier()
	f.insert(span, ts)
	return prevFrontier.Less(f.Frontier())
}

func (f *Frontier) insert(span roachpb.Span, ts hlc.Timestamp) {
	entryKeys := span.AsRange()
	overlapping := f.tree.Get(entryKeys)

	// TODO(dan): OverlapCoveringMerge is overkill, do this without it. See
	// `tscache/treeImpl.Add` for inspiration.
	entryCov := covering.Covering{{Start: span.Key, End: span.EndKey, Payload: ts}}
	overlapCov := make(covering.Covering, len(overlapping))
	for i, o := range overlapping {
		spe := o.(*frontierEntry)
		overlapCov[i] = covering.Range{
			Start: spe.span.Key, End: spe.span.EndKey, Payload: spe,
		}
	}
	merged := covering.OverlapCoveringMerge([]covering.Covering{entryCov, overlapCov})

	toInsert := make([]frontierEntry, 0, len(merged))
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
			case *frontierEntry:
				tracked = true
				if mergedTs.Less(p.ts) {
					mergedTs = p.ts
				}
			}
		}
		// TODO(dan): Collapse span-adjacent entries with the same value for
		// timestamp and tracked to save space.
		if tracked {
			toInsert = append(toInsert, frontierEntry{
				id:   f.idAlloc,
				keys: interval.Range{Start: m.Start, End: m.End},
				span: roachpb.Span{Key: m.Start, EndKey: m.End},
				ts:   mergedTs,
			})
			f.idAlloc++
		}
	}

	// All the entries in `overlapping` have been replaced by updated ones in
	// `toInsert`, so remove them all from the tree and heap.
	needAdjust := false
	if len(overlapping) == 1 {
		spe := overlapping[0].(*frontierEntry)
		if err := f.tree.Delete(spe, false /* fast */); err != nil {
			panic(err)
		}
		heap.Remove(&f.minHeap, spe.index)
	} else {
		for i := range overlapping {
			spe := overlapping[i].(*frontierEntry)
			if err := f.tree.Delete(spe, true /* fast */); err != nil {
				panic(err)
			}
			heap.Remove(&f.minHeap, spe.index)
		}
		needAdjust = true
	}
	// Then insert!
	if len(toInsert) == 1 {
		if err := f.tree.Insert(&toInsert[0], false /* fast */); err != nil {
			panic(err)
		}
		heap.Push(&f.minHeap, &toInsert[0])
	} else {
		for i := range toInsert {
			if err := f.tree.Insert(&toInsert[i], true /* fast */); err != nil {
				panic(err)
			}
			heap.Push(&f.minHeap, &toInsert[i])
		}
		needAdjust = true
	}
	if needAdjust {
		f.tree.AdjustRanges()
	}
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
