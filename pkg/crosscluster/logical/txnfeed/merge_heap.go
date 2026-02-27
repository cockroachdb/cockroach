// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// mergeEntry tracks the state of a single input subscription in the heap.
type mergeEntry struct {
	orderedSub streamclient.Subscription

	// peekedKVs holds the remaining KVs from the current peeked event when it is
	// a KV event. KVs are sorted in increasing timestamp order. As KVs are
	// consumed, the slice is resliced forward. The KVs are sorted in (timestamp,
	// key) order.
	peekedKVs []streampb.StreamEvent_KV

	// peekedCheckpoint holds the checkpoint from the current peeked event when
	// it is a checkpoint event.
	peekedCheckpoint *streampb.StreamEvent_StreamCheckpoint

	// peekTS caches the MVCC timestamp of the current peeked item so that Less
	// can compare entries without recomputing it. All remaining peekedKVs have
	// timestamps >= peekTS.
	peekTS hlc.Timestamp
}

// isKV reports whether the current peeked item is a KV.
func (e *mergeEntry) isKV() bool {
	return e.peekedKVs != nil
}

// currentKV returns the first KV in the peeked batch.
func (e *mergeEntry) currentKV() streampb.StreamEvent_KV {
	return e.peekedKVs[0]
}

// advanceKV moves to the next KV within the current batch by reslicing. If
// there are remaining KVs, it updates peekTS and returns. Otherwise it falls
// through to advanceFromChannel to read the next event.
func (e *mergeEntry) advanceKV(ctx context.Context) (exhausted bool, _ error) {
	e.peekedKVs[0] = streampb.StreamEvent_KV{} // allow GC of consumed element
	e.peekedKVs = e.peekedKVs[1:]
	if len(e.peekedKVs) != 0 {
		e.peekTS = e.peekedKVs[0].KeyValue.Value.Timestamp
		return false, nil
	}
	return e.advanceFromChannel(ctx)
}

// advanceFromChannel reads the next event from the subscription channel and
// populates the appropriate peeked fields.
func (e *mergeEntry) advanceFromChannel(ctx context.Context) (exhausted bool, _ error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case ev, ok := <-e.orderedSub.Events():
		if !ok {
			if e.orderedSub.Err() != nil {
				return false, e.orderedSub.Err()
			}
			return true, nil
		}
		switch ev.Type() {
		case crosscluster.KVEvent:
			e.peekedKVs = ev.GetKVs()
			e.peekedCheckpoint = nil
			e.peekTS = e.peekedKVs[0].KeyValue.Value.Timestamp
		case crosscluster.CheckpointEvent:
			e.peekedKVs = nil
			e.peekedCheckpoint = ev.GetCheckpoint()
			e.peekTS = e.peekedCheckpoint.ResolvedSpans[0].Timestamp
		default:
			return false, errors.AssertionFailedf(
				"unexpected event type in advanceFromChannel: %s", redact.Safe(ev.Type()))
		}
		return false, nil
	}
}

// mergeHeap is a min-heap of mergeEntry pointers ordered by event timestamp. At equal
// timestamps, KVEvents sort before CheckpointEvents so that data is always
// emitted before the checkpoint that resolves it.
type mergeHeap []*mergeEntry

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	if cmp := h[i].peekTS.Compare(h[j].peekTS); cmp != 0 {
		return cmp < 0
	}
	// At the same timestamp, emit KVs before checkpoints so that data is
	// always delivered before the checkpoint that resolves it.
	return h[i].isKV() && !h[j].isKV()
}

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x *mergeEntry) {
	*h = append(*h, x)
}

func (h *mergeHeap) Pop() *mergeEntry {
	n := len(*h)
	item := (*h)[n-1]
	(*h)[n-1] = nil // avoid retaining a reference in the underlying array
	*h = (*h)[:n-1]
	return item
}

func (h *mergeHeap) init()              { heap.Init(h) }
func (h *mergeHeap) push(e *mergeEntry) { heap.Push(h, e) }
func (h *mergeHeap) pop() *mergeEntry   { return heap.Pop(h) }
func (h *mergeHeap) len() int           { return len(*h) }

func (h *mergeHeap) peek() *mergeEntry {
	if len(*h) == 0 {
		return nil
	}
	return (*h)[0]
}

// isResolved reports whether all KVs at the given timestamp have been consumed
// from the heap. It returns true if the heap is empty, the next entry is a
// checkpoint with a timestamp >= ts, or the next entry is a KV with a timestamp
// > ts.
func (h *mergeHeap) isResolved(ts hlc.Timestamp) bool {
	e := h.peek()
	if e == nil {
		return true
	}
	if e.isKV() {
		return ts.Less(e.peekTS)
	}
	return ts.LessEq(e.peekTS)
}
