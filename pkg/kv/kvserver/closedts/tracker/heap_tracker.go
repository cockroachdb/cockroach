// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracker

import (
	"container/heap"
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// heapTracker is a reference implementation of Tracker. Its LowerBound()
// response is precise: the lowest timestamps of the tracked set. The production
// implementation is the more performant lockfreeTracker.
//
// heapTracker maintains the currently tracked set of timestamps in a heap. Each
// element maintains its heap index, so random deletes are supported. All
// methods do internal locking, so all methods can be called concurrently.
type heapTracker struct {
	mu struct {
		syncutil.Mutex
		rs tsHeap
	}
}

var _ Tracker = &heapTracker{}

func newHeapTracker() Tracker {
	return &heapTracker{}
}

type item struct {
	ts hlc.Timestamp
	// This item's index in the heap.
	index int
}

type tsHeap []*item

var _ heap.Interface = &tsHeap{}

// Less is part of heap.Interface.
func (h tsHeap) Less(i, j int) bool {
	return h[i].ts.Less(h[j].ts)
}

// Swap is part of heap.Interface.
func (h tsHeap) Swap(i, j int) {
	tmp := h[i]
	h[i] = h[j]
	h[j] = tmp
	h[i].index = i
	h[j].index = j
}

// Push is part of heap.Interface.
func (h *tsHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*item)
	item.index = n
	*h = append(*h, item)
}

// Pop is part of heap.Interface.
func (h *tsHeap) Pop() interface{} {
	it := (*h)[len(*h)-1]
	// Poison the removed element, for safety.
	it.index = -1
	*h = (*h)[0 : len(*h)-1]
	return it
}

// Len is part of heap.Interface.
func (h *tsHeap) Len() int {
	return len(*h)
}

// heapToken implements RemovalToken.
type heapToken struct {
	*item
}

// RemovalTokenMarker implements RemovalToken.
func (heapToken) RemovalTokenMarker() {}

var _ RemovalToken = heapToken{}

// Track is part of the Tracker interface.
func (h *heapTracker) Track(ctx context.Context, ts hlc.Timestamp) RemovalToken {
	h.mu.Lock()
	defer h.mu.Unlock()
	i := &item{ts: ts}
	heap.Push(&h.mu.rs, i)
	return heapToken{i}
}

// Untrack is part of the Tracker interface.
func (h *heapTracker) Untrack(ctx context.Context, tok RemovalToken) {
	idx := tok.(heapToken).index
	if idx == -1 {
		log.Fatalf(ctx, "attempting to untrack already-untracked item")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	heap.Remove(&h.mu.rs, idx)
}

// LowerBound is part of the Tracker interface.
func (h *heapTracker) LowerBound(ctx context.Context) hlc.Timestamp {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.mu.rs.Len() == 0 {
		return hlc.Timestamp{}
	}
	return h.mu.rs[0].ts
}

// Count is part of the Tracker interface.
func (h *heapTracker) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.rs.Len()
}
