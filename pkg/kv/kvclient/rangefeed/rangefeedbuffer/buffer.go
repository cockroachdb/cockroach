// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeedbuffer

import (
	"container/heap"
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// ErrBufferLimitExceeded is returned by the buffer when attempting to add more
// events than the limit the buffer is configured with.
var ErrBufferLimitExceeded = errors.New("rangefeed buffer limit exceeded")

// Buffer provides a thin memory-bounded buffer to sit on top of a rangefeed. It
// accumulates raw rangefeed events[1], which can be flushed out in timestamp
// sorted order en-masse whenever the rangefeed frontier is bumped. If we
// accumulate more events than the limit allows for, we error out to the caller.
//
// [1]: Rangefeed error events are propagated to the caller, checkpoint events
//      are discarded.
//
// TODO(irfansharif): We could also de-bounce values with the same timestamp,
// instead of letting the caller handle it themselves.
type Buffer struct {
	limit int

	mu struct {
		syncutil.Mutex
		entryHeap
		frontier hlc.Timestamp
	}
}

// New constructs a Buffer with the provided limit.
func New(limit int) *Buffer {
	return &Buffer{limit: limit}
}

// Add adds the given rangefeed entry to the buffer.
func (b *Buffer) Add(ctx context.Context, ev *roachpb.RangeFeedEvent) error {
	if ev.Error != nil {
		return ev.Error.Error.GoError()
	}

	if checkpoint := ev.Checkpoint; checkpoint != nil {
		// We don't care about rangefeed checkpoint events; discard.
		return nil

		// TODO(irfansharif): Should we instead accumulate these events as well,
		// with the resolved timestamps?
	}

	value := ev.Val.Value
	if !value.IsPresent() {
		value = ev.Val.PrevValue
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if value.Timestamp.LessEq(b.mu.frontier) {
		// If the rangefeed entry is at a timestamp less than or equal to our
		// last known checkpoint, we don't need to record it.
		return nil
	}

	if b.mu.entryHeap.Len()+1 > b.limit {
		return ErrBufferLimitExceeded
	}
	heap.Push(&b.mu.entryHeap, &entry{RangeFeedEvent: ev, Timestamp: value.Timestamp})

	return nil
}

// Flush returns the timestamp sorted list of accumulated events with timestamps
// less than or equal to the one provided (typically the rangefeed frontier
// timestamp, whenever it's advanced). The timestamp is recorded, and future
// events with timestamps less than or equal to it are discarded.
func (b *Buffer) Flush(
	ctx context.Context, frontier hlc.Timestamp,
) (events []*roachpb.RangeFeedEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if frontier.Less(b.mu.frontier) {
		log.Fatalf(ctx, "frontier timestamp regressed: saw %s, previously %s", frontier, b.mu.frontier)
	}

	// Accumulate all events with timestamps <= the given timestamp.
	for {
		if len(b.mu.entryHeap) == 0 || !b.mu.entryHeap[0].Timestamp.LessEq(frontier) {
			break
		}

		ev := heap.Pop(&b.mu.entryHeap).(*entry)
		events = append(events, ev.RangeFeedEvent)
	}

	b.mu.frontier = frontier
	return events
}

type entry struct {
	*roachpb.RangeFeedEvent
	hlc.Timestamp
}

// entryHeap is a min heap for rangefeed events and their corresponding
// timestamps.
type entryHeap []*entry

var _ heap.Interface = (*entryHeap)(nil)

// Len is part of heap.Interface.
func (ih *entryHeap) Len() int {
	return len(*ih)
}

// Less is part of heap.Interface.
func (ih *entryHeap) Less(i, j int) bool {
	return (*ih)[i].Timestamp.Less((*ih)[j].Timestamp)
}

// Swap is part of heap.Interface.
func (ih *entryHeap) Swap(i, j int) {
	(*ih)[i], (*ih)[j] = (*ih)[j], (*ih)[i]
}

// Push is part of heap.Interface.
func (ih *entryHeap) Push(x interface{}) {
	item := x.(*entry)
	*ih = append(*ih, item)
}

// Pop is part of heap.Interface.
func (ih *entryHeap) Pop() interface{} {
	old := *ih
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*ih = old[0 : n-1]
	return item
}
