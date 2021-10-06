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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Buffer provides a thin memory-bounded buffer to sit on top of a rangefeed. It
// accumulates raw rangefeed events, including checkpoints, at which point a
// timestamp sorted batch of events is made available to the user. If we're past
// the configured memory limit before having received a checkpoint, we error out
// to the caller.
//
// TODO(irfansharif): We could also de-dup away values with the same timestamp,
// instead of letting the caller handle that themselves.
type Buffer struct {
	mu struct {
		syncutil.Mutex
		entryHeap
		checkpointTS hlc.Timestamp
	}
}

// New constructs a Buffer.
func New() *Buffer {
	return &Buffer{}
}

// Add adds the given rangefeed entry to the buffer.
func (b *Buffer) Add(ctx context.Context, ev *roachpb.RangeFeedEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if checkpoint := ev.Checkpoint; checkpoint != nil {
		b.mu.checkpointTS.Forward(checkpoint.ResolvedTS)
		return nil
	}

	if ev.Error != nil {
		return ev.Error.Error.GoError()
	}

	// XXX: Actually do some memory accounting here. We probably want the caller
	// to provide the size of the entry itself, either as part of each entry or
	// just an over-estimate during buffer initialization.

	value := ev.Val.Value
	if !value.IsPresent() {
		value = ev.Val.PrevValue
	}

	if value.Timestamp.LessEq(b.mu.checkpointTS) {
		// If the rangefeed entry is at a timestamp less than or equal to our
		// last known checkpoint, we don't need to record it.
		return nil
	}

	heap.Push(&b.mu.entryHeap, &entry{RangeFeedEvent: ev, Timestamp: value.Timestamp})
	return nil
}

func (b *Buffer) Flush() (events []*roachpb.RangeFeedEvent, checkpoint hlc.Timestamp) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Accumulate all events with timestamps <= the last checkpoint.
	for {
		if len(b.mu.entryHeap) == 0 || !b.mu.entryHeap[0].Timestamp.LessEq(b.mu.checkpointTS) {
			break
		}

		ev := heap.Pop(&b.mu.entryHeap).(*entry)
		// XXX: Checkpoint events cover a specific keyspan, don't we then want
		// to only flush out events overlapping with the checkpoint keyspan (and
		// <= its timestamp?)
		events = append(events, ev.RangeFeedEvent)
	}

	return events, b.mu.checkpointTS
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
