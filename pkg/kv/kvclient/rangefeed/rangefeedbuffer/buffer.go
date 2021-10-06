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
	"context"
	"sort"

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
// TODO(irfansharif): We could also de-dup values with the same timestamp
// instead of letting the caller handle it themselves.
type Buffer struct {
	limit int

	mu struct {
		syncutil.Mutex
		rangefeedEvents
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

		// TODO(irfansharif): Do we care about accumulating checkpoint events as
		// well?
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if eventTS(ev).LessEq(b.mu.frontier) {
		// If the rangefeed entry is at a timestamp less than or equal to our
		// last known frontier, we can discard it.
		return nil
	}

	if b.mu.rangefeedEvents.Len()+1 > b.limit {
		return ErrBufferLimitExceeded
	}

	b.mu.rangefeedEvents = append(b.mu.rangefeedEvents, ev)
	return nil
}

// Flush returns the timestamp sorted list of accumulated events with timestamps
// less than or equal to the one provided frontier timestamp (typically invoked
// whenever it's advanced). The timestamp is recorded, and future events with
// timestamps less than or equal to it are discarded.
func (b *Buffer) Flush(
	ctx context.Context, frontier hlc.Timestamp,
) (events []*roachpb.RangeFeedEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if frontier.Less(b.mu.frontier) {
		log.Fatalf(ctx, "frontier timestamp regressed: saw %s, previously %s", frontier, b.mu.frontier)
	}

	// Accumulate all events with timestamps <= the given timestamp in sorted
	// order.
	sort.Sort(b.mu.rangefeedEvents)
	idx := sort.Search(len(b.mu.rangefeedEvents), func(i int) bool {
		return !eventTS(b.mu.rangefeedEvents[i]).LessEq(frontier)
	})

	events = b.mu.rangefeedEvents[:idx]
	b.mu.rangefeedEvents = b.mu.rangefeedEvents[idx:]
	b.mu.frontier = frontier
	return events
}

func eventTS(ev *roachpb.RangeFeedEvent) hlc.Timestamp {
	value := ev.Val.Value
	if !value.IsPresent() {
		value = ev.Val.PrevValue
	}
	return value.Timestamp
}

type rangefeedEvents []*roachpb.RangeFeedEvent

var _ sort.Interface = (*rangefeedEvents)(nil)

func (events rangefeedEvents) Len() int           { return len(events) }
func (events rangefeedEvents) Less(i, j int) bool { return eventTS(events[i]).Less(eventTS(events[j])) }
func (events rangefeedEvents) Swap(i, j int)      { events[i], events[j] = events[j], events[i] }
