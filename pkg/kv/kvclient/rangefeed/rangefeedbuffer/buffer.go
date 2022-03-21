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

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// ErrBufferLimitExceeded is returned by the buffer when attempting to add more
// events than the limit the buffer is configured with.
var ErrBufferLimitExceeded = errors.New("rangefeed buffer limit exceeded")

// Event is the unit of what can be added to the buffer.
type Event interface {
	Timestamp() hlc.Timestamp
}

// Buffer provides a thin memory-bounded buffer to sit on top of a rangefeed. It
// accumulates raw events which can then be flushed out in timestamp sorted
// order en-masse whenever the rangefeed frontier is bumped. If we accumulate
// more events than the limit allows for, we error out to the caller.
type Buffer struct {
	mu struct {
		syncutil.Mutex

		events
		frontier hlc.Timestamp
		limit    int
	}
}

// New constructs a Buffer with the provided limit.
func New(limit int) *Buffer {
	b := &Buffer{}
	b.mu.limit = limit
	return b
}

// Add adds the given entry to the buffer.
func (b *Buffer) Add(ev Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ev.Timestamp().LessEq(b.mu.frontier) {
		// If the entry is at a timestamp less than or equal to our last known
		// frontier, we can discard it.
		return nil
	}

	if b.mu.events.Len()+1 > b.mu.limit {
		return ErrBufferLimitExceeded
	}

	b.mu.events = append(b.mu.events, ev)
	return nil
}

// Flush returns the timestamp sorted list of accumulated events with timestamps
// less than or equal to the provided frontier timestamp. The timestamp is
// recorded (expected to monotonically increase), and future events with
// timestamps less than or equal to it are discarded.
func (b *Buffer) Flush(ctx context.Context, frontier hlc.Timestamp) (events []Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if frontier.Less(b.mu.frontier) {
		log.Fatalf(ctx, "frontier timestamp regressed: saw %s, previously %s", frontier, b.mu.frontier)
	}

	// Accumulate all events with timestamps <= the given timestamp in sorted
	// order.
	sort.Sort(&b.mu.events)
	idx := sort.Search(len(b.mu.events), func(i int) bool {
		return !b.mu.events[i].Timestamp().LessEq(frontier)
	})

	events = b.mu.events[:idx]
	b.mu.events = b.mu.events[idx:]
	b.mu.frontier = frontier
	return events
}

// SetLimit is used to limit the number of events the buffer internally tracks.
// If already in excess of the limit, future additions will error out (until the
// buffer is Flush()-ed at least).
func (b *Buffer) SetLimit(limit int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.mu.limit = limit
}

type events []Event

var _ sort.Interface = (*events)(nil)

func (es *events) Len() int           { return len(*es) }
func (es *events) Less(i, j int) bool { return (*es)[i].Timestamp().Less((*es)[j].Timestamp()) }
func (es *events) Swap(i, j int)      { (*es)[i], (*es)[j] = (*es)[j], (*es)[i] }
