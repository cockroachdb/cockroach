// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import (
	"context"
	"time"
)

// A Timing is a container that stores Events resulting from a single-threaded
// execution of an operation, which it thus breaks up into contiguous (i.e.
// non-overlapping and without gaps) time intervals. The use of a Timing
// integrates easily into existing code paths and interacts well with both
// metrics and tracing.
//
// With each event added, the caller learns the duration elapsed since the last
// event (or another previous event of choice), which facilitates recording the
// durations of sub-operations to metrics. A "middleware" OnEvent can be set up
// to tie into tracing, so that existing calls to `log.Event` can be lifted to
// calls to `Timing.Event`.
type Timing struct {
	Now func() time.Time
	// OnEvent is called every time Event is called. It will be invoked directly
	// from Event, i.e. logging in OnEvent needs to skip two stack frames to get
	// to the caller of Event.
	OnEvent func(context.Context, interface{}, time.Time)

	ents []entry
}

type entry struct {
	ts time.Time
	tr interface{}
}

// Reset resets the Timing. This prepares it for recording a new
// operation while allowing re-use of underlying memory and configuration.
func (tm *Timing) Reset() {
	tm.ents = append(tm.ents[:0], entry{
		ts: tm.Now(),
	})
}

func (tm *Timing) lastIdx() int {
	return len(tm.ents) - 1
}

type End struct {
	Event interface{}
}

// Event records an event to the timing. It returns the duration
// elapsed since the last Event, and the internal index assigned
// to the new event (for use in Between). The same event may be
// recorded multiple times, which is useful in case of retry
// loops.
//
// Note that the returned duration is only meaningful if the preceding
// event is stable. In the following code, the likelyhood of a third event
// being present (or slipping in down the road) is high and would lead to
// under-reporting of the duration attributed to 'pouncing':
//
//   dur, _ := tm.Event(ctx, "pouncing ends")
//   doSomethingIncludingMaybeAddAnotherEvent(tm)
//   fmt.Printf("pouncing took %s!", dur)
//
// The use of Between is generally preferable to avoid this problem in all but
// trivially correct code.
func (tm *Timing) Event(ctx context.Context, tr interface{}) (time.Duration, int) {
	prev := tm.ents[tm.lastIdx()]
	ts := tm.Now()
	tm.ents = append(tm.ents, entry{
		ts: ts,
		tr: tr,
	})
	idx := tm.lastIdx()
	tm.OnEvent(ctx, tr, ts)
	return tm.ents[idx].ts.Sub(prev.ts), idx
}

// Between returns the duration elapsed between two calls to Event as identified
// by their respective indexes.
func (tm *Timing) Between(from, to int) time.Duration {
	return tm.ents[to].ts.Sub(tm.ents[from].ts)
}

func (tm *Timing) Duration() time.Duration {
	last := tm.lastIdx()
	if last == 0 {
		return 0
	}
	return tm.Between(0, last)
}

func (tm *Timing) Summary() map[interface{}]time.Duration {
	res := make(map[interface{}]time.Duration)
	m := map[interface{}]int{} // idx, key is never &End{X}
	for i := range tm.ents {
		el := &tm.ents[i]
		e, ok := el.tr.(*End)
		if !ok {
			idx, open := m[el.tr]
			if open {
				// Invalid nesting, close current interval and start new one.
				res[el.tr] += tm.Between(idx, i)
				// Fall through.
			}
			m[el.tr] = i
			continue
		}
		// Closing an existing interval. If it isn't open, that's a programming
		// error but we'll handle it gracefully by ignoring it.
		idx, open := m[e]
		if !open {
			continue // ignore
		}
		res[el.tr] += tm.Between(idx, i)
		delete(m, el.tr)
	}
	for e, idx := range m {
		// Close any leftover events.
		res[e] += tm.Between(idx, tm.lastIdx())
	}
	return res
}

// TODO(tbg): provide a method to summarize the timings.
// This could power a granular per-range breakdown of execution
// timings via a map[struct{A, B interface{}}ewma.MovingAverage.
// where there is an entry for any events A and B that were ever
// observed in succession. We should only do this if we know that
// the set of possible events is small, for example if they are
// guaranteed to come from a small set of singletons (as is the
// envisioned reality).
