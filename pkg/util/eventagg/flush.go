// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eventagg

import "time"

// FlushTrigger defines the interface used by aggregators, such as MapReduceAggregator,
// to determine when a flush of the current aggregation should be triggered.
//
// FlushTriggers are generally invoked as soon as events are consumed by an aggregator,
// but *before* the event itself is aggregated.
type FlushTrigger interface {
	// shouldFlush returns true if this FlushTrigger has been tripped.
	shouldFlush() bool
}

// WindowedFlush is a FlushTrigger which triggers flushes on a wall clock aligned
// time window.
//
// On initialization, a window duration is provided. Each time window will be a time
// truncated to that time window.
//
// For example, if we provide a window of 5 minutes, the windows will be:
//   - [12:00:00, 12:05:00)
//   - [12:05:00, 12:10:00)
//   - [12:10:00, 12:15:00)
//   - etc.
type WindowedFlush struct {
	window       time.Duration
	curWindowEnd time.Time
	nowFn        func() time.Time
}

var _ FlushTrigger = (*WindowedFlush)(nil)

// NewWindowedFlush returns a new WindowedFlush for the provided window.
func NewWindowedFlush(window time.Duration, nowFn func() time.Time) *WindowedFlush {
	w := &WindowedFlush{
		window: window,
		nowFn:  nowFn,
	}
	w.curWindowEnd = w.newWindowEnd()
	return w
}

func (w *WindowedFlush) newWindowEnd() time.Time {
	return w.nowFn().Truncate(w.window).Add(w.window)
}

func (w *WindowedFlush) shouldFlush() bool {
	t := w.nowFn()
	if t.Equal(w.curWindowEnd) || t.After(w.curWindowEnd) {
		w.curWindowEnd = w.newWindowEnd()
		return true
	}
	return false
}
