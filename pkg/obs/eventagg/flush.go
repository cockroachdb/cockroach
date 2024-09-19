// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eventagg

import "time"

// FlushKind is the type of FlushTrigger used, which communicates aggregation strategy.
type FlushKind string

// Windowed is a FlushTrigger type that enforces windowed flush periods, containing a
// window start and end time.
const Windowed FlushKind = "WINDOWED"

// AggInfo is a type used to communicate metadata from a FlushTrigger to the
// targets of the flush about the aggregation type period. For example, the type
// of flush, and associated timestamps.
type AggInfo struct {
	// Kind is the FlushKind used in the FlushTrigger.
	Kind FlushKind `json:"kind"`
	// StartTime is the primary timestamp for the flushed data.
	StartTime int64 `json:"start_time"`
	// EndTime, if present, represents the end timestamp of the flush interval.
	EndTime int64 `json:"end_time"`
}

// FlushTrigger defines the interface used by aggregators, such as MapReduceAggregator,
// to determine when a flush of the current aggregation should be triggered.
//
// FlushTriggers are generally invoked as soon as events are consumed by an aggregator,
// but *before* the event itself is aggregated.
type FlushTrigger interface {
	// shouldFlush returns true if this FlushTrigger has been tripped.
	shouldFlush() (bool, AggInfo)
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

func (w *WindowedFlush) shouldFlush() (bool, AggInfo) {
	t := w.nowFn()
	if t.Equal(w.curWindowEnd) || t.After(w.curWindowEnd) {
		meta := AggInfo{
			Kind:      Windowed,
			StartTime: w.curWindowEnd.Add(-w.window).UnixNano(),
			EndTime:   w.curWindowEnd.UnixNano(),
		}
		w.curWindowEnd = w.newWindowEnd()
		return true, meta
	}
	return false, AggInfo{}
}
