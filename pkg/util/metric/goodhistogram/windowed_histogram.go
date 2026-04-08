// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goodhistogram

import (
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// WindowedHistogram combines a cumulative histogram (for Prometheus export)
// with a windowed pair (prev/cur) for percentile computation over a recent
// time window.
//
// The caller rotates the window by calling Rotate() at a fixed interval
// (e.g. every 5s). Each rotation moves the current window to prev and
// installs a fresh histogram as cur. WindowedSnapshot() returns the merge
// of prev and cur, covering approximately the last two rotation intervals.
//
// This matches the windowing pattern used by CockroachDB's other histogram
// implementations (Histogram, ManualWindowHistogram, GoodHistogramWrapper).
//
// Recording is lock-free: each Record() atomically increments counters in
// both the cumulative and current-window histograms. The lock is only held
// during Rotate() and WindowedSnapshot().
type WindowedHistogram struct {
	config Config
	cum    *Histogram // cumulative, never reset

	mu struct {
		syncutil.Mutex
		prev unsafe.Pointer // *Histogram, previous window (may be nil)
		cur  unsafe.Pointer // *Histogram, current window
	}
}

// NewWindowed creates a WindowedHistogram for the given range and error bound.
func NewWindowed(lo, hi, desiredError float64) *WindowedHistogram {
	config := NewConfig(lo, hi, desiredError)
	wh := &WindowedHistogram{
		config: config,
		cum:    newHistogram(&config),
	}
	wh.mu.cur = unsafe.Pointer(newHistogram(&config))
	return wh
}

// Record adds a value to both the cumulative and current-window histograms.
func (wh *WindowedHistogram) Record(v int64) {
	wh.cum.Record(v)
	cur := (*Histogram)(atomic.LoadPointer(&wh.mu.cur))
	cur.Record(v)
}

// Rotate moves the current window to prev and installs a fresh histogram
// as cur. This should be called at a regular interval (e.g. every 5s)
// by a tick.Ticker.
func (wh *WindowedHistogram) Rotate() {
	wh.mu.Lock()
	defer wh.mu.Unlock()
	wh.mu.prev = wh.mu.cur
	fresh := newHistogram(&wh.config)
	atomic.StorePointer(&wh.mu.cur, unsafe.Pointer(fresh))
}

// CumulativeSnapshot returns a point-in-time snapshot of the cumulative
// (all-time) histogram data. Suitable for Prometheus export.
func (wh *WindowedHistogram) CumulativeSnapshot() Snapshot {
	return wh.cum.Snapshot()
}

// WindowedSnapshot returns a merged snapshot of the prev and cur windows,
// covering approximately the last two rotation intervals. If no rotation
// has occurred yet, it returns a snapshot of cur only.
func (wh *WindowedHistogram) WindowedSnapshot() Snapshot {
	wh.mu.Lock()
	defer wh.mu.Unlock()
	cur := (*Histogram)(wh.mu.cur)
	if cur == nil {
		return Snapshot{}
	}
	curSnap := cur.Snapshot()

	prev := (*Histogram)(wh.mu.prev)
	if prev != nil {
		prevSnap := prev.Snapshot()
		merged := curSnap.Merge(&prevSnap)
		return merged
	}
	return curSnap
}

// Config returns the histogram's configuration.
func (wh *WindowedHistogram) Config() Config {
	return wh.config
}
