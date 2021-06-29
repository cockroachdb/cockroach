// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TelemetryLoggingMetrics keeps track of a rolling interval of previous query counts
// with timestamps. The rolling interval of query counts + timestamps is used in combination
// with the smoothing alpha value in an exponential smoothing function to approximate cluster
// QPS.
type TelemetryLoggingMetrics struct {
	mu struct {
		syncutil.RWMutex
		RollingQueryCounts QueryCountCircularBuffer
		// MovingQPS is used in ExpSmoothQPS(), where we calculate a smoothed QPS value.
		MovingQPS []int64
	}
	smoothingAlpha  float64
	rollingInterval int
	Knobs           *TelemetryLoggingTestingKnobs
}

// TelemetryLoggingTestingKnobs provides hooks and knobs for unit tests.
type TelemetryLoggingTestingKnobs struct {
	// GetRollingIntervalLength allows tests to override a cluster's rolling interval
	// cluster setting.
	GetRollingIntervalLength func() int64

	// GetTimeNow allows tests to override the timeutil.Now() function used
	// when updating rolling query counts.
	GetTimeNow func() time.Time
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*TelemetryLoggingTestingKnobs) ModuleTestingKnobs() {}

// NewTelemetryLoggingMetrics returns a new TelemetryLoggingMetrics object.
func NewTelemetryLoggingMetrics(alpha float64, interval int64) *TelemetryLoggingMetrics {
	t := TelemetryLoggingMetrics{
		smoothingAlpha:  alpha,
		rollingInterval: int(interval),
	}
	t.mu.RollingQueryCounts = NewQueryCountCircularBuffer(int(interval))
	// MovingQPS calculates the QPS values between the query counts in RollingQueryCounts.
	// Consequently, MovingQPS can only have interval - 1 values.
	t.mu.MovingQPS = make([]int64, interval-1)
	return &t
}

func (t *TelemetryLoggingMetrics) timeNow() time.Time {
	if t.Knobs != nil && t.Knobs.GetTimeNow != nil {
		return t.Knobs.GetTimeNow()
	}
	return timeutil.Now()
}

func (t *TelemetryLoggingMetrics) getInterval() int {
	if t.Knobs != nil && t.Knobs.GetRollingIntervalLength != nil {
		return int(t.Knobs.GetRollingIntervalLength())
	}
	return t.rollingInterval
}

// UpdateRollingQueryCounts appends a new QueryCountAndTime to the
// list of query counts in the telemetry logging metrics. Old
// QueryCountAndTime values are removed from the slice once the size
// of the slice has exceeded the rollingInterval.
func (t *TelemetryLoggingMetrics) UpdateRollingQueryCounts() {
	t.mu.RLock()

	currentTime := t.timeNow()

	// Get the latest entry.
	// If the time since the latest entry was less than a second, bucket the current query
	// into the previous timestamp.
	if currentTime.Sub(t.mu.RollingQueryCounts.LastQueryCount().Timestamp()) < time.Second {
		t.mu.RollingQueryCounts.LastQueryCount().IncrementCount()
		t.mu.RUnlock()
		return
	}

	// Insert a new query count entry at the current timestamp.
	// Insertion occurs:
	// 		- when query counts interval is empty (i.e. the first insertion).
	//		- when the time since the last entry exceeds 1 second.
	newLatest := NewQueryCountAndTime(currentTime.Unix(), 1)
	t.mu.RUnlock()
	t.InsertQueryCount(newLatest)
}

// InsertQueryCount is a TelemetryLoggingMetrics wrapper around inserting
// into the RollingQueryCounts.
func (t *TelemetryLoggingMetrics) InsertQueryCount(qc QueryCountAndTime) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.RollingQueryCounts.Insert(qc)
}

// InsertMovingQPS inserts the given QPS value into MovingQPS.
func (t *TelemetryLoggingMetrics) InsertMovingQPS(qpsVal int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.MovingQPS = append(t.mu.MovingQPS, qpsVal)
}

// ResetMovingQPS slices the header of the MovingQPS to empty it.
func (t *TelemetryLoggingMetrics) ResetMovingQPS() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.MovingQPS = t.mu.MovingQPS[:0]
}

// QueryCountCircularBuffer is a circular buffer of QueryCountAndTime objects.
// As part of the TelemetryLoggingMetrics object, QueryCountCircularBuffer
// should be accessed and written to via read/write locks.
type QueryCountCircularBuffer struct {
	QueryCounts []QueryCountAndTime
	End         int
}

// NewQueryCountCircularBuffer creates a new QueryCountCircularBuffer object.
func NewQueryCountCircularBuffer(n int) QueryCountCircularBuffer {
	return QueryCountCircularBuffer{QueryCounts: make([]QueryCountAndTime, n)}
}

// Insert inserts a QueryCountAndTime object into the circular buffer. If the buffer
// is full, the oldest value is overwritten.
// Write lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *QueryCountCircularBuffer) Insert(val QueryCountAndTime) {
	// Increment the end pointer to the next index.
	// Update the value of the next index.
	b.End = b.NextIndex(b.End)
	b.QueryCounts[b.End] = val
}

// GetQueryCount returns the query count at the given index.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *QueryCountCircularBuffer) GetQueryCount(n int) *QueryCountAndTime {
	return &b.QueryCounts[n]
}

// LastQueryCount returns the latest query count.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *QueryCountCircularBuffer) LastQueryCount() *QueryCountAndTime {
	return &b.QueryCounts[b.End]
}

// EndPointer returns the index of the buffer's end pointer.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *QueryCountCircularBuffer) EndPointer() int {
	return b.End
}

// PrevIndex returns the index previous to the given index 'n'.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *QueryCountCircularBuffer) PrevIndex(n int) int {
	// Do not need mutex access, length of QueryCounts never changes.
	return (n + len(b.QueryCounts) - 1) % len(b.QueryCounts)
}

// NextIndex returns the index after the given index 'n'.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *QueryCountCircularBuffer) NextIndex(n int) int {
	// Do not need mutex access, length of QueryCounts never changes.
	return (n + 1) % len(b.QueryCounts)
}

// QueryCountAndTime keeps a count of user initiated statements,
// and the timestamp at the latest count change.
// QueryCountAndTime fields are accessed with atomics.
type QueryCountAndTime struct {
	atomics struct {
		timestamp int64
		count     int64
	}
}

// NewQueryCountAndTime returns a new QueryCountAndTime object.
func NewQueryCountAndTime(timestamp int64, count int64) QueryCountAndTime {
	qc := QueryCountAndTime{}
	qc.atomics.count = count
	qc.atomics.timestamp = timestamp
	return qc
}

// IncrementCount increments a query count by 1.
func (q *QueryCountAndTime) IncrementCount() {
	atomic.AddInt64(&q.atomics.count, 1)
}

// Count atomically returns the query count of a QueryCountAndTime object.
func (q *QueryCountAndTime) Count() int64 {
	return atomic.LoadInt64(&q.atomics.count)
}

// Timestamp atomically returns the timestamp of a QueryCountAndTime object.
func (q *QueryCountAndTime) Timestamp() time.Time {
	return timeutil.Unix(atomic.LoadInt64(&q.atomics.timestamp), 0)
}
