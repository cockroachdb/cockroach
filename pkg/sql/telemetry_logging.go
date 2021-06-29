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
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TelemetryLoggingMetrics keeps track of a rolling interval of previous query
// counts with timestamps. The rolling interval of query counts + timestamps is
// used in combination with the smoothing alpha value in an exponential
// smoothing function to approximate cluster QPS.
type TelemetryLoggingMetrics struct {
	mu struct {
		syncutil.RWMutex
		rollingQueryCounts QueryCountCircularBuffer
		// MovingQPS is used in ExpSmoothQPS(), where we calculate a smoothed QPS
		// value.
		MovingQPS []int64
	}
	smoothingAlpha  float64
	rollingInterval int
	Knobs           *TelemetryLoggingTestingKnobs
}

// TelemetryLoggingTestingKnobs provides hooks and knobs for unit tests.
type TelemetryLoggingTestingKnobs struct {
	// GetRollingIntervalLength allows tests to override the rolling
	// interval cluster setting.
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
	t.mu.rollingQueryCounts = NewQueryCountCircularBuffer(int(interval))
	// MovingQPS calculates the QPS values between the query counts in
	// rollingQueryCounts. Consequently, MovingQPS can only have interval - 1
	// values.
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

// updateRollingQueryCounts appends a new QueryCountAndTime to the
// list of query counts in the telemetry logging metrics. Old
// QueryCountAndTime values are removed from the slice once the size
// of the slice has exceeded the rollingInterval.
func (t *TelemetryLoggingMetrics) updateRollingQueryCounts() {
	t.mu.Lock()
	defer t.mu.Unlock()

	currentTime := t.timeNow()

	// Get the latest entry.
	// If the time since the latest entry was less than a second, bucket the
	// current query into the previous timestamp.
	if currentTime.Sub(t.mu.rollingQueryCounts.lastQueryCount().Timestamp()) < time.Second {
		t.mu.rollingQueryCounts.lastQueryCount().IncrementCount()
		return
	}

	newLatest := NewQueryCountAndTime(currentTime.Unix(), 1)
	t.mu.rollingQueryCounts.Insert(newLatest)
}

// QueryCountCircularBuffer is a circular buffer of QueryCountAndTime objects.
// As part of the TelemetryLoggingMetrics object, QueryCountCircularBuffer
// should be accessed and written to via read/write locks.
type QueryCountCircularBuffer struct {
	queryCounts []QueryCountAndTime
	end         int
}

// NewQueryCountCircularBuffer creates a new QueryCountCircularBuffer object.
func NewQueryCountCircularBuffer(n int) QueryCountCircularBuffer {
	return QueryCountCircularBuffer{queryCounts: make([]QueryCountAndTime, n)}
}

// Insert inserts a QueryCountAndTime object into the circular buffer. If the
// buffer is full, the oldest value is overwritten.
// Write lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *QueryCountCircularBuffer) Insert(val QueryCountAndTime) {
	// Increment the end pointer to the next index.
	// Update the value of the next index.
	b.end = b.NextIndex(b.end)
	b.queryCounts[b.end] = val
}

// getQueryCount returns the query count at the given index.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *QueryCountCircularBuffer) getQueryCount(n int) *QueryCountAndTime {
	return &b.queryCounts[n]
}

// lastQueryCount returns the latest query count.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *QueryCountCircularBuffer) lastQueryCount() *QueryCountAndTime {
	return &b.queryCounts[b.end]
}

// endPointer returns the index of the buffer's end pointer.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *QueryCountCircularBuffer) endPointer() int {
	return b.end
}

// PrevIndex returns the index previous to the given index 'n'.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *QueryCountCircularBuffer) PrevIndex(n int) int {
	// Do not need mutex access, length of queryCounts never changes.
	return (n + len(b.queryCounts) - 1) % len(b.queryCounts)
}

// NextIndex returns the index after the given index 'n'.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *QueryCountCircularBuffer) NextIndex(n int) int {
	// Do not need mutex access, length of queryCounts never changes.
	return (n + 1) % len(b.queryCounts)
}

// QueryCountAndTime keeps a count of user initiated statements,
// and the timestamp at the latest count change.
// QueryCountAndTime objects are used as part of the QueryCountCircularBuffer,
// which in turn is used concurrently as part of the TelemetryLoggingMetrics
// object. As such, QueryCountAndTime objects are to be accessed and written to
// via locks.
type QueryCountAndTime struct {
	timestamp int64
	count     int64
}

// NewQueryCountAndTime returns a new QueryCountAndTime object.
func NewQueryCountAndTime(timestamp int64, count int64) QueryCountAndTime {
	qc := QueryCountAndTime{}
	qc.count = count
	qc.timestamp = timestamp
	return qc
}

// IncrementCount increments a query count by 1.
// Write lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (q *QueryCountAndTime) IncrementCount() {
	q.count++
}

// Count atomically returns the query count of a QueryCountAndTime object.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (q *QueryCountAndTime) Count() int64 {
	return q.count
}

// Timestamp atomically returns the timestamp of a QueryCountAndTime object.
// No lock needs to be acquired to call this method as the timestamp of a query
// count never changes.
func (q *QueryCountAndTime) Timestamp() time.Time {
	return timeutil.Unix(q.timestamp, 0)
}
