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
		rollingQueryCounts queryCountCircularBuffer
		// MovingQPS is used in ExpSmoothQPS(), where we calculate a smoothed QPS
		// value.
		MovingQPS []int64
	}
	// TODO(thardy98): add counter of number of not emitted events since the last
	// emitted event (#69653).
	smoothingAlpha  float64
	rollingInterval int
	Knobs           *TelemetryLoggingTestingKnobs
}

// TelemetryLoggingTestingKnobs provides hooks and knobs for unit tests.
type TelemetryLoggingTestingKnobs struct {
	// getRollingIntervalLength allows tests to override the rolling
	// interval cluster setting.
	getRollingIntervalLength func() int64

	// getTimeNow allows tests to override the timeutil.Now() function used
	// when updating rolling query counts.
	getTimeNow func() time.Time
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*TelemetryLoggingTestingKnobs) ModuleTestingKnobs() {}

// NewTelemetryLoggingMetrics returns a new TelemetryLoggingMetrics object.
func NewTelemetryLoggingMetrics(alpha float64, interval int64) *TelemetryLoggingMetrics {
	t := TelemetryLoggingMetrics{
		smoothingAlpha:  alpha,
		rollingInterval: int(interval),
	}
	t.mu.rollingQueryCounts = queryCountCircularBuffer{queryCounts: make([]queryCountAndTime, interval)}
	// MovingQPS calculates the QPS values between the query counts in
	// rollingQueryCounts. Consequently, MovingQPS can only have interval - 1
	// values.
	t.mu.MovingQPS = make([]int64, interval-1)
	return &t
}

func (t *TelemetryLoggingMetrics) timeNow() time.Time {
	if t.Knobs != nil && t.Knobs.getTimeNow != nil {
		return t.Knobs.getTimeNow()
	}
	return timeutil.Now()
}

func (t *TelemetryLoggingMetrics) getInterval() int {
	if t.Knobs != nil && t.Knobs.getRollingIntervalLength != nil {
		return int(t.Knobs.getRollingIntervalLength())
	}
	return t.rollingInterval
}

// updateRollingQueryCounts appends a new queryCountAndTime to the
// list of query counts in the telemetry logging metrics. Old
// queryCountAndTime values are removed from the slice once the size
// of the slice has exceeded the rollingInterval.
func (t *TelemetryLoggingMetrics) updateRollingQueryCounts() {
	t.mu.Lock()
	defer t.mu.Unlock()

	currentTime := t.timeNow()

	// Get the latest entry.
	// If the time since the latest entry was less than a second, bucket the
	// current query into the previous timestamp.
	if currentTime.Sub(t.mu.rollingQueryCounts.lastQueryCount().timestamp) < time.Second {
		t.mu.rollingQueryCounts.lastQueryCount().incrementCount()
		return
	}

	newLatest := queryCountAndTime{
		currentTime,
		1,
	}
	t.mu.rollingQueryCounts.insert(newLatest)
}

// queryCountCircularBuffer is a circular buffer of queryCountAndTime objects.
// As part of the TelemetryLoggingMetrics object, queryCountCircularBuffer
// should be accessed and written to via read/write locks.
type queryCountCircularBuffer struct {
	queryCounts []queryCountAndTime
	end         int
}

// insert a queryCountAndTime object into the circular buffer. If the
// buffer is full, the oldest value is overwritten.
// Write lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *queryCountCircularBuffer) insert(val queryCountAndTime) {
	// Increment the end pointer to the next index.
	// Update the value of the next index.
	b.end = b.nextIndex(b.end)
	b.queryCounts[b.end] = val
}

// getQueryCount returns the query count at the given index.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *queryCountCircularBuffer) getQueryCount(n int) *queryCountAndTime {
	return &b.queryCounts[n]
}

// lastQueryCount returns the latest query count.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *queryCountCircularBuffer) lastQueryCount() *queryCountAndTime {
	return &b.queryCounts[b.end]
}

// endPointer returns the index of the buffer's end pointer.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *queryCountCircularBuffer) endPointer() int {
	return b.end
}

// prevIndex returns the index previous to the given index 'n'.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *queryCountCircularBuffer) prevIndex(n int) int {
	// Do not need mutex access, length of queryCounts never changes.
	return (n + len(b.queryCounts) - 1) % len(b.queryCounts)
}

// nextIndex returns the index after the given index 'n'.
// Read lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (b *queryCountCircularBuffer) nextIndex(n int) int {
	// Do not need mutex access, length of queryCounts never changes.
	return (n + 1) % len(b.queryCounts)
}

// queryCountAndTime keeps a count of user initiated statements,
// and the timestamp at the latest count change.
// queryCountAndTime objects are used as part of the queryCountCircularBuffer,
// which in turn is used concurrently as part of the TelemetryLoggingMetrics
// object. As such, queryCountAndTime objects are to be accessed and written to
// via locks.
type queryCountAndTime struct {
	// No lock needs to be acquired to access this field as the timestamp of never
	// changes.
	timestamp time.Time
	// Read lock needs to be acquired to access this field from a
	// TelemetryLoggingMetrics object.
	count int64
}

// incrementCount increments a query count by 1.
// Write lock needs to be acquired to call this method from a
// TelemetryLoggingMetrics object.
func (q *queryCountAndTime) incrementCount() {
	q.count++
}
