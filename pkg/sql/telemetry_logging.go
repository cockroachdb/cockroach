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
	smoothingAlpha     float64
	rollingInterval    int
	RollingQueryCounts *QueryCountCircularBuffer
	// MovingQPS is used in ExpSmoothQPS(), where we calculate a smoothed QPS value.
	MovingQPS atomic.Value
	Knobs     *TelemetryLoggingTestingKnobs
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
		smoothingAlpha:     alpha,
		rollingInterval:    int(interval),
		RollingQueryCounts: NewQueryCountCircularBuffer(int(interval)),
	}
	// MovingQPS calculates the QPS values between the query counts in RollingQueryCounts.
	// Consequently, MovingQPS can only have interval - 1 values.
	t.MovingQPS.Store(make([]int64, interval-1))
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
	currentTime := t.timeNow()

	// Get the latest entry.
	// If the time since the latest entry was less than a second, bucket the current query
	// into the previous timestamp.
	if currentTime.Sub(t.LastQueryCount().Timestamp()) < time.Second {
		t.LastQueryCount().IncrementCount()
		return
	}

	// Insert a new query count entry at the current timestamp.
	// Insertion occurs:
	// 		- when query counts interval is empty (i.e. the first insertion).
	//		- when the time since the last entry exceeds 1 second.
	newLatest := QueryCountAndTime{currentTime.Unix(), 1}
	t.RollingQueryCounts.Insert(newLatest)
}

// InsertQueryCount is a TelemetryLoggingMetrics wrapper around inserting
// into the RollingQueryCounts.
func (t *TelemetryLoggingMetrics) InsertQueryCount(qc QueryCountAndTime) {
	t.RollingQueryCounts.Insert(qc)
}

// GetQueryCount is a TelemetryLoggingMetrics wrapper around getting a query
// count from RollingQueryCounts.
func (t *TelemetryLoggingMetrics) GetQueryCount(n int) *QueryCountAndTime {
	return t.RollingQueryCounts.GetQueryCount(n)
}

// LastQueryCount is a TelemetryLoggingMetrics wrapper around getting the
// latest entry from RollingQueryCounts.
func (t *TelemetryLoggingMetrics) LastQueryCount() *QueryCountAndTime {
	return t.RollingQueryCounts.LastQueryCount()
}

// GetMovingQPS returns the QPS value at the given index.
func (t *TelemetryLoggingMetrics) GetMovingQPS(idx int) int64 {
	movingQPS := t.MovingQPS.Load().([]int64)
	return movingQPS[idx]
}

// InsertMovingQPS inserts the given QPS value into MovingQPS.
func (t *TelemetryLoggingMetrics) InsertMovingQPS(qpsVal int64) {
	movingQPS := t.MovingQPS.Load().([]int64)
	movingQPS = append(movingQPS, qpsVal)
	t.MovingQPS.Store(movingQPS)
}

// ResetMovingQPS slices the header of the MovingQPS to empty it.
func (t *TelemetryLoggingMetrics) ResetMovingQPS() {
	movingQPS := t.MovingQPS.Load().([]int64)
	movingQPS = movingQPS[:0]
	t.MovingQPS.Store(movingQPS)
}

// MovingQPSLength returns the length of MovingQPS.
func (t *TelemetryLoggingMetrics) MovingQPSLength() int {
	movingQPS := t.MovingQPS.Load().([]int64)
	return len(movingQPS)
}

// QueryCountCircularBuffer is a circular buffer of QueryCountAndTime objects.
type QueryCountCircularBuffer struct {
	mu struct {
		syncutil.RWMutex
		QueryCounts []QueryCountAndTime
		End         int
	}
}

// NewQueryCountCircularBuffer creates a new QueryCountCircularBuffer object.
func NewQueryCountCircularBuffer(n int) *QueryCountCircularBuffer {
	buff := QueryCountCircularBuffer{}
	buff.mu.QueryCounts = make([]QueryCountAndTime, n)
	return &buff
}

// GetQueryCounts returns QueryCounts from the buffer.
func (b *QueryCountCircularBuffer) GetQueryCounts() []QueryCountAndTime {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.mu.QueryCounts
}

// Insert inserts a QueryCountAndTime object into the circular buffer. If the buffer
// is full, the oldest value is overwritten.
func (b *QueryCountCircularBuffer) Insert(val QueryCountAndTime) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Increment the end pointer to the next index.
	// Update the value of the next index.
	b.mu.End = b.NextIndex(b.mu.End)
	b.mu.QueryCounts[b.mu.End] = val
}

// GetQueryCount returns the query count at the given index.
func (b *QueryCountCircularBuffer) GetQueryCount(n int) *QueryCountAndTime {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return &b.mu.QueryCounts[n]
}

// LastQueryCount returns the latest query count.
func (b *QueryCountCircularBuffer) LastQueryCount() *QueryCountAndTime {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return &b.mu.QueryCounts[b.mu.End]
}

// EndPointer returns the index of the buffer's end pointer.
func (b *QueryCountCircularBuffer) EndPointer() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.mu.End
}

// PrevIndex returns the index previous to the given index 'n'.
func (b *QueryCountCircularBuffer) PrevIndex(n int) int {
	// Do not need mutex access, length of QueryCounts never changes.
	return (n + len(b.mu.QueryCounts) - 1) % len(b.mu.QueryCounts)
}

// NextIndex returns the index after the given index 'n'.
func (b *QueryCountCircularBuffer) NextIndex(n int) int {
	// Do not need mutex access, length of QueryCounts never changes.
	return (n + 1) % len(b.mu.QueryCounts)
}

// QueryCountAndTime keeps a count of user initiated statements,
// and the timestamp at the latest count change.
type QueryCountAndTime struct {
	timestamp int64
	count     int64
}

// NewQueryCountAndTime returns a new QueryCountAndTime object.
func NewQueryCountAndTime(timestamp int64, count int64) QueryCountAndTime {
	return QueryCountAndTime{
		timestamp,
		count,
	}
}

// IncrementCount increments a query count by 1.
func (q *QueryCountAndTime) IncrementCount() {
	atomic.AddInt64(&q.count, 1)
}

// Count atomically returns the query count of a QueryCountAndTime object
func (q *QueryCountAndTime) Count() int64 {
	return atomic.LoadInt64(&q.count)
}

// Timestamp atomically returns the timestamp of a QueryCountAndTime object
func (q *QueryCountAndTime) Timestamp() time.Time {
	return timeutil.Unix(atomic.LoadInt64(&q.timestamp), 0)
}
