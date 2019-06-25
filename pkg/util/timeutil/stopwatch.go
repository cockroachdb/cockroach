// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timeutil

import (
	"sync/atomic"
	"time"
)

// StopWatch is a utility stop watch that can be safely started and stopped
// multiple times and can be used concurrently.
type StopWatch struct {
	// started is used as boolean where 0 indicates false and 1 indicates true.
	// started is "true" if the stop watch has been started.
	started int32
	// startedAt is the time when the stop watch was started.
	startedAt time.Time
	// elapsed is the total time measured by the stop watch (i.e. between all
	// Starts and Stops).
	elapsed time.Duration
	// timeSource is the source of time used by the stop watch. It is always
	// timeutil.Now except for tests.
	timeSource func() time.Time
}

// NewStopWatch creates a new StopWatch.
func NewStopWatch() *StopWatch {
	return newStopWatch(Now)
}

// NewTestStopWatch create a new StopWatch with the given time source. It is
// used for testing only.
func NewTestStopWatch(timeSource func() time.Time) *StopWatch {
	return newStopWatch(timeSource)
}

func newStopWatch(timeSource func() time.Time) *StopWatch {
	return &StopWatch{timeSource: timeSource}
}

// Start starts the stop watch if it hasn't already been started.
func (w *StopWatch) Start() {
	if atomic.CompareAndSwapInt32(&w.started, 0, 1) {
		w.startedAt = w.timeSource()
	}
}

// Stop stops the stop watch if it hasn't already been stopped and accumulates
// the duration that elapsed since it was started. If the stop watch has
// already been stopped, it is a noop.
func (w *StopWatch) Stop() {
	if atomic.CompareAndSwapInt32(&w.started, 1, 0) {
		w.elapsed += w.timeSource().Sub(w.startedAt)
	}
}

// Elapsed returns the total time measured by the stop watch so far.
func (w *StopWatch) Elapsed() time.Duration {
	return w.elapsed
}

// TestTimeSource is a source of time that remembers when it was created (in
// terms of the real time) and returns the time based on its creation time and
// the number of "advances" it has had. It is used for testing only.
type TestTimeSource struct {
	initTime time.Time
	counter  int64
}

// NewTestTimeSource create a new TestTimeSource.
func NewTestTimeSource() *TestTimeSource {
	return &TestTimeSource{initTime: Now()}
}

// Now tells the current time according to t.
func (t *TestTimeSource) Now() time.Time {
	return t.initTime.Add(time.Duration(t.counter))
}

// Advance advances the current time according to t by 1 nanosecond.
func (t *TestTimeSource) Advance() {
	t.counter++
}

// Elapsed returns how much time has passed since t has been created. Note that
// it is equal to the number of advances in nanoseconds.
func (t *TestTimeSource) Elapsed() time.Duration {
	return time.Duration(t.counter)
}
