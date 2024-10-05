// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// StopWatch is a utility stop watch that can be safely started and stopped
// multiple times and can be used concurrently.
type StopWatch struct {
	mu struct {
		syncutil.Mutex
		// started is true if the stop watch has been started and haven't been
		// stopped after that.
		started bool
		// startedAt is the time when the stop watch was started.
		startedAt time.Time
		// elapsed is the total time measured by the stop watch (i.e. between
		// all Starts and Stops).
		elapsed time.Duration
		// timeSource is the source of time used by the stop watch. It is always
		// timeutil.Now except for tests.
		timeSource func() time.Time
		// cpuStopWatch is used to track CPU usage. It may be nil, in which case any
		// operations on it are no-ops.
		cpuStopWatch *cpuStopWatch
	}
}

// NewStopWatch creates a new StopWatch.
func NewStopWatch() *StopWatch {
	return newStopWatch(Now)
}

// NewStopWatchWithCPU creates a new StopWatch that will track CPU usage in
// addition to wall-clock time.
func NewStopWatchWithCPU() *StopWatch {
	w := newStopWatch(Now)
	if grunning.Supported() {
		w.mu.cpuStopWatch = &cpuStopWatch{}
	}
	return w
}

// NewTestStopWatch create a new StopWatch with the given time source. It is
// used for testing only.
func NewTestStopWatch(timeSource func() time.Time) *StopWatch {
	return newStopWatch(timeSource)
}

func newStopWatch(timeSource func() time.Time) *StopWatch {
	w := &StopWatch{}
	w.mu.timeSource = timeSource
	return w
}

// Start starts the stop watch if it hasn't already been started.
func (w *StopWatch) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.mu.started {
		w.mu.started = true
		w.mu.startedAt = w.mu.timeSource()
		w.mu.cpuStopWatch.start()
	}
}

// Stop stops the stop watch if it hasn't already been stopped and accumulates
// the duration that elapsed since it was started. If the stop watch has
// already been stopped, it is a noop.
func (w *StopWatch) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.mu.started {
		w.mu.started = false
		w.mu.elapsed += w.mu.timeSource().Sub(w.mu.startedAt)
		w.mu.cpuStopWatch.stop()
	}
}

// Elapsed returns the total time measured by the stop watch so far.
func (w *StopWatch) Elapsed() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.elapsed
}

// ElapsedCPU returns the total CPU time measured by the stop watch so far. It
// returns zero if cpuStopWatch is nil (which is the case if NewStopWatchWithCPU
// was not called or the platform does not support grunning).
func (w *StopWatch) ElapsedCPU() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.cpuStopWatch.elapsed()
}

// LastStartedAt returns the time the stopwatch was last started, and a bool
// indicating if the stopwatch is currently started.
func (w *StopWatch) LastStartedAt() (startedAt time.Time, started bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.startedAt, w.mu.started
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
