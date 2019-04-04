// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
	// timeSource is the source of time used by the stop watch. It is always
	// timeutil.Now except for tests.
	timeSource func() time.Time
}

// NewStopWatch creates a new StopWatch.
func NewStopWatch() *StopWatch {
	return newStopWatch(Now)
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

// Stop stops the stop watch if it hasn't already been stopped and returns the
// duration that elapsed since it was started. If the stop watch has already
// been stopped, it returns zero duration.
func (w *StopWatch) Stop() time.Duration {
	if atomic.CompareAndSwapInt32(&w.started, 1, 0) {
		elapsed := w.timeSource().Sub(w.startedAt)
		return elapsed
	}
	return time.Duration(0)
}
