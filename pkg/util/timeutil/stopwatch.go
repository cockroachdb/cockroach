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
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// StopWatch is a utility stop watch that can be safely started and stopped
// multiple times and can be used concurrently.
type StopWatch struct {
	mu struct {
		syncutil.Mutex
		startedAt *time.Time
	}
}

// NewStopWatch create a new StopWatch.
func NewStopWatch() *StopWatch {
	return &StopWatch{}
}

// Start starts the stop watch if it hasn't already been started.
func (w *StopWatch) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.mu.startedAt == nil {
		startedAt := Now()
		w.mu.startedAt = &startedAt
	}
}

// Stop stops the stop watch if it hasn't already been stopped and returns the
// duration that elapsed since it was started. If the stop watch has already
// been stopped, it returns zero duration.
func (w *StopWatch) Stop() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.mu.startedAt != nil {
		elapsed := Since(*w.mu.startedAt)
		w.mu.startedAt = nil
		return elapsed
	}
	return time.Duration(0)
}
