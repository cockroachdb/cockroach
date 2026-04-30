// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/grunning"
)

// CPUStopWatch measures CPU time on the current goroutine between Start and
// Stop calls. It is designed to be held as a value, not a pointer. The zero
// value is valid (Stop returns 0). All methods check grunning.Supported
// internally, so the caller need not guard.
type CPUStopWatch struct {
	start time.Duration
}

// Start begins a CPU measurement interval. No-op if grunning is not supported
// on this platform.
func (w *CPUStopWatch) Start() {
	if grunning.Supported {
		w.start = grunning.Time()
	}
}

// Stop ends the current measurement interval and returns the elapsed CPU time.
// Returns 0 if grunning is not supported or Start was not called.
func (w *CPUStopWatch) Stop() time.Duration {
	if !grunning.Supported || w.start == 0 {
		return 0
	}
	elapsed := grunning.Elapsed(w.start, grunning.Time())
	w.start = 0
	return elapsed
}
