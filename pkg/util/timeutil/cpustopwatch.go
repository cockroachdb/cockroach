// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// CPUStopWatch is a wrapper around cpuStopWatch that is safe to use
// concurrently. If CPUStopWatch is nil, all operations are no-ops and no
// locks are acquired.
type CPUStopWatch struct {
	mu struct {
		syncutil.Mutex
		cpuStopWatch cpuStopWatch
	}
}

// NewCPUStopWatch returns a new CPUStopWatch if the grunning library is
// supported. Otherwise, it returns nil.
func NewCPUStopWatch() *CPUStopWatch {
	if grunning.Supported() {
		return &CPUStopWatch{}
	}
	return nil
}

// Start starts the CPU stop watch if it hasn't already been started.
func (w *CPUStopWatch) Start() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mu.cpuStopWatch.start()
}

// Stop stops the CPU stop watch if it hasn't already been stopped and
// accumulates the CPU time that has been spent since it was started. If the
// CPU stop watch has already been stopped, it is a noop.
func (w *CPUStopWatch) Stop() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mu.cpuStopWatch.stop()
}

// Elapsed returns the total CPU time measured by the stop watch so far.
func (w *CPUStopWatch) Elapsed() time.Duration {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.cpuStopWatch.elapsed()
}

// cpuStopWatch is a utility stop watch for measuring CPU time spent by a
// component. It can be safely started and stopped multiple times, but is
// not safe to use concurrently. If cpuStopWatch is nil, all operations are
// no-ops.
//
// Note that the grunning library uses a non-monotonic clock, so the measured
// duration between clock start and stop can come out as negative. This can lead
// to discrepancies in the measured CPU time - for example, a child stopwatch
// that is started and stopped while a parent stopwatch is running can rarely
// measure a larger CPU time duration than the parent. Users must be prepared to
// handle this case.
type cpuStopWatch struct {
	startCPU time.Duration
	totalCPU time.Duration
}

func (w *cpuStopWatch) start() {
	if w == nil {
		return
	}
	w.startCPU = grunning.Time()
}

func (w *cpuStopWatch) stop() {
	if w == nil {
		return
	}
	w.totalCPU += grunning.Elapsed(w.startCPU, grunning.Time())
}

func (w *cpuStopWatch) elapsed() time.Duration {
	if w == nil {
		return 0
	}
	return w.totalCPU
}
