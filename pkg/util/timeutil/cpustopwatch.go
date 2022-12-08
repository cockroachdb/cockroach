// Copyright 2022 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/grunning"
)

// cpuStopWatch is a utility stop watch for measuring CPU time spent by a
// component. It can be safely started and stopped multiple times, but is
// not safe to use concurrently. If cpuStopWatch is nil, all operations are
// no-ops.
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
	w.totalCPU += grunning.Difference(w.startCPU, grunning.Time())
}

func (w *cpuStopWatch) elapsed() time.Duration {
	if w == nil {
		return 0
	}
	return w.totalCPU
}
