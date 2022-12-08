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

// CPUStopWatch is a utility stop watch for measuring CPU time spent by a
// component. It can be safely started and stopped multiple times, but is
// not safe to use concurrently. If CPUStopWatch is nil, all operations are
// no-ops.
type CPUStopWatch struct {
	startedAt time.Duration
	elapsed   time.Duration
}

func (w *CPUStopWatch) Start() {
	if w == nil {
		return
	}
	w.startedAt = grunning.Time()
}

func (w *CPUStopWatch) Stop() {
	if w == nil {
		return
	}
	w.elapsed += grunning.Difference(w.startedAt, grunning.Time())
}

func (w *CPUStopWatch) Elapsed() time.Duration {
	if w == nil {
		return 0
	}
	return w.elapsed
}
