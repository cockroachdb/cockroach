// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schedulerlatency

import "time"

type LatencyObserver interface {
	// SchedulerLatency is provided the current value of the scheduler's p99 latency and the
	// period over which the measurement applies.
	SchedulerLatency(p99 time.Duration, period time.Duration)
}
