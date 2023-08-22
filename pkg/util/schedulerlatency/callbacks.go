// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schedulerlatency

import "time"

type LatencyObserver interface {
	// SchedulerLatency is provided the current value of the scheduler's p99 latency and the
	// period over which the measurement applies.
	SchedulerLatency(p99 time.Duration, period time.Duration)
}
