// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Default value used to designate a rate at which logs to the telemetry channel
// will be sampled.
const defaultTelemetrySampleRate = 0.1

var telemetrySampleRate = settings.RegisterFloatSetting(
	"sql.telemetry.query_sampling.sample_rate",
	"the rate/probability at which we sample queries for telemetry",
	defaultTelemetrySampleRate,
	settings.FloatBetweenZeroAndOneInclusive,
)

// TelemetryLoggingMetrics keeps track of a rolling interval of previous query
// counts with timestamps. The rolling interval of query counts + timestamps is
// used in combination with the smoothing alpha value in an exponential
// smoothing function to approximate cluster QPS.
type TelemetryLoggingMetrics struct {
	mu struct {
		syncutil.RWMutex
		// The timestamp of the last emitted telemetry event.
		lastEmittedTime time.Time
	}
	Knobs *TelemetryLoggingTestingKnobs

	// skippedQueryCount is used to produce the count of non-sampled queries.
	skippedQueryCount uint64
}

// TelemetryLoggingTestingKnobs provides hooks and knobs for unit tests.
type TelemetryLoggingTestingKnobs struct {
	// getRollingIntervalLength allows tests to override the rolling
	// interval cluster setting.
	getRollingIntervalLength func() int64

	// getTimeNow allows tests to override the timeutil.Now() function used
	// when updating rolling query counts.
	getTimeNow func() time.Time
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*TelemetryLoggingTestingKnobs) ModuleTestingKnobs() {}

// NewTelemetryLoggingMetrics returns a new TelemetryLoggingMetrics object.
func NewTelemetryLoggingMetrics() *TelemetryLoggingMetrics {
	t := TelemetryLoggingMetrics{}
	return &t
}

func (t *TelemetryLoggingMetrics) timeNow() time.Time {
	if t.Knobs != nil && t.Knobs.getTimeNow != nil {
		return t.Knobs.getTimeNow()
	}
	return timeutil.Now()
}

// maybeUpdateLastEmittedTime updates the lastEmittedTime if the amount of time
// elapsed between lastEmittedTime and newTime is greather than requiredSecondsElapsed.
func (t *TelemetryLoggingMetrics) maybeUpdateLastEmittedTime(
	newTime time.Time, requiredSecondsElapsed float64,
) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	lastEmittedTime := t.mu.lastEmittedTime

	if float64(newTime.Sub(lastEmittedTime))*1e-9 >= requiredSecondsElapsed {
		t.mu.lastEmittedTime = newTime
		return true
	}

	return false
}

func (t *TelemetryLoggingMetrics) resetSkippedQueryCount() (res uint64) {
	return atomic.SwapUint64(&t.skippedQueryCount, 0)
}

func (t *TelemetryLoggingMetrics) incSkippedQueryCount() {
	atomic.AddUint64(&t.skippedQueryCount, 1)
}
