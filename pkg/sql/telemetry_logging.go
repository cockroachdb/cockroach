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

// Default value used to designate the maximum frequency at which events
// are logged to the telemetry channel.
const defaultMaxEventFrequency = 8

var telemetryMaxEventFrequency = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.telemetry.query_sampling.max_event_frequency",
	"the max event frequency at which we sample queries for telemetry, "+
		"note that this value shares a log-line limit of 10 logs per second on the "+
		"telemetry pipeline with all other telemetry events",
	defaultMaxEventFrequency,
	settings.NonNegativeInt,
)

// TelemetryLoggingMetrics keeps track of the last time at which an event
// was logged to the telemetry channel, and the number of skipped queries
// since the last logged event.
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
	// getTimeNow allows tests to override the timeutil.Now() function used
	// when updating rolling query counts.
	getTimeNow func() time.Time
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*TelemetryLoggingTestingKnobs) ModuleTestingKnobs() {}

func (t *TelemetryLoggingMetrics) timeNow() time.Time {
	if t.Knobs != nil && t.Knobs.getTimeNow != nil {
		return t.Knobs.getTimeNow()
	}
	return timeutil.Now()
}

// maybeUpdateLastEmittedTime updates the lastEmittedTime if the amount of time
// elapsed between lastEmittedTime and newTime is greater than requiredSecondsElapsed.
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
