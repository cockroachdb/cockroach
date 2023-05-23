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
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// Default value used to designate the maximum frequency at which events
// are logged to the telemetry channel.
const defaultMaxEventFrequency = 8

var TelemetryMaxEventFrequency = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.telemetry.query_sampling.max_event_frequency",
	"the max event frequency at which we sample queries for telemetry, "+
		"note that this value shares a log-line limit of 10 logs per second on the "+
		"telemetry pipeline with all other telemetry events",
	defaultMaxEventFrequency,
	settings.PositiveInt,
)

var telemetryEventFrequencyStmtFingerprint = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.telemetry.query_sampling.stmt_fingerprint.max_event_frequency",
	"the max event frequency at which we sample each stmt fingerprint for telemetry",
	5,
	settings.PositiveInt,
)

var telemetrySamplingPerFingerprintEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.telemetry.query_sampling.stmt_fingerprint.enabled",
	"if on, we apply an additional max event frequency for each statement fingerprint id "+
		"in addition to the one set by sql.telemetry.query_sampling.max_event_frequency",
	false,
)

type telemetryEvent struct {
	// The timestamp of the last emitted telemetry event.
	lastEmittedTime time.Time
}

// TelemetryLoggingMetrics keeps track of the last time at which an event
// was logged to the telemetry channel, and the number of skipped queries
// since the last logged event.
type TelemetryLoggingMetrics struct {
	mu struct {
		syncutil.RWMutex
		// The timestamp of the last emitted telemetry event.
		lastEmittedTime time.Time

		// stmtFingerprintLoggingRecord is used to track when a stmt fingerprint
		// id was last emitted to telemetry. Note this field is only updated when
		// sql.telemetry.query_sampling.stmt_fingerprint.enabled is true.
		stmtFingerprintLoggingRecord *cache.OrderedCache
	}
	Knobs *TelemetryLoggingTestingKnobs

	// skippedQueryCount is used to produce the count of non-sampled queries.
	skippedQueryCount uint64
}

func newTelemetryLoggingmetrics(knobs *TelemetryLoggingTestingKnobs) *TelemetryLoggingMetrics {
	t := TelemetryLoggingMetrics{Knobs: knobs}
	t.mu.stmtFingerprintLoggingRecord = cache.NewOrderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > 4096
		},
	})

	return &t
}

// TelemetryLoggingTestingKnobs provides hooks and knobs for unit tests.
type TelemetryLoggingTestingKnobs struct {
	// getTimeNow allows tests to override the timeutil.Now() function used
	// when updating rolling query counts.
	getTimeNow func() time.Time
	// getQueryLevelMetrics allows tests to override the recorded query level stats.
	getQueryLevelStats func() execstats.QueryLevelStats
	// getTracingStatus allows tests to override whether the current query has tracing
	// enabled or not. Queries with tracing enabled are always sampled to telemetry.
	getTracingStatus func() bool
}

func NewTelemetryLoggingTestingKnobs(
	getTimeNowFunc func() time.Time,
	getQueryLevelStatsFunc func() execstats.QueryLevelStats,
	getTracingStatusFunc func() bool,
) *TelemetryLoggingTestingKnobs {
	return &TelemetryLoggingTestingKnobs{
		getTimeNow:         getTimeNowFunc,
		getQueryLevelStats: getQueryLevelStatsFunc,
		getTracingStatus:   getTracingStatusFunc,
	}
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
// It also updates the corresponding entry in stmtFingerprintLoggingRecord for the
// provided fingerprint id if specified to do so.
func (t *TelemetryLoggingMetrics) maybeUpdateLastEmittedTime(
	newTime time.Time,
	requiredSecondsElapsed float64,
	shouldUpdateFingerprintEmittedTime bool,
	stmtFingerprintID appstatspb.StmtFingerprintID,
	requiredSecsElapsedPerFingerprint float64,
) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	allStmtsRequiredTimeElapsed := float64(newTime.Sub(t.mu.lastEmittedTime))*1e-9 >= requiredSecondsElapsed
	stmtFingerprintRequiredTimeElapsed := true

	// Check if we are applying a sampling rate per stmt fingerprint id.
	var stmtFingerprintEntry *telemetryEvent
	if shouldUpdateFingerprintEmittedTime {
		entry, ok := t.mu.stmtFingerprintLoggingRecord.Get(stmtFingerprintID)

		if ok {
			stmtFingerprintEntry = entry.(*telemetryEvent)
			// Check if enough time has elapsed for this fingerprint ID.
			stmtFingerprintRequiredTimeElapsed =
				float64(newTime.Sub(stmtFingerprintEntry.lastEmittedTime))*1e-9 >= requiredSecsElapsedPerFingerprint
		} else {
			// Create new entry and add it to the cache.
			stmtFingerprintEntry = &telemetryEvent{}
			t.mu.stmtFingerprintLoggingRecord.Add(stmtFingerprintID, stmtFingerprintEntry)
		}
	}

	if !allStmtsRequiredTimeElapsed || !stmtFingerprintRequiredTimeElapsed {
		// Not enough time has elapsed to emit this event.
		return false
	}

	if stmtFingerprintEntry != nil {
		stmtFingerprintEntry.lastEmittedTime = newTime
	}

	t.mu.lastEmittedTime = newTime
	return true
}

func (t *TelemetryLoggingMetrics) getQueryLevelStats(
	queryLevelStats execstats.QueryLevelStats,
) execstats.QueryLevelStats {
	if t.Knobs != nil && t.Knobs.getQueryLevelStats != nil {
		return t.Knobs.getQueryLevelStats()
	}
	return queryLevelStats
}

func (t *TelemetryLoggingMetrics) isTracing(_ *tracing.Span, tracingEnabled bool) bool {
	if t.Knobs != nil && t.Knobs.getTracingStatus != nil {
		return t.Knobs.getTracingStatus()
	}
	return tracingEnabled
}

func (t *TelemetryLoggingMetrics) resetSkippedQueryCount() (res uint64) {
	return atomic.SwapUint64(&t.skippedQueryCount, 0)
}

func (t *TelemetryLoggingMetrics) incSkippedQueryCount() {
	atomic.AddUint64(&t.skippedQueryCount, 1)
}
