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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// Default value used to designate the maximum frequency at which events
// are logged to the telemetry channel.
const defaultMaxEventFrequency = 8

var TelemetryMaxStatementEventFrequency = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.telemetry.query_sampling.max_event_frequency",
	"the max event frequency at which we sample executions for telemetry, "+
		"note that it is recommended that this value shares a log-line limit of 10 "+
		" logs per second on the telemetry pipeline with all other telemetry events. "+
		"If sampling mode is set to 'transaction', this value is ignored. ",
	defaultMaxEventFrequency,
	settings.NonNegativeInt,
	settings.WithPublic,
)

var telemetryTransactionSamplingFrequency = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.telemetry.transaction_sampling.max_event_frequency",
	"the max event frequency at which we sample transactions for telemetry, "+
		"If sampling mode is set to 'statement', this setting is ignored.",
	defaultMaxEventFrequency,
	settings.NonNegativeInt,
	settings.WithPublic,
)

var telemetryQueriesPerTransactionMax = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.telemetry.transaction_sampling.statement_events_per_transaction.max",
	"the maximum number of statement events to log for every sampled transaction."+
		"Note that statements that are logged by force do not adhere to this limit.",
	50,
	settings.NonNegativeInt,
	settings.WithPublic,
)

var telemetryInternalQueriesEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.telemetry.query_sampling.internal.enabled",
	"when set to true, internal queries will be sampled in telemetry logging",
	false,
	settings.WithPublic)

var telemetryInternalConsoleQueriesEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.telemetry.query_sampling.internal_console.enabled",
	"when set to true, all internal queries used to populated the UI Console"+
		"will be logged into telemetry",
	true,
)

const (
	telemetryModeStatement = iota
	telemetryModeTransaction
)

var telemetrySamplingMode = settings.RegisterEnumSetting(
	settings.ApplicationLevel,
	"sql.telemetry.query_sampling.mode",
	"the execution level used for telemetry sampling. If set to 'statement', events "+
		"are sampled at the statement execution level. If set to 'transaction', events are "+
		"sampled at the transaction execution level, i.e. all statements for a transaction "+
		"will be logged and are counted together as one sampled event (events are still emitted one "+
		"per statement). In transaction mode a dedicated transaction event will also be emitted for "+
		"every sampled transaction.",
	"statement",
	map[int64]string{
		telemetryModeStatement:   "statement",
		telemetryModeTransaction: "transaction",
	},
	settings.WithPublic,
)

// TelemetryLoggingMetrics keeps track of the last time at which an event
// was sampled to the telemetry channel, and the number of skipped events
// since the last sampled event.
//
// There are two modes for telemetry logging, set via the setting telemetrySamplingMode:
//
//  1. Statement mode: Events are sampled at the statement level. In this mode,
//     the sampling frequency for SampledQuery events is defined by the setting
//     TelemetryMaxStatementEventFrequency. No transaction execution events are
//     emitted in this mode.
//
//  2. Transaction mode: Events are sampled at the transaction level. In this mode,
//     the sampling frequency for SampledQuery events is defined by the setting
//     telemetryTransactionSamplingFrequency. In this mode, all of a transaction's
//     statement execution events are logged up to a maximum set by
//     telemetryQueriesPerTransactionMax.
type TelemetryLoggingMetrics struct {
	st *cluster.Settings

	mu struct {
		syncutil.RWMutex
		// The last time at which an event was sampled to the telemetry channel.
		lastEmittedTime time.Time
	}

	Knobs *TelemetryLoggingTestingKnobs

	// skippedQueryCount is used to produce the count of non-sampled queries.
	skippedQueryCount atomic.Uint64
}

func newTelemetryLoggingMetrics(
	knobs *TelemetryLoggingTestingKnobs, st *cluster.Settings,
) *TelemetryLoggingMetrics {
	t := TelemetryLoggingMetrics{Knobs: knobs, st: st}
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

func (t *TelemetryLoggingMetrics) shouldTrackTransaction(isInternal bool) bool {
	isTxnMode := telemetrySamplingMode.Get(&t.st.SV) == telemetryModeTransaction
	if !telemetryLoggingEnabled.Get(&t.st.SV) || !isTxnMode ||
		(isInternal && !telemetryInternalQueriesEnabled.Get(&t.st.SV)) {
		return false
	}

	txnStart := t.timeNow()
	maxEventFrequency := telemetryTransactionSamplingFrequency.Get(&t.st.SV)
	requiredTimeElapsed := time.Second / time.Duration(maxEventFrequency)

	// Avoid taking the full lock if we don't have to.
	var enoughTimeElapsed bool
	func() {
		t.mu.RLock()
		defer t.mu.RUnlock()
		enoughTimeElapsed = txnStart.Sub(t.mu.lastEmittedTime) >= requiredTimeElapsed
	}()

	if !enoughTimeElapsed {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	enoughTimeElapsed = txnStart.Sub(t.mu.lastEmittedTime) >= requiredTimeElapsed
	if !enoughTimeElapsed {
		return false
	}

	t.mu.lastEmittedTime = txnStart

	return true
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*TelemetryLoggingTestingKnobs) ModuleTestingKnobs() {}

func (t *TelemetryLoggingMetrics) timeNow() time.Time {
	if t.Knobs != nil && t.Knobs.getTimeNow != nil {
		return t.Knobs.getTimeNow()
	}
	return timeutil.Now()
}

// shouldEmitStatementLog returns true if the stmt should be logged to telemetry. The last emitted time
// tracked by telemetry logging metrics will be updated to the given time if any of the following
// are met:
//   - The telemetry mode is set to "transaction" and the statement's transaction is being tracked
//     AND the transaction has not reached its limit for the number of statements logged.
//   - The telemetry mode is set to "statement" AND the required amount of time has elapsed
//   - The stmt is being forced to log.
func (t *TelemetryLoggingMetrics) shouldEmitStatementLog(
	newTime time.Time, txnIsTracked bool, stmtNum int, force bool,
) bool {
	maxEventFrequency := TelemetryMaxStatementEventFrequency.Get(&t.st.SV)
	requiredTimeElapsed := time.Second / time.Duration(maxEventFrequency)
	isTxnMode := telemetrySamplingMode.Get(&t.st.SV) == telemetryModeTransaction

	if stmtNum == 0 && isTxnMode {
		// We skip BEGIN statements for transaction telemetry mode. This is because BEGIN statements
		// don't have associated transaction execution ids since the transaction doesn't actually
		// officially start execution until its first statement.
		return false
	}

	var enoughTimeElapsed bool
	if !force {
		if txnIsTracked {
			// We are at the limit for the number of statements logged
			// for this transaction.

			return int64(stmtNum) <= telemetryQueriesPerTransactionMax.Get(&t.st.SV)
		}

		if isTxnMode {
			// If the transaction is not being tracked then we are done.
			return false
		}

		// Avoid taking the full lock if we don't have to.
		func() {
			t.mu.RLock()
			defer t.mu.RUnlock()
			enoughTimeElapsed = newTime.Sub(t.mu.lastEmittedTime) >= requiredTimeElapsed
		}()

		if !enoughTimeElapsed {
			return false
		}
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if !force && (newTime.Sub(t.mu.lastEmittedTime) < requiredTimeElapsed) {
		return false
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
	return t.skippedQueryCount.Swap(0)
}

func (t *TelemetryLoggingMetrics) incSkippedQueryCount() {
	t.skippedQueryCount.Add(1)
}

func (t *TelemetryLoggingMetrics) ResetLastEmittedTime() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.lastEmittedTime = time.Time{}
}
