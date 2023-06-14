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
	"context"
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

var TelemetryMaxEventFrequency = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.telemetry.query_sampling.max_event_frequency",
	"the max event frequency at which we sample executions for telemetry, "+
		"note that it is recommended that this value shares a log-line limit of 10 "+
		" logs per second on the telemetry pipeline with all other telemetry events. "+
		"If sampling mode is set to transaction, all statements associated with a single "+
		"transaction are counted as 1 unit.",
	defaultMaxEventFrequency,
	settings.NonNegativeInt,
)

const (
	TelemetryModeStatement = iota
	TelemetryModeTransaction
)

var telemetrySamplingMode = settings.RegisterEnumSetting(
	settings.TenantWritable,
	"sql.telemetry.query_sampling.mode",
	"the execution level used for telemetry sampling. If set to 'statement', events "+
		"are sampled at the statement execution level. If set to transaction, events are "+
		"sampled at the txn execution level, i.e. all statements for a txn will be logged "+
		"and are counted together as one sampled event (events are still emitted one per "+
		"statement)",
	"statement",
	map[int64]string{
		TelemetryModeStatement:   "statement",
		TelemetryModeTransaction: "transaction",
	},
)

// TelemetryLoggingMetrics keeps track of the last time at which an event
// was logged to the telemetry channel, and the number of skipped queries
// since the last logged event.
type TelemetryLoggingMetrics struct {
	st *cluster.Settings

	mu struct {
		syncutil.RWMutex
		// The timestamp of the last emitted telemetry event.
		lastEmittedTime time.Time

		// observedTxnExecutions is used to track txn executions that are currently
		// being logged. When the sampling mode is set to txns, we must ensure we
		// log all stmts for a txn. Txns are removed upon completing execution when
		// all events have been captured.
		observedTxnExecutions map[string]interface{}
	}

	Knobs *TelemetryLoggingTestingKnobs

	// skippedQueryCount is used to produce the count of non-sampled queries.
	skippedQueryCount uint64
}

func newTelemetryLoggingmetrics(
	knobs *TelemetryLoggingTestingKnobs, st *cluster.Settings,
) *TelemetryLoggingMetrics {
	t := TelemetryLoggingMetrics{Knobs: knobs, st: st}
	t.mu.observedTxnExecutions = make(map[string]interface{})
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

// registerOnTelemetrySamplingModeChange sets up the callback for when the
// telemetry sampling mode is changed. When switching from txn to stmt, we
// clear the txns we are currently tracking for logging.
func (t *TelemetryLoggingMetrics) registerOnTelemetrySamplingModeChange(
	settings *cluster.Settings,
) {
	telemetrySamplingMode.SetOnChange(&settings.SV, func(ctx context.Context) {
		mode := telemetrySamplingMode.Get(&settings.SV)
		t.mu.Lock()
		defer t.mu.Unlock()
		if mode == TelemetryModeStatement {
			// Clear currently observed txns.
			t.mu.observedTxnExecutions = make(map[string]interface{})
		}
	})
}

func (t *TelemetryLoggingMetrics) onTxnFinish(txnExecutionID string) {
	if telemetrySamplingMode.Get(&t.st.SV) == TelemetryModeTransaction {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.mu.observedTxnExecutions, txnExecutionID)
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*TelemetryLoggingTestingKnobs) ModuleTestingKnobs() {}

func (t *TelemetryLoggingMetrics) timeNow() time.Time {
	if t.Knobs != nil && t.Knobs.getTimeNow != nil {
		return t.Knobs.getTimeNow()
	}
	return timeutil.Now()
}

// isObservingTxnExecution returns true if stmts in this txn should be // logged to telemetry.
func (t *TelemetryLoggingMetrics) isObservingTxnExecution(txnExecutionID string) bool {
	isTxnMode := telemetrySamplingMode.Get(&t.st.SV) == TelemetryModeTransaction
	if !isTxnMode || txnExecutionID == "" {
		return false
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	_, ok := t.mu.observedTxnExecutions[txnExecutionID]

	return ok
}

// maybeUpdateLastLogTime returns true if the stmt should be logged. If true, then
// the last emitted time tracked by telemetry logging metrics is updated to the given time.
func (t *TelemetryLoggingMetrics) maybeUpdateLastLogTime(
	newTime time.Time, txnExecutionID string, force bool,
) bool {
	maxEventFrequency := TelemetryMaxEventFrequency.Get(&t.st.SV)
	requiredTimeElapsed := time.Second / time.Duration(maxEventFrequency)
	isTxnMode := telemetrySamplingMode.Get(&t.st.SV) == TelemetryModeTransaction

	t.mu.Lock()
	defer t.mu.Unlock()

	lastEmittedTime := t.mu.lastEmittedTime

	if !force && newTime.Sub(lastEmittedTime) < requiredTimeElapsed {
		return false
	}

	if isTxnMode && txnExecutionID != "" {
		t.mu.observedTxnExecutions[txnExecutionID] = struct{}{}
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
