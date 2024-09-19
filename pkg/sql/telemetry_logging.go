// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// Default value used to designate the maximum frequency at which events
// are logged to the telemetry channel.
const (
	internalConsoleAppName   = "$ internal-console"
	defaultMaxEventFrequency = 8
)

var TelemetryMaxStatementEventFrequency = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.telemetry.query_sampling.max_event_frequency",
	"the max event frequency (events per second) at which we sample executions for telemetry, "+
		"note that it is recommended that this value shares a log-line limit of 10 "+
		"logs per second on the telemetry pipeline with all other telemetry events. "+
		"If sampling mode is set to 'transaction', this value is ignored.",
	defaultMaxEventFrequency,
	settings.NonNegativeInt,
	settings.WithPublic,
)

var telemetryTransactionSamplingFrequency = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.telemetry.transaction_sampling.max_event_frequency",
	"the max event frequency (events per second) at which we sample transactions for "+
		"telemetry. If sampling mode is set to 'statement', this setting is ignored. In "+
		"practice, this means that we only sample a transaction if 1/max_event_frequency seconds "+
		"have elapsed since the last transaction was sampled.",
	defaultMaxEventFrequency,
	settings.NonNegativeInt,
	settings.WithPublic,
)

var telemetryStatementsPerTransactionMax = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.telemetry.transaction_sampling.statement_events_per_transaction.max",
	"the maximum number of statement events to log for every sampled transaction. "+
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
		"per statement).",
	"statement",
	map[int64]string{
		telemetryModeStatement:   "statement",
		telemetryModeTransaction: "transaction",
	},
	settings.WithPublic,
)

// SampledQuery objects are short-lived but can be
// allocated frequently if logging frequency is high.
var sampledQueryPool = sync.Pool{
	New: func() interface{} {
		return new(eventpb.SampledQuery)
	},
}

func getSampledQuery() *eventpb.SampledQuery {
	return sampledQueryPool.Get().(*eventpb.SampledQuery)
}

func releaseSampledQuery(sq *eventpb.SampledQuery) {
	*sq = eventpb.SampledQuery{}
	sampledQueryPool.Put(sq)
}

// SampledTransaction objects are short-lived but can be
// allocated frequently if logging frequency is high.
var sampledTransactionPool = sync.Pool{
	New: func() interface{} {
		return new(eventpb.SampledTransaction)
	},
}

func getSampledTransaction() *eventpb.SampledTransaction {
	return sampledTransactionPool.Get().(*eventpb.SampledTransaction)
}

func releaseSampledTransaction(st *eventpb.SampledTransaction) {
	*st = eventpb.SampledTransaction{}
	sampledTransactionPool.Put(st)
}

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
//     telemetryStatementsPerTransactionMax.
type telemetryLoggingMetrics struct {
	st *cluster.Settings

	mu struct {
		syncutil.RWMutex
		// The last time at which an event was sampled to the telemetry channel.
		lastSampledTime time.Time
	}

	Knobs *TelemetryLoggingTestingKnobs

	// skippedQueryCount is used to produce the count of non-sampled queries.
	skippedQueryCount atomic.Uint64

	// skippedTransactionCount is used to produce the count of non-sampled transactions.
	skippedTransactionCount atomic.Uint64
}

func newTelemetryLoggingMetrics(
	knobs *TelemetryLoggingTestingKnobs, st *cluster.Settings,
) *telemetryLoggingMetrics {
	t := telemetryLoggingMetrics{Knobs: knobs, st: st}
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

// shouldEmitTransactionLog returns true if the transaction should be tracked for telemetry.
// A transaction is tracked if telemetry logging is enabled , the telemetry mode is set to "transaction"
// and at least one of the following conditions is true:
//   - the transaction is not internal OR internal queries are enabled
//   - the required amount of time has elapsed since the last transaction began sampling
//   - the transaction is from the console and telemetryInternalConsoleQueriesEnabled is true
//   - the transaction has tracing enabled
//
// If the conditions are met, the last sampled time is updated.
func (t *telemetryLoggingMetrics) shouldEmitTransactionLog(
	isTracing, isInternal bool, applicationName string,
) (emit bool, skippedTxns uint64) {
	// We should not increase the skipped transaction count if telemetry logging is disabled.
	if !telemetryLoggingEnabled.Get(&t.st.SV) {
		return false, t.skippedTransactionCount.Load()
	}
	if telemetrySamplingMode.Get(&t.st.SV) != telemetryModeTransaction {
		return false, t.skippedTransactionCount.Load()
	}
	logConsoleQuery := telemetryInternalConsoleQueriesEnabled.Get(&t.st.SV) &&
		strings.HasPrefix(applicationName, internalConsoleAppName)
	if !logConsoleQuery && isInternal && !telemetryInternalQueriesEnabled.Get(&t.st.SV) {
		return false, t.skippedTransactionCount.Load()
	}
	maxEventFrequency := telemetryTransactionSamplingFrequency.Get(&t.st.SV)
	if maxEventFrequency == 0 {
		return false, t.skippedTransactionCount.Load()
	}

	txnSampleTime := t.timeNow()
	tracingEnabled := t.isTracing(nil, isTracing)

	if logConsoleQuery || tracingEnabled {
		// Force log.
		t.mu.Lock()
		defer t.mu.Unlock()
		t.mu.lastSampledTime = txnSampleTime
		return true, t.skippedTransactionCount.Swap(0)
	}

	requiredTimeElapsed := time.Second / time.Duration(maxEventFrequency)
	var enoughTimeElapsed bool
	func() {
		// Avoid taking the full lock if we don't have to.
		t.mu.RLock()
		defer t.mu.RUnlock()
		enoughTimeElapsed = txnSampleTime.Sub(t.mu.lastSampledTime) >= requiredTimeElapsed
	}()

	if !enoughTimeElapsed {
		return false, t.skippedTransactionCount.Add(1)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if txnSampleTime.Sub(t.mu.lastSampledTime) < requiredTimeElapsed {
		return false, t.skippedTransactionCount.Add(1)
	}

	t.mu.lastSampledTime = txnSampleTime

	return true, t.skippedTransactionCount.Swap(0)
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*TelemetryLoggingTestingKnobs) ModuleTestingKnobs() {}

func (t *telemetryLoggingMetrics) timeNow() time.Time {
	if t.Knobs != nil && t.Knobs.getTimeNow != nil {
		return t.Knobs.getTimeNow()
	}
	return timeutil.Now()
}

// shouldEmitStatementLog returns true if the stmt should be logged to telemetry.
// One of the following must be true for a statement to be logged:
//   - The telemetry mode is set to "transaction" and the statement's transaction is being tracked
//     AND the transaction has not reached its limit for the number of statements logged.
//   - The telemetry mode is set to "statement" AND the required amount of time has elapsed
//   - The stmt is being forced to log.
//
// In addition, the lastSampledTime tracked by TelemetryLoggingMetrics is updated if the statement
// is logged and we are in "statement" mode.
func (t *telemetryLoggingMetrics) shouldEmitStatementLog(
	txnIsTracked bool, stmtNum int, forceSampling bool,
) (emit bool, skippedQueryCount uint64) {
	// For these early exit cases, we don't want to increment the skipped queries count
	// since telemetry logging for statements is off in these cases.
	if !telemetryLoggingEnabled.Get(&t.st.SV) {
		return false, t.skippedQueryCount.Load()
	}
	isTxnMode := telemetrySamplingMode.Get(&t.st.SV) == telemetryModeTransaction
	if isTxnMode && telemetryTransactionSamplingFrequency.Get(&t.st.SV) == 0 {
		return false, t.skippedQueryCount.Load()
	}
	maxEventFrequency := TelemetryMaxStatementEventFrequency.Get(&t.st.SV)
	if !isTxnMode && maxEventFrequency == 0 {
		return false, t.skippedQueryCount.Load()
	}

	newTime := t.timeNow()

	if forceSampling {
		// We should hold the lock while resetting the skipped queries.
		t.mu.Lock()
		defer t.mu.Unlock()
		if !isTxnMode {
			t.mu.lastSampledTime = newTime
		}
		return true, t.skippedQueryCount.Swap(0)
	}

	if isTxnMode {
		if stmtNum == 0 {
			// We skip BEGIN statements for transaction telemetry mode. This is because BEGIN statements
			// don't have associated transaction execution ids since the transaction doesn't actually
			// officially start execution until its first statement.
			return false, t.skippedQueryCount.Add(1)
		}

		if txnIsTracked {
			// Log if we are not at the limit for the number of statements logged
			// for this transaction.
			// We don't need to update the last sampled time in this case.
			if int64(stmtNum) <= telemetryStatementsPerTransactionMax.Get(&t.st.SV) {
				return true, t.skippedQueryCount.Swap(0)
			}
			return false, t.skippedQueryCount.Add(1)
		}

		// If the transaction is not being tracked then we are done.
		return false, t.skippedQueryCount.Add(1)
	}

	// We are in statement mode. Check if enough time has elapsed since the last sampled time.
	requiredTimeElapsed := time.Second / time.Duration(maxEventFrequency)

	var enoughTimeElapsed bool
	func() {
		// Avoid taking the full lock if we don't have to.
		t.mu.RLock()
		defer t.mu.RUnlock()
		enoughTimeElapsed = newTime.Sub(t.mu.lastSampledTime) >= requiredTimeElapsed
	}()

	if !enoughTimeElapsed {
		return false, t.skippedQueryCount.Add(1)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if newTime.Sub(t.mu.lastSampledTime) < requiredTimeElapsed {
		return false, t.skippedQueryCount.Add(1)
	}

	t.mu.lastSampledTime = newTime
	return true, t.skippedQueryCount.Swap(0)
}

func (t *telemetryLoggingMetrics) getQueryLevelStats(
	queryLevelStats execstats.QueryLevelStats,
) execstats.QueryLevelStats {
	if t.Knobs != nil && t.Knobs.getQueryLevelStats != nil {
		return t.Knobs.getQueryLevelStats()
	}
	return queryLevelStats
}

func (t *telemetryLoggingMetrics) isTracing(_ *tracing.Span, tracingEnabled bool) bool {
	if t.Knobs != nil && t.Knobs.getTracingStatus != nil {
		return t.Knobs.getTracingStatus()
	}
	return tracingEnabled
}

func (t *telemetryLoggingMetrics) resetLastSampledTime() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.lastSampledTime = time.Time{}
}

// resetCounters resets the skipped query and transaction counters
func (t *telemetryLoggingMetrics) resetCounters() {
	t.skippedQueryCount.Swap(0)
	t.skippedTransactionCount.Swap(0)
}

func (t *telemetryLoggingMetrics) getSkippedTransactionCount() uint64 {
	return t.skippedTransactionCount.Load()
}
