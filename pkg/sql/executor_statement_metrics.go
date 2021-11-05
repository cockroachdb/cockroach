// Copyright 2017 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// SQL execution is separated in 3+ phases:
// - parse/prepare
// - plan
// - run
//
// The commonly used term "execution latency" encompasses this entire
// process. However for the purpose of analyzing / optimizing
// individual parts of the SQL execution engine, it is useful to
// separate the durations of these individual phases. The code below
// does this.

// sessionPhase is used to index the Session.phaseTimes array.
type sessionPhase int

const (
	// When the session is created (pgwire). Used to compute
	// the session age.
	sessionInit sessionPhase = iota

	// Executor phases.
	sessionQueryReceived    // Query is received.
	sessionStartParse       // Parse starts.
	sessionEndParse         // Parse ends.
	plannerStartLogicalPlan // Planning starts.
	plannerEndLogicalPlan   // Planning ends.
	plannerStartExecStmt    // Execution starts.
	plannerEndExecStmt      // Execution ends.
	// Query is serviced. Note that we compute this even for empty queries or
	// "special" statements that have no execution, like SHOW TRANSACTION STATUS.
	sessionQueryServiced

	sessionTransactionReceived            // Transaction is received.
	sessionFirstStartExecTransaction      // Transaction is started for the first time.
	sessionMostRecentStartExecTransaction // Transaction is started for the most recent time.
	sessionEndExecTransaction             // Transaction is committed/rolled back.
	sessionStartTransactionCommit         // Transaction `COMMIT` starts.
	sessionEndTransactionCommit           // Transaction `COMMIT` ends.
	sessionStartPostCommitJob             // Post transaction `COMMIT` jobs start.
	sessionEndPostCommitJob               // Post transaction `COMMIT` jobs end.

	// sessionNumPhases must be listed last so that it can be used to
	// define arrays sufficiently large to hold all the other values.
	sessionNumPhases
)

// phaseTimes is the type of the session.phaseTimes array.
//
// It's important that this is an array and not a slice, as we rely on the array
// copy behavior.
type phaseTimes [sessionNumPhases]time.Time

// getServiceLatencyNoOverhead returns the latency of serving a query excluding
// miscellaneous sources of the overhead (e.g. internal retries). This method is
// safe to call if sessionQueryServiced phase hasn't been set yet.
func (p *phaseTimes) getServiceLatencyNoOverhead() time.Duration {
	// To have an accurate representation of how long it took to service this
	// single query, we ignore the time between when parsing ends and planning
	// begins. This avoids the latency being inflated in a few different cases:
	// when there are internal transaction retries, and when multiple statements
	// are submitted together, e.g. "SELECT 1; SELECT 2".
	//
	// If we're executing a portal, both parsing start and end times will be
	// zero, so subtracting the actual time at which the query was received will
	// produce a negative value which doesn't make sense. Therefore, we "fake"
	// the received time to be the parsing start time.
	var queryReceivedTime time.Time
	if p[sessionEndParse].IsZero() {
		queryReceivedTime = p[sessionStartParse]
	} else {
		queryReceivedTime = p[sessionQueryReceived]
	}
	parseLatency := p[sessionEndParse].Sub(queryReceivedTime)
	// If we encounter an error during the logical planning, the
	// plannerEndExecStmt phase will not be set, so we need to use the end of
	// planning phase in the computation of planAndExecuteLatency.
	var queryEndExecTime time.Time
	if p[plannerEndExecStmt].IsZero() {
		queryEndExecTime = p[plannerEndLogicalPlan]
	} else {
		queryEndExecTime = p[plannerEndExecStmt]
	}
	planAndExecuteLatency := queryEndExecTime.Sub(p[plannerStartLogicalPlan])
	return parseLatency + planAndExecuteLatency
}

// getServiceLatencyTotal returns the total latency of serving a query including
// any overhead like internal retries.
// NOTE: sessionQueryServiced phase must have been set.
func (p *phaseTimes) getServiceLatencyTotal() time.Duration {
	return p[sessionQueryServiced].Sub(p[sessionQueryReceived])
}

// getRunLatency returns the time between a query execution starting and ending.
func (p *phaseTimes) getRunLatency() time.Duration {
	return p[plannerEndExecStmt].Sub(p[plannerStartExecStmt])
}

// getPlanningLatency returns the time it takes for a query to be planned.
func (p *phaseTimes) getPlanningLatency() time.Duration {
	return p[plannerEndLogicalPlan].Sub(p[plannerStartLogicalPlan])
}

// getParsingLatency returns the time it takes for a query to be parsed.
func (p *phaseTimes) getParsingLatency() time.Duration {
	return p[sessionEndParse].Sub(p[sessionStartParse])
}

// getPostCommitJobsLatency returns the time spent running the post transaction
// commit jobs, such as schema changes.
func (p *phaseTimes) getPostCommitJobsLatency() time.Duration {
	return p[sessionEndPostCommitJob].Sub(p[sessionStartPostCommitJob])
}

func (p *phaseTimes) getTransactionRetryLatency() time.Duration {
	return p[sessionMostRecentStartExecTransaction].Sub(p[sessionFirstStartExecTransaction])
}

func (p *phaseTimes) getTransactionServiceLatency() time.Duration {
	return p[sessionEndExecTransaction].Sub(p[sessionTransactionReceived])
}

func (p *phaseTimes) getCommitLatency() time.Duration {
	return p[sessionEndTransactionCommit].Sub(p[sessionStartTransactionCommit])
}

// EngineMetrics groups a set of SQL metrics.
type EngineMetrics struct {
	// The subset of SELECTs that are processed through DistSQL.
	DistSQLSelectCount *metric.Counter
	// The subset of queries which we attempted and failed to plan with the
	// cost-based optimizer.
	SQLOptFallbackCount   *metric.Counter
	SQLOptPlanCacheHits   *metric.Counter
	SQLOptPlanCacheMisses *metric.Counter

	DistSQLExecLatency    *metric.Histogram
	SQLExecLatency        *metric.Histogram
	DistSQLServiceLatency *metric.Histogram
	SQLServiceLatency     *metric.Histogram
	SQLTxnLatency         *metric.Histogram
	SQLTxnsOpen           *metric.Gauge

	// TxnAbortCount counts transactions that were aborted, either due
	// to non-retriable errors, or retriable errors when the client-side
	// retry protocol is not in use.
	TxnAbortCount *metric.Counter

	// FailureCount counts non-retriable errors in open transactions.
	FailureCount *metric.Counter

	// FullTableOrIndexScanCount counts the number of full table or index scans.
	FullTableOrIndexScanCount *metric.Counter

	// FullTableOrIndexScanRejectedCount counts the number of queries that were
	// rejected because of the `disallow_full_table_scans` guardrail.
	FullTableOrIndexScanRejectedCount *metric.Counter
}

// EngineMetrics implements the metric.Struct interface
var _ metric.Struct = EngineMetrics{}

// MetricStruct is part of the metric.Struct interface.
func (EngineMetrics) MetricStruct() {}

// GuardrailMetrics groups metrics related to different guardrails in the SQL
// layer.
type GuardrailMetrics struct {
	TxnRowsWrittenLogCount *metric.Counter
	TxnRowsWrittenErrCount *metric.Counter
	TxnRowsReadLogCount    *metric.Counter
	TxnRowsReadErrCount    *metric.Counter
}

var _ metric.Struct = GuardrailMetrics{}

// MetricStruct is part of the metric.Struct interface.
func (GuardrailMetrics) MetricStruct() {}

// recordStatementSummery gathers various details pertaining to the
// last executed statement/query and performs the associated
// accounting in the passed-in EngineMetrics.
// - distSQLUsed reports whether the query was distributed.
// - automaticRetryCount is the count of implicit txn retries
//   so far.
// - result is the result set computed by the query/statement.
// - err is the error encountered, if any.
func (ex *connExecutor) recordStatementSummary(
	ctx context.Context,
	planner *planner,
	automaticRetryCount int,
	rowsAffected int,
	err error,
	stats topLevelQueryStats,
) {
	phaseTimes := &ex.statsCollector.phaseTimes

	// Collect the statistics.
	runLatRaw := phaseTimes.getRunLatency()
	runLat := runLatRaw.Seconds()
	parseLat := phaseTimes.getParsingLatency().Seconds()
	planLat := phaseTimes.getPlanningLatency().Seconds()
	// We want to exclude any overhead to reduce possible confusion.
	svcLatRaw := phaseTimes.getServiceLatencyNoOverhead()
	svcLat := svcLatRaw.Seconds()

	// processing latency: contributing towards SQL results.
	processingLat := parseLat + planLat + runLat

	// overhead latency: txn/retry management, error checking, etc
	execOverhead := svcLat - processingLat

	stmt := &planner.stmt
	flags := planner.curPlan.flags
	if automaticRetryCount == 0 {
		ex.updateOptCounters(flags)
		m := &ex.metrics.EngineMetrics
		if flags.IsDistributed() {
			if _, ok := stmt.AST.(*tree.Select); ok {
				m.DistSQLSelectCount.Inc(1)
			}
			m.DistSQLExecLatency.RecordValue(runLatRaw.Nanoseconds())
			m.DistSQLServiceLatency.RecordValue(svcLatRaw.Nanoseconds())
		}
		m.SQLExecLatency.RecordValue(runLatRaw.Nanoseconds())
		m.SQLServiceLatency.RecordValue(svcLatRaw.Nanoseconds())
	}

	stmtID := ex.statsCollector.recordStatement(
		stmt, planner.instrumentation.PlanForStats(ctx),
		flags.IsDistributed(), flags.IsSet(planFlagVectorized),
		flags.IsSet(planFlagImplicitTxn),
		flags.IsSet(planFlagContainsFullIndexScan) || flags.IsSet(planFlagContainsFullTableScan),
		automaticRetryCount, rowsAffected, err,
		parseLat, planLat, runLat, svcLat, execOverhead, stats, planner,
	)

	// Do some transaction level accounting for the transaction this statement is
	// a part of.

	// We limit the number of statementIDs stored for a transaction, as dictated
	// by the TxnStatsNumStmtIDsToRecord cluster setting.
	maxStmtIDsLen := TxnStatsNumStmtIDsToRecord.Get(&ex.server.cfg.Settings.SV)
	if int64(len(ex.extraTxnState.transactionStatementIDs)) < maxStmtIDsLen {
		ex.extraTxnState.transactionStatementIDs = append(
			ex.extraTxnState.transactionStatementIDs, stmtID)
	}
	// Add the current statement's ID to the hash. We don't track queries issued
	// by the internal executor, in which case the hash is uninitialized, and
	// can therefore be safely ignored.
	if ex.extraTxnState.transactionStatementsHash.IsInitialized() {
		ex.extraTxnState.transactionStatementsHash.Add(uint64(stmtID))
	}
	ex.extraTxnState.numRows += rowsAffected

	if log.V(2) {
		// ages since significant epochs
		sessionAge := phaseTimes[plannerEndExecStmt].
			Sub(phaseTimes[sessionInit]).Seconds()

		log.Infof(ctx,
			"query stats: %d rows, %d retries, "+
				"parse %.2fµs (%.1f%%), "+
				"plan %.2fµs (%.1f%%), "+
				"run %.2fµs (%.1f%%), "+
				"overhead %.2fµs (%.1f%%), "+
				"session age %.4fs",
			rowsAffected, automaticRetryCount,
			parseLat*1e6, 100*parseLat/svcLat,
			planLat*1e6, 100*planLat/svcLat,
			runLat*1e6, 100*runLat/svcLat,
			execOverhead*1e6, 100*execOverhead/svcLat,
			sessionAge,
		)
	}
}

func (ex *connExecutor) updateOptCounters(planFlags planFlags) {
	m := &ex.metrics.EngineMetrics

	if planFlags.IsSet(planFlagOptCacheHit) {
		m.SQLOptPlanCacheHits.Inc(1)
	} else if planFlags.IsSet(planFlagOptCacheMiss) {
		m.SQLOptPlanCacheMisses.Inc(1)
	}
}
