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

	// sessionNumPhases must be listed last so that it can be used to
	// define arrays sufficiently large to hold all the other values.
	sessionNumPhases
)

// phaseTimes is the type of the session.phaseTimes array.
//
// It's important that this is an array and not a slice, as we rely on the array
// copy behavior.
type phaseTimes [sessionNumPhases]time.Time

// getServiceLatency returns the time between a query being received and the end
// of run.
func (p *phaseTimes) getServiceLatency() time.Duration {
	// Ideally, service latency would always be defined as:
	// p[sessionQueryServiced] - p[sessionQueryReceived]. Unfortunately, this
	// isn't always possible with the current structure of the code, as the
	// service latency calculation is required when recording metrics for
	// a statement that hits the execution engine. At this point,
	// `sessionQueryServiced` is unset, because that happens in execCmd. To
	// prevent negative values for the case mentioned above, we have this second
	// possible way of calculating the service latency by relying on the
	// plannerEndExecStmt phase. It's worth noting that the plannerEndExecStmt
	// phase is unset for queries that don't go through the execution engine (such
	// as observer statements, prepare statements etc.), so simply relying on the
	// second calculation isn't an option either.
	if !p[sessionQueryServiced].IsZero() {
		return p[sessionQueryServiced].Sub(p[sessionQueryReceived])
	}
	return p[plannerEndExecStmt].Sub(p[sessionQueryReceived])
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
}

// EngineMetrics implements the metric.Struct interface
var _ metric.Struct = EngineMetrics{}

// MetricStruct is part of the metric.Struct interface.
func (EngineMetrics) MetricStruct() {}

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
	svcLatRaw := phaseTimes.getServiceLatency()
	svcLat := svcLatRaw.Seconds()

	// processing latency: contributing towards SQL results.
	processingLat := parseLat + planLat + runLat

	// overhead latency: txn/retry management, error checking, etc
	execOverhead := svcLat - processingLat

	stmt := &planner.stmt
	shouldIncludeInLatencyMetrics := shouldIncludeStmtInLatencyMetrics(stmt)
	flags := planner.curPlan.flags
	if automaticRetryCount == 0 {
		ex.updateOptCounters(flags)
		m := &ex.metrics.EngineMetrics
		if flags.IsDistributed() {
			if _, ok := stmt.AST.(*tree.Select); ok {
				m.DistSQLSelectCount.Inc(1)
			}
			if shouldIncludeInLatencyMetrics {
				m.DistSQLExecLatency.RecordValue(runLatRaw.Nanoseconds())
				m.DistSQLServiceLatency.RecordValue(svcLatRaw.Nanoseconds())
			}
		}
		if shouldIncludeInLatencyMetrics {
			m.SQLExecLatency.RecordValue(runLatRaw.Nanoseconds())
			m.SQLServiceLatency.RecordValue(svcLatRaw.Nanoseconds())
		}
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

// Bulk IO operations cause spikes in our time series chart for service latency. We exclude them from the service
// latency metrics to avoid confusions.
func shouldIncludeStmtInLatencyMetrics(stmt *Statement) bool {
	switch stmt.AST.(type) {
	case *tree.Backup:
		return false
	case *tree.ShowBackup:
		return false
	case *tree.Restore:
		return false
	case *tree.Import:
		return false
	case *tree.Export:
		return false
	case *tree.ScheduledBackup:
		return false
	case *tree.StreamIngestion:
		return false
	case *tree.ReplicationStream:
		return false
	}

	return true
}
