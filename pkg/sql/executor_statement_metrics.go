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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

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

// EngineMetrics implements the metric.Struct interface.
var _ metric.Struct = EngineMetrics{}

// MetricStruct is part of the metric.Struct interface.
func (EngineMetrics) MetricStruct() {}

// StatsMetrics groups metrics related to SQL Stats collection.
type StatsMetrics struct {
	SQLStatsMemoryMaxBytesHist  *metric.Histogram
	SQLStatsMemoryCurBytesCount *metric.Gauge

	ReportedSQLStatsMemoryMaxBytesHist  *metric.Histogram
	ReportedSQLStatsMemoryCurBytesCount *metric.Gauge

	DiscardedStatsCount *metric.Counter
}

// StatsMetrics is part of the metric.Struct interface.
var _ metric.Struct = StatsMetrics{}

// MetricStruct is part of the metric.Struct interface.
func (StatsMetrics) MetricStruct() {}

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
	stmtErr error,
	stats topLevelQueryStats,
) {
	phaseTimes := ex.statsCollector.phaseTimes

	// Collect the statistics.
	runLatRaw := phaseTimes.GetRunLatency()
	runLat := runLatRaw.Seconds()
	parseLat := phaseTimes.GetParsingLatency().Seconds()
	planLat := phaseTimes.GetPlanningLatency().Seconds()
	// We want to exclude any overhead to reduce possible confusion.
	svcLatRaw := phaseTimes.GetServiceLatencyNoOverhead()
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

	stmtID, err := ex.statsCollector.recordStatement(
		ctx, stmt, planner.instrumentation.PlanForStats(ctx),
		flags.IsDistributed(), flags.IsSet(planFlagVectorized),
		flags.IsSet(planFlagImplicitTxn),
		flags.IsSet(planFlagContainsFullIndexScan) || flags.IsSet(planFlagContainsFullTableScan),
		automaticRetryCount, rowsAffected, stmtErr,
		parseLat, planLat, runLat, svcLat, execOverhead, stats, planner,
	)

	if err != nil {
		if log.V(1) {
			log.Warningf(ctx, "failed to record statement: %s", err)
		}
		ex.metrics.StatsMetrics.DiscardedStatsCount.Inc(1)
	}

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
		sessionAge := phaseTimes.GetSessionAge().Seconds()

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

// We only want to keep track of DML (Data Manipulation Language) statements in our latency metrics.
func shouldIncludeStmtInLatencyMetrics(stmt *Statement) bool {
	return stmt.AST.StatementType() == tree.TypeDML
}
