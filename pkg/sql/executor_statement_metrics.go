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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
	SQLActiveStatements   *metric.Gauge

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

	SQLStatsFlushStarted  *metric.Counter
	SQLStatsFlushFailure  *metric.Counter
	SQLStatsFlushDuration *metric.Histogram
	SQLStatsRemovedRows   *metric.Counter

	SQLTxnStatsCollectionOverhead *metric.Histogram
}

// StatsMetrics is part of the metric.Struct interface.
var _ metric.Struct = StatsMetrics{}

// MetricStruct is part of the metric.Struct interface.
func (StatsMetrics) MetricStruct() {}

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
	stmtErr error,
	stats topLevelQueryStats,
) {
	phaseTimes := ex.statsCollector.PhaseTimes()

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

	recordedStmtStatsKey := roachpb.StatementStatisticsKey{
		Query:        stmt.StmtNoConstants,
		QuerySummary: stmt.StmtSummary,
		DistSQL:      flags.IsDistributed(),
		Vec:          flags.IsSet(planFlagVectorized),
		ImplicitTxn:  flags.IsSet(planFlagImplicitTxn),
		FullScan:     flags.IsSet(planFlagContainsFullIndexScan) || flags.IsSet(planFlagContainsFullTableScan),
		Failed:       stmtErr != nil,
		Database:     planner.SessionData().Database,
		PlanHash:     planner.instrumentation.planGist.Hash(),
	}

	// We only populate the transaction fingerprint ID field if we are in an
	// implicit transaction.
	//
	// TODO(azhng): This will require some big refactoring later, we already
	//  compute statement's fingerprintID in RecordStatement().
	//  However, we need to recompute the Fingerprint() here because this
	//  is required to populate the transaction fingerprint ID field.
	//
	//  The reason behind it is that: for explicit transactions, we have a final
	//  callback that will eventually invoke
	//  statsCollector.EndExplicitTransaction() which will use the extraTxnState
	//  stored in the connExecutor to compute the transaction fingerprintID.
	//  Unfortunately, that callback is not invoked for implicit transactions,
	//  because we don't create temporary stats container for the implicit
	//  transactions. (The statement stats directly gets written to the actual
	//  stats container). This means that, unless we populate the transaction
	//  fingerprintID here, we will not have another chance to do so later.
	if ex.implicitTxn() {
		stmtFingerprintID := recordedStmtStatsKey.FingerprintID()
		txnFingerprintHash := util.MakeFNV64()
		txnFingerprintHash.Add(uint64(stmtFingerprintID))
		recordedStmtStatsKey.TransactionFingerprintID =
			roachpb.TransactionFingerprintID(txnFingerprintHash.Sum())
	}

	recordedStmtStats := sqlstats.RecordedStmtStats{
		AutoRetryCount:  automaticRetryCount,
		RowsAffected:    rowsAffected,
		ParseLatency:    parseLat,
		PlanLatency:     planLat,
		RunLatency:      runLat,
		ServiceLatency:  svcLat,
		OverheadLatency: execOverhead,
		BytesRead:       stats.bytesRead,
		RowsRead:        stats.rowsRead,
		RowsWritten:     stats.rowsWritten,
		Nodes:           getNodesFromPlanner(planner),
		StatementType:   stmt.AST.StatementType(),
		Plan:            planner.instrumentation.PlanForStats(ctx),
		PlanGist:        planner.instrumentation.planGist.String(),
		StatementError:  stmtErr,
	}

	stmtFingerprintID, err :=
		ex.statsCollector.RecordStatement(ctx, recordedStmtStatsKey, recordedStmtStats)

	if err != nil {
		if log.V(1) {
			log.Warningf(ctx, "failed to record statement: %s", err)
		}
		ex.server.ServerMetrics.StatsMetrics.DiscardedStatsCount.Inc(1)
	}

	// Do some transaction level accounting for the transaction this statement is
	// a part of.

	// We limit the number of statementFingerprintIDs stored for a transaction, as dictated
	// by the TxnStatsNumStmtFingerprintIDsToRecord cluster setting.
	maxStmtFingerprintIDsLen := sqlstats.TxnStatsNumStmtFingerprintIDsToRecord.Get(&ex.server.cfg.Settings.SV)
	if int64(len(ex.extraTxnState.transactionStatementFingerprintIDs)) < maxStmtFingerprintIDsLen {
		ex.extraTxnState.transactionStatementFingerprintIDs = append(
			ex.extraTxnState.transactionStatementFingerprintIDs, stmtFingerprintID)
	}

	// Add the current statement's ID to the hash. We don't track queries issued
	// by the internal executor, in which case the hash is uninitialized, and
	// can therefore be safely ignored.
	if ex.extraTxnState.transactionStatementsHash.IsInitialized() {
		ex.extraTxnState.transactionStatementsHash.Add(uint64(stmtFingerprintID))
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

func getNodesFromPlanner(planner *planner) []int64 {
	// Retrieve the list of all nodes which the statement was executed on.
	var nodes []int64
	if planner.instrumentation.sp != nil {
		trace := planner.instrumentation.sp.GetRecording(tracing.RecordingStructured)
		// ForEach returns nodes in order.
		execinfrapb.ExtractNodesFromSpans(planner.EvalContext().Context, trace).ForEach(func(i int) {
			nodes = append(nodes, int64(i))
		})
	}

	return nodes
}
