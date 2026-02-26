// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/sql/idxrecommendations"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
)

// EngineMetrics groups a set of SQL metrics.
type EngineMetrics struct {
	// The subset of SELECTs that are requested to be processed through DistSQL.
	DistSQLSelectCount *metric.Counter
	// The subset of SELECTs that were executed by DistSQL with full or partial
	// distribution.
	DistSQLSelectDistributedCount *metric.Counter
	DistSQLExecLatency            metric.IHistogram
	DistSQLServiceLatency         metric.IHistogram

	SQLOptPlanCacheHits   *metric.Counter
	SQLOptPlanCacheMisses *metric.Counter

	StatementFingerprintCount *metric.UniqueCounter
	SQLExecLatencyDetail      *metric.HistogramVec

	SQLExecLatency metric.IHistogram
	// Exec Latency of only non-AOST queries
	SQLExecLatencyConsistent metric.IHistogram
	// Exec Latency of only AOST queries
	SQLExecLatencyHistorical metric.IHistogram

	SQLServiceLatency *aggmetric.SQLHistogram
	// Service Latency of only non-AOST queries
	SQLServiceLatencyConsistent metric.IHistogram
	// Service Latency of only AOST queries
	SQLServiceLatencyHistorical metric.IHistogram

	SQLTxnLatency       *aggmetric.SQLHistogram
	SQLTxnsOpen         *aggmetric.SQLGauge
	SQLActiveStatements *aggmetric.SQLGauge
	SQLContendedTxns    *metric.Counter

	// TxnAbortCount counts transactions that were aborted, either due
	// to non-retryable errors, or retryable errors when the client-side
	// retry protocol is not in use.
	TxnAbortCount *metric.Counter

	// FailureCount counts non-retryable errors in open transactions.
	FailureCount *aggmetric.SQLCounter

	// StatementTimeoutCount tracks the number of statement failures due
	// to exceeding the statement timeout.
	StatementTimeoutCount *metric.Counter

	// TransactionTimeoutCount tracks the number of statement failures due
	// to exceeding the transaction timeout.
	TransactionTimeoutCount *metric.Counter

	// FullTableOrIndexScanCount counts the number of full table or index scans.
	FullTableOrIndexScanCount *aggmetric.SQLCounter

	// FullTableOrIndexScanRejectedCount counts the number of queries that were
	// rejected because of the `disallow_full_table_scans` guardrail.
	FullTableOrIndexScanRejectedCount *metric.Counter

	// TxnRetryCount counts the number of automatic transaction retries that
	// have occurred.
	TxnRetryCount *metric.Counter

	// StatementRetryCount counts the number of automatic statement retries that
	// have occurred under READ COMMITTED isolation.
	StatementRetryCount *metric.Counter

	// StatementRowsRead counts the number of rows read by SQL statements from
	// primary and secondary indexes. Note that some secondary indexes can have
	// multiple index rows per primary index row (e.g. inverted and vector).
	StatementRowsRead *metric.Counter

	// StatementBytesRead counts the number of bytes scanned by SQL statements
	// from primary and secondary indexes.
	StatementBytesRead *metric.Counter

	// StatementIndexRowsWritten counts the number of primary and secondary index
	// rows modified by SQL statements.
	StatementIndexRowsWritten *metric.Counter

	// StatementIndexBytesWritten counts the number of primary and secondary index
	// bytes modified by SQL statements.
	StatementIndexBytesWritten *metric.Counter

	// QueryWithStatementHintsCount counts queries executed with external statement hints.
	QueryWithStatementHintsCount *metric.Counter

	// RLSPoliciesAppliedCount counts the number of SQL statements where
	// row-level security policies were applied during query planning.
	RLSPoliciesAppliedCount *metric.Counter
}

// EngineMetrics implements the metric.Struct interface.
var _ metric.Struct = EngineMetrics{}

// MetricStruct is part of the metric.Struct interface.
func (EngineMetrics) MetricStruct() {}

// StatsMetrics groups metrics related to SQL Stats collection.
type StatsMetrics struct {
	SQLStatsMemoryMaxBytesHist  metric.IHistogram
	SQLStatsMemoryCurBytesCount *metric.Gauge

	ReportedSQLStatsMemoryMaxBytesHist  metric.IHistogram
	ReportedSQLStatsMemoryCurBytesCount *metric.Gauge

	DiscardedStatsCount *metric.Counter

	SQLStatsFlushesSuccessful       *metric.Counter
	SQLStatsFlushDoneSignalsIgnored *metric.Counter
	SQLStatsFlushFingerprintCount   *metric.Counter
	SQLStatsFlushesFailed           *metric.Counter
	SQLStatsFlushLatency            metric.IHistogram
	SQLStatsRemovedRows             *metric.Counter

	SQLTxnStatsCollectionOverhead metric.IHistogram
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

// recordStatementSummary gathers various details pertaining to the
// last executed statement/query and performs the associated
// accounting in the passed-in EngineMetrics.
//   - distSQLUsed reports whether the query was distributed.
//   - automaticRetryTxnCount is the count of implicit txn retries
//     so far.
//   - automaticRetryStmtCount is the count of implicit stmt retries
//     so far.
//   - result is the result set computed by the query/statement.
//   - err is the error encountered, if any.
func (ex *connExecutor) recordStatementSummary(
	ctx context.Context,
	planner *planner,
	automaticRetryTxnCount int,
	automaticRetryStmtCount int,
	rowsAffected int,
	stmtErr error,
	stats topLevelQueryStats,
) appstatspb.StmtFingerprintID {

	stmt := &planner.stmt
	flags := planner.curPlan.flags
	ex.recordStatementLatencyMetrics(
		stmt, flags, automaticRetryTxnCount+automaticRetryStmtCount, ex.statsCollector.RunLatency(), ex.statsCollector.ServiceLatency(),
	)

	idxRecommendations := idxrecommendations.FormatIdxRecommendations(planner.instrumentation.indexRecs)
	queryLevelStats, queryLevelStatsOk := planner.instrumentation.GetQueryLevelStats()

	stmtFingerprintID := planner.instrumentation.fingerprintId
	autoRetryReason := ex.state.mu.autoRetryReason
	if automaticRetryStmtCount > 0 {
		autoRetryReason = planner.autoRetryStmtReason
	}

	// Update SQL statement metrics.
	ex.metrics.EngineMetrics.StatementRowsRead.Inc(stats.rowsRead)
	ex.metrics.EngineMetrics.StatementBytesRead.Inc(stats.bytesRead)
	ex.metrics.EngineMetrics.StatementIndexRowsWritten.Inc(stats.indexRowsWritten)
	ex.metrics.EngineMetrics.StatementIndexBytesWritten.Inc(stats.indexBytesWritten)
	if ex.statsCollector.EnabledForTransaction() {
		b := sqlstats.NewRecordedStatementStatsBuilder(
			stmtFingerprintID,
			planner.SessionData().Database,
			stmt.StmtNoConstants,
			stmt.StmtSummary,
			stmt.AST.StatementType(),
			ex.statsCollector.CurrentApplicationName(),
		).
			QueryID(stmt.QueryID).
			SessionID(ex.planner.extendedEvalCtx.SessionID).
			PlanMetadata(
				flags.IsSet(planFlagGeneric),
				flags.ShouldBeDistributed(),
				flags.IsSet(planFlagVectorized),
				flags.IsSet(planFlagImplicitTxn),
				flags.IsSet(planFlagContainsFullIndexScan) || flags.IsSet(planFlagContainsFullTableScan),
			).
			PlanGist(planner.instrumentation.planGist.String(), planner.instrumentation.planGist.Hash()).
			LatencyRecorder(ex.statsCollector).
			QueryLevelStats(stats.bytesRead, stats.rowsRead, stats.rowsWritten, stats.kvCPUTimeNanos.Nanoseconds()).
			ExecStats(queryLevelStats).
			// TODO(mgartner): Use a slice of struct{uint64, uint64} instead of
			// converting to strings.
			Indexes(planner.instrumentation.indexesUsed.Strings()).
			AutoRetry(automaticRetryTxnCount+automaticRetryStmtCount, autoRetryReason).
			RowsAffected(rowsAffected).
			IndexRecommendations(idxRecommendations).
			QueryTags(stmt.QueryTags).
			StatementError(stmtErr)

		if ex.extraTxnState.underOuterTxn {
			b.UnderOuterTxn()
		}

		if len(stmt.Hints) > 0 {
			b.AppliedStatementHints()
		}

		ex.statsCollector.RecordStatement(ctx, b.Build())
	}

	// Record statement execution statistics if span is recorded and no error was
	// encountered while collecting query-level statistics.
	if queryLevelStatsOk {
		for _, ev := range queryLevelStats.ContentionEvents {
			if ev.IsLatch && !planner.SessionData().RegisterLatchWaitContentionEvents {
				// This event should be included in the trace and contention time
				// metrics, but not registered with the *_contention_events tables.
				continue
			}
			contentionEvent := contentionpb.ExtendedContentionEvent{
				BlockingEvent:            ev,
				WaitingTxnID:             planner.txn.ID(),
				WaitingStmtFingerprintID: stmtFingerprintID,
				WaitingStmtID:            stmt.QueryID,
				ContentionType:           contentionpb.ContentionType_LOCK_WAIT,
			}

			ex.server.cfg.ContentionRegistry.AddContentionEvent(contentionEvent)
		}

		if queryLevelStats.ContentionTime > 0 {
			dbName := ex.sessionData().Database
			appName := ex.sessionData().ApplicationName
			ex.planner.DistSQLPlanner().distSQLSrv.Metrics.ContendedQueriesCount.Inc(1, dbName, appName)
			ex.planner.DistSQLPlanner().distSQLSrv.Metrics.CumulativeContentionNanos.Inc(queryLevelStats.ContentionTime.Nanoseconds(), dbName, appName)
		}
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
	ex.extraTxnState.idleLatency += ex.statsCollector.IdleLatency()

	if log.V(2) {
		// ages since significant epochs
		sessionAge := ex.statsCollector.PhaseTimes().GetSessionAge().Seconds()
		parseLatSec := ex.statsCollector.ParsingLatency().Seconds()
		planLatSec := ex.statsCollector.PlanningLatency().Seconds()
		runLatSec := ex.statsCollector.RunLatency().Seconds()
		svcLatSec := ex.statsCollector.ServiceLatency().Seconds()
		execOverheadSec := ex.statsCollector.ExecOverheadLatency().Seconds()

		log.Dev.Infof(ctx,
			"query stats: %d rows, %d retries, "+
				"parse %.2fµs (%.1f%%), "+
				"plan %.2fµs (%.1f%%), "+
				"run %.2fµs (%.1f%%), "+
				"overhead %.2fµs (%.1f%%), "+
				"session age %.4fs",
			rowsAffected, automaticRetryTxnCount+automaticRetryStmtCount,
			parseLatSec*1e6, 100*parseLatSec/svcLatSec,
			planLatSec*1e6, 100*planLatSec/svcLatSec,
			runLatSec*1e6, 100*runLatSec/svcLatSec,
			execOverheadSec*1e6, 100*execOverheadSec/svcLatSec,
			sessionAge,
		)
	}

	return stmtFingerprintID
}

func (ex *connExecutor) recordStatementLatencyMetrics(
	stmt *Statement,
	flags planFlags,
	automaticRetryCount int,
	runLatRaw time.Duration,
	svcLatRaw time.Duration,
) {
	shouldIncludeInLatencyMetrics := shouldIncludeStmtInLatencyMetrics(stmt)
	if automaticRetryCount == 0 {
		ex.updateOptCounters(flags)
		m := &ex.metrics.EngineMetrics

		m.StatementFingerprintCount.Add([]byte(stmt.StmtNoConstants))

		if flags.ShouldBeDistributed() {
			if _, ok := stmt.AST.(*tree.Select); ok {
				m.DistSQLSelectCount.Inc(1)
				if flags.IsSet(planFlagDistributedExecution) {
					m.DistSQLSelectDistributedCount.Inc(1)
				}
			}
			if shouldIncludeInLatencyMetrics {
				m.DistSQLExecLatency.RecordValue(runLatRaw.Nanoseconds())
				m.DistSQLServiceLatency.RecordValue(svcLatRaw.Nanoseconds())
			}
		}
		if shouldIncludeInLatencyMetrics {
			if detailedLatencyMetrics.Get(&ex.server.cfg.Settings.SV) {
				labels := map[string]string{
					detailedLatencyMetricLabel: stmt.StmtNoConstants,
				}
				m.SQLExecLatencyDetail.Observe(labels, float64(runLatRaw.Nanoseconds()))
			}
			m.SQLExecLatency.RecordValue(runLatRaw.Nanoseconds())
			m.SQLServiceLatency.RecordValue(svcLatRaw.Nanoseconds(), ex.sessionData().Database, ex.sessionData().ApplicationName)
			if ex.state.isHistorical.Load() {
				m.SQLExecLatencyHistorical.RecordValue(runLatRaw.Nanoseconds())
				m.SQLServiceLatencyHistorical.RecordValue(svcLatRaw.Nanoseconds())
			} else {
				m.SQLExecLatencyConsistent.RecordValue(runLatRaw.Nanoseconds())
				m.SQLServiceLatencyConsistent.RecordValue(svcLatRaw.Nanoseconds())
			}
		}
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
