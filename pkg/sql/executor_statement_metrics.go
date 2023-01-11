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
	"sort"
	"strconv"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/idxrecommendations"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
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
	SQLContendedTxns      *metric.Counter

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

// recordStatementSummary gathers various details pertaining to the
// last executed statement/query and performs the associated
// accounting in the passed-in EngineMetrics.
//   - distSQLUsed reports whether the query was distributed.
//   - automaticRetryCount is the count of implicit txn retries
//     so far.
//   - result is the result set computed by the query/statement.
//   - err is the error encountered, if any.
func (ex *connExecutor) recordStatementSummary(
	ctx context.Context,
	planner *planner,
	automaticRetryCount int,
	rowsAffected int,
	stmtErr error,
	stats topLevelQueryStats,
) roachpb.StmtFingerprintID {
	phaseTimes := ex.statsCollector.PhaseTimes()

	// Collect the statistics.
	idleLatRaw := phaseTimes.GetIdleLatency(ex.statsCollector.PreviousPhaseTimes())
	idleLat := idleLatRaw.Seconds()
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

	fullScan := flags.IsSet(planFlagContainsFullIndexScan) || flags.IsSet(planFlagContainsFullTableScan)
	recordedStmtStatsKey := roachpb.StatementStatisticsKey{
		Query:        stmt.StmtNoConstants,
		QuerySummary: stmt.StmtSummary,
		DistSQL:      flags.IsDistributed(),
		Vec:          flags.IsSet(planFlagVectorized),
		ImplicitTxn:  flags.IsSet(planFlagImplicitTxn),
		FullScan:     fullScan,
		Failed:       stmtErr != nil,
		Database:     planner.SessionData().Database,
		PlanHash:     planner.instrumentation.planGist.Hash(),
	}

	idxRecommendations := idxrecommendations.FormatIdxRecommendations(planner.instrumentation.indexRecs)
	queryLevelStats, queryLevelStatsOk := planner.instrumentation.GetQueryLevelStats()

	// We only have node information when it was collected with trace, but we know at least the current
	// node should be on the list.
	nodeID, err := strconv.ParseInt(ex.server.sqlStats.GetSQLInstanceID().String(), 10, 64)
	if err != nil {
		log.Warningf(ctx, "failed to convert node ID to int: %s", err)
	}

	nodeIDs := util.CombineUniqueInt64(getNodesFromPlanner(planner), []int64{nodeID})
	regions := getRegionsForNodes(ctx, planner, nodeIDs)

	recordedStmtStats := sqlstats.RecordedStmtStats{
		SessionID:            ex.sessionID,
		StatementID:          stmt.QueryID,
		AutoRetryCount:       automaticRetryCount,
		AutoRetryReason:      ex.state.mu.autoRetryReason,
		RowsAffected:         rowsAffected,
		IdleLatency:          idleLat,
		ParseLatency:         parseLat,
		PlanLatency:          planLat,
		RunLatency:           runLat,
		ServiceLatency:       svcLat,
		OverheadLatency:      execOverhead,
		BytesRead:            stats.bytesRead,
		RowsRead:             stats.rowsRead,
		RowsWritten:          stats.rowsWritten,
		Nodes:                nodeIDs,
		Regions:              regions,
		StatementType:        stmt.AST.StatementType(),
		Plan:                 planner.instrumentation.PlanForStats(ctx),
		PlanGist:             planner.instrumentation.planGist.String(),
		StatementError:       stmtErr,
		IndexRecommendations: idxRecommendations,
		Query:                stmt.StmtNoConstants,
		StartTime:            phaseTimes.GetSessionPhaseTime(sessionphase.PlannerStartExecStmt),
		EndTime:              phaseTimes.GetSessionPhaseTime(sessionphase.PlannerStartExecStmt).Add(svcLatRaw),
		FullScan:             fullScan,
		ExecStats:            queryLevelStats,
		Indexes:              planner.instrumentation.indexesUsed,
		Database:             planner.SessionData().Database,
	}

	stmtFingerprintID, err :=
		ex.statsCollector.RecordStatement(ctx, recordedStmtStatsKey, recordedStmtStats)

	if err != nil {
		if log.V(1) {
			log.Warningf(ctx, "failed to record statement: %s", err)
		}
		ex.server.ServerMetrics.StatsMetrics.DiscardedStatsCount.Inc(1)
	}

	// Record statement execution statistics if span is recorded and no error was
	// encountered while collecting query-level statistics.
	if queryLevelStatsOk {
		for _, ev := range queryLevelStats.ContentionEvents {
			contentionEvent := contentionpb.ExtendedContentionEvent{
				BlockingEvent: ev,
				WaitingTxnID:  planner.txn.ID(),
			}

			ex.server.cfg.ContentionRegistry.AddContentionEvent(contentionEvent)
		}

		err = ex.statsCollector.RecordStatementExecStats(recordedStmtStatsKey, *queryLevelStats)
		if err != nil {
			if log.V(2 /* level */) {
				log.Warningf(ctx, "unable to record statement exec stats: %s", err)
			}
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
	ex.extraTxnState.idleLatency += idleLatRaw

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
	return stmtFingerprintID
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
	if _, ok := planner.instrumentation.Tracing(); !ok {
		trace := planner.instrumentation.sp.GetRecording(tracingpb.RecordingStructured)
		// ForEach returns nodes in order.
		execinfrapb.ExtractNodesFromSpans(trace).ForEach(func(i int) {
			nodes = append(nodes, int64(i))
		})
	}
	return nodes
}

var regionsPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]struct{})
	},
}

func getRegionsForNodes(ctx context.Context, planner *planner, nodeIDs []int64) []string {
	regions := regionsPool.Get().(map[string]struct{})
	defer func() {
		for region := range regions {
			delete(regions, region)
		}
		regionsPool.Put(regions)
	}()

	if planner.DistSQLPlanner().codec.ForSystemTenant() {
		// TODO(todd): I am not sure this is okay. ListNodesInternal makes
		//  a KV call to get the current list of nodes, which includes extra
		//  liveness information we don't need. Do we already have this
		//  information somewhere?
		status, err := planner.execCfg.NodesStatusServer.OptionalNodesStatusServer(47900)
		if err != nil {
			return nil
		}
		resp, err := status.ListNodesInternal(ctx, &serverpb.NodesRequest{})
		if err != nil {
			return nil
		}
		// Since we expect nodeIDs to be small, at worst say O(num(regions)),
		// we prefer this quick double for-loop to allocating a transient map
		// (or intsets.Fast) of them.
		for _, node := range resp.Nodes {
			for _, nodeID := range nodeIDs {
				// TODO(todd): Since roachpb.NodeID and base.SQLInstanceID
				//  are both aliases for int32, I think we've been sloppy
				//  using int64 here. I've filed #XXXXX to follow up.
				if int64(node.Desc.NodeID) == nodeID {
					if region, ok := node.Desc.Locality.Find("region"); ok {
						regions[region] = struct{}{}
					}
				}
			}
		}
	} else {
		// Note that, in contrast to the above questionable ListNodesInternal
		// call, this one should be fine: GetAllInstances is non-blocking,
		// completely served in-memory.
		instances, err := planner.DistSQLPlanner().sqlAddressResolver.GetAllInstances(ctx)
		if err != nil {
			return nil
		}
		// Since we expect nodeIDs to be small, at worst say O(num(regions)),
		// we prefer this quick double for-loop to allocating a transient map
		// (or intsets.Fast) of them.
		for _, instance := range instances {
			for _, nodeID := range nodeIDs {
				// TODO(todd): Since roachpb.NodeID and base.SQLInstanceID
				//  are both aliases for int32, I think we've been sloppy
				//  using int64 here. I've filed #95088 to follow up.
				if int64(instance.InstanceID) == nodeID {
					if region, ok := instance.Locality.Find("region"); ok {
						regions[region] = struct{}{}
					}
				}
			}
		}
	}

	result := make([]string, 0, len(regions))
	for region := range regions {
		result = append(result, region)
	}
	sort.Strings(result)
	return result
}
