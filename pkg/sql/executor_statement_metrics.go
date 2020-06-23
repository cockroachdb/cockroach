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

	// sessionNumPhases must be listed last so that it can be used to
	// define arrays sufficiently large to hold all the other values.
	sessionNumPhases
)

// phaseTimes is the type of the session.phaseTimes array.
//
// It's important that this is an array and not a slice, as we rely on the array
// copy behavior.
type phaseTimes [sessionNumPhases]time.Time

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

	// TxnAbortCount counts transactions that were aborted, either due
	// to non-retriable errors, or retriable errors when the client-side
	// retry protocol is not in use.
	TxnAbortCount *metric.Counter

	// FailureCount counts non-retriable errors in open transactions.
	FailureCount *metric.Counter
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
	bytesRead int64,
	rowsRead int64,
) {
	phaseTimes := &ex.statsCollector.phaseTimes

	// Compute the run latency. This is always recorded in the
	// server metrics.
	runLatRaw := phaseTimes[plannerEndExecStmt].Sub(phaseTimes[plannerStartExecStmt])

	// Collect the statistics.
	runLat := runLatRaw.Seconds()

	parseLat := phaseTimes[sessionEndParse].
		Sub(phaseTimes[sessionStartParse]).Seconds()
	planLat := phaseTimes[plannerEndLogicalPlan].
		Sub(phaseTimes[plannerStartLogicalPlan]).Seconds()
	// service latency: time query received to end of run
	svcLatRaw := phaseTimes[plannerEndExecStmt].Sub(phaseTimes[sessionQueryReceived])
	svcLat := svcLatRaw.Seconds()

	// processing latency: contributing towards SQL results.
	processingLat := parseLat + planLat + runLat

	// overhead latency: txn/retry management, error checking, etc
	execOverhead := svcLat - processingLat

	stmt := planner.stmt
	flags := planner.curPlan.flags
	if automaticRetryCount == 0 {
		ex.updateOptCounters(flags)
		m := &ex.metrics.EngineMetrics
		if flags.IsSet(planFlagDistributed) {
			if _, ok := stmt.AST.(*tree.Select); ok {
				m.DistSQLSelectCount.Inc(1)
			}
			m.DistSQLExecLatency.RecordValue(runLatRaw.Nanoseconds())
			m.DistSQLServiceLatency.RecordValue(svcLatRaw.Nanoseconds())
		}
		m.SQLExecLatency.RecordValue(runLatRaw.Nanoseconds())
		m.SQLServiceLatency.RecordValue(svcLatRaw.Nanoseconds())
	}

	ex.statsCollector.recordStatement(
		stmt, planner.curPlan.instrumentation.savedPlanForStats,
		flags.IsSet(planFlagDistributed), flags.IsSet(planFlagVectorized),
		flags.IsSet(planFlagImplicitTxn), automaticRetryCount, rowsAffected, err,
		parseLat, planLat, runLat, svcLat, execOverhead, bytesRead, rowsRead,
	)

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
