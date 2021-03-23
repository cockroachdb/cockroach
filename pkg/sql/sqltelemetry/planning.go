// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqltelemetry

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
)

// CteUseCounter is to be incremented every time a CTE (WITH ...)
// is planned without error in a query (this includes both recursive and
// non-recursive CTEs).
var CteUseCounter = telemetry.GetCounterOnce("sql.plan.cte")

// RecursiveCteUseCounter is to be incremented every time a recursive CTE (WITH
// RECURSIVE...) is planned without error in a query.
var RecursiveCteUseCounter = telemetry.GetCounterOnce("sql.plan.cte.recursive")

// SubqueryUseCounter is to be incremented every time a subquery is
// planned.
var SubqueryUseCounter = telemetry.GetCounterOnce("sql.plan.subquery")

// CorrelatedSubqueryUseCounter is to be incremented every time a
// correlated subquery has been processed during planning.
var CorrelatedSubqueryUseCounter = telemetry.GetCounterOnce("sql.plan.subquery.correlated")

// UniqueChecksUseCounter is to be incremented every time a mutation has
// unique checks and the checks are planned by the optimizer.
var UniqueChecksUseCounter = telemetry.GetCounterOnce("sql.plan.unique.checks")

// ForeignKeyChecksUseCounter is to be incremented every time a mutation has
// foreign key checks and the checks are planned by the optimizer.
var ForeignKeyChecksUseCounter = telemetry.GetCounterOnce("sql.plan.fk.checks")

// ForeignKeyCascadesUseCounter is to be incremented every time a mutation
// involves a cascade.
var ForeignKeyCascadesUseCounter = telemetry.GetCounterOnce("sql.plan.fk.cascades")

// LateralJoinUseCounter is to be incremented whenever a query uses the
// LATERAL keyword.
var LateralJoinUseCounter = telemetry.GetCounterOnce("sql.plan.lateral-join")

// HashJoinHintUseCounter is to be incremented whenever a query specifies a
// hash join via a query hint.
var HashJoinHintUseCounter = telemetry.GetCounterOnce("sql.plan.hints.hash-join")

// MergeJoinHintUseCounter is to be incremented whenever a query specifies a
// merge join via a query hint.
var MergeJoinHintUseCounter = telemetry.GetCounterOnce("sql.plan.hints.merge-join")

// LookupJoinHintUseCounter is to be incremented whenever a query specifies a
// lookup join via a query hint.
var LookupJoinHintUseCounter = telemetry.GetCounterOnce("sql.plan.hints.lookup-join")

// InvertedJoinHintUseCounter is to be incremented whenever a query specifies an
// inverted join via a query hint.
var InvertedJoinHintUseCounter = telemetry.GetCounterOnce("sql.plan.hints.inverted-join")

// IndexHintUseCounter is to be incremented whenever a query specifies an index
// hint. Incremented whenever one of the more specific variants below is
// incremented.
var IndexHintUseCounter = telemetry.GetCounterOnce("sql.plan.hints.index")

// IndexHintSelectUseCounter is to be incremented whenever a query specifies an
// index hint in a SELECT.
var IndexHintSelectUseCounter = telemetry.GetCounterOnce("sql.plan.hints.index.select")

// IndexHintUpdateUseCounter is to be incremented whenever a query specifies an
// index hint in an UPDATE.
var IndexHintUpdateUseCounter = telemetry.GetCounterOnce("sql.plan.hints.index.update")

// IndexHintDeleteUseCounter is to be incremented whenever a query specifies an
// index hint in a DELETE.
var IndexHintDeleteUseCounter = telemetry.GetCounterOnce("sql.plan.hints.index.delete")

// ExplainPlanUseCounter is to be incremented whenever vanilla EXPLAIN is run.
var ExplainPlanUseCounter = telemetry.GetCounterOnce("sql.plan.explain")

// ExplainDistSQLUseCounter is to be incremented whenever EXPLAIN (DISTSQL) is
// run.
var ExplainDistSQLUseCounter = telemetry.GetCounterOnce("sql.plan.explain-distsql")

// ExplainAnalyzeUseCounter is to be incremented whenever EXPLAIN ANALYZE is run.
var ExplainAnalyzeUseCounter = telemetry.GetCounterOnce("sql.plan.explain-analyze")

// ExplainAnalyzeDistSQLUseCounter is to be incremented whenever EXPLAIN ANALYZE
// (DISTSQL) is run.
var ExplainAnalyzeDistSQLUseCounter = telemetry.GetCounterOnce("sql.plan.explain-analyze-distsql")

// ExplainAnalyzeDebugUseCounter is to be incremented whenever
// EXPLAIN ANALYZE (DEBUG) is run.
var ExplainAnalyzeDebugUseCounter = telemetry.GetCounterOnce("sql.plan.explain-analyze-debug")

// ExplainOptUseCounter is to be incremented whenever EXPLAIN (OPT) is run.
var ExplainOptUseCounter = telemetry.GetCounterOnce("sql.plan.explain-opt")

// ExplainVecUseCounter is to be incremented whenever EXPLAIN (VEC) is run.
var ExplainVecUseCounter = telemetry.GetCounterOnce("sql.plan.explain-vec")

// ExplainDDLStages is to be incremented whenever EXPLAIN (DDL, STAGES) is run.
var ExplainDDLStages = telemetry.GetCounterOnce("sql.plan.explain-ddl-stages")

// ExplainDDLDeps is to be incremented whenever EXPLAIN (DDL, DEPS) is run.
var ExplainDDLDeps = telemetry.GetCounterOnce("sql.plan.explain-ddl-deps")

// ExplainOptVerboseUseCounter is to be incremented whenever
// EXPLAIN (OPT, VERBOSE) is run.
var ExplainOptVerboseUseCounter = telemetry.GetCounterOnce("sql.plan.explain-opt-verbose")

// CreateStatisticsUseCounter is to be incremented whenever a non-automatic
// run of CREATE STATISTICS occurs.
var CreateStatisticsUseCounter = telemetry.GetCounterOnce("sql.plan.stats.created")

// TurnAutoStatsOnUseCounter is to be incremented whenever automatic stats
// collection is explicitly enabled.
var TurnAutoStatsOnUseCounter = telemetry.GetCounterOnce("sql.plan.automatic-stats.enabled")

// TurnAutoStatsOffUseCounter is to be incremented whenever automatic stats
// collection is explicitly disabled.
var TurnAutoStatsOffUseCounter = telemetry.GetCounterOnce("sql.plan.automatic-stats.disabled")

// StatsHistogramOOMCounter is to be incremented whenever statistics histogram
// generation is disabled due to an out of memory error.
var StatsHistogramOOMCounter = telemetry.GetCounterOnce("sql.plan.stats.histogram-oom")

// JoinAlgoHashUseCounter is to be incremented whenever a hash join node is
// planned.
var JoinAlgoHashUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.algo.hash")

// JoinAlgoMergeUseCounter is to be incremented whenever a merge join node is
// planned.
var JoinAlgoMergeUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.algo.merge")

// JoinAlgoLookupUseCounter is to be incremented whenever a lookup join node is
// planned.
var JoinAlgoLookupUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.algo.lookup")

// JoinAlgoCrossUseCounter is to be incremented whenever a cross join node is
// planned.
var JoinAlgoCrossUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.algo.cross")

// JoinTypeInnerUseCounter is to be incremented whenever an inner join node is
// planned.
var JoinTypeInnerUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.type.inner")

// JoinTypeLeftUseCounter is to be incremented whenever a left or right outer
// join node is planned.
var JoinTypeLeftUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.type.left-outer")

// JoinTypeFullUseCounter is to be incremented whenever a full outer join node is
// planned.
var JoinTypeFullUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.type.full-outer")

// JoinTypeSemiUseCounter is to be incremented whenever a semi-join node is
// planned.
var JoinTypeSemiUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.type.semi")

// JoinTypeAntiUseCounter is to be incremented whenever an anti-join node is
// planned.
var JoinTypeAntiUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.type.anti")

// PartialIndexScanUseCounter is to be incremented whenever a partial index scan
// node is planned.
var PartialIndexScanUseCounter = telemetry.GetCounterOnce("sql.plan.opt.partial-index.scan")

// PartialIndexLookupJoinUseCounter is to be incremented whenever a lookup join
// on a partial index is planned.
var PartialIndexLookupJoinUseCounter = telemetry.GetCounterOnce("sql.plan.opt.partial-index.lookup-join")

// LocalityOptimizedSearchUseCounter is to be incremented whenever a locality
// optimized search node is planned.
var LocalityOptimizedSearchUseCounter = telemetry.GetCounterOnce("sql.plan.opt.locality-optimized-search")

// CancelQueriesUseCounter is to be incremented whenever CANCEL QUERY or
// CANCEL QUERIES is run.
var CancelQueriesUseCounter = telemetry.GetCounterOnce("sql.session.cancel-queries")

// CancelSessionsUseCounter is to be incremented whenever CANCEL SESSION or
// CANCEL SESSIONS is run.
var CancelSessionsUseCounter = telemetry.GetCounterOnce("sql.session.cancel-sessions")

// We can't parameterize these telemetry counters, so just make a bunch of
// buckets for setting the join reorder limit since the range of reasonable
// values for the join reorder limit is quite small.
// reorderJoinLimitUseCounters is a list of counters. The entry at position i
// is the counter for SET reorder_join_limit = i.
var reorderJoinLimitUseCounters []telemetry.Counter

const reorderJoinsCounters = 12

func init() {
	reorderJoinLimitUseCounters = make([]telemetry.Counter, reorderJoinsCounters)

	for i := 0; i < reorderJoinsCounters; i++ {
		reorderJoinLimitUseCounters[i] = telemetry.GetCounterOnce(
			fmt.Sprintf("sql.plan.reorder-joins.set-limit-%d", i),
		)
	}
}

// ReorderJoinLimitMoreCounter is the counter for the number of times someone
// set the join reorder limit above reorderJoinsCounters.
var reorderJoinLimitMoreCounter = telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-more")

// ReportJoinReorderLimit is to be called whenever the reorder joins session variable
// is set.
func ReportJoinReorderLimit(value int) {
	if value < reorderJoinsCounters {
		telemetry.Inc(reorderJoinLimitUseCounters[value])
	} else {
		telemetry.Inc(reorderJoinLimitMoreCounter)
	}
}

// WindowFunctionCounter is to be incremented every time a window function is
// being planned.
func WindowFunctionCounter(wf string) telemetry.Counter {
	return telemetry.GetCounter("sql.plan.window_function." + wf)
}

// OptNodeCounter should be incremented every time a node of the given
// type is encountered at the end of the query optimization (i.e. it
// counts the nodes actually used for physical planning).
func OptNodeCounter(nodeType string) telemetry.Counter {
	return telemetry.GetCounterOnce(fmt.Sprintf("sql.plan.opt.node.%s", nodeType))
}
