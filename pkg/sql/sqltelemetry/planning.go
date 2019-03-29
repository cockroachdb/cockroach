// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqltelemetry

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
)

// CteUseCounter is to be incremented every time a CTE (WITH ...)
// is planned without error in a query.
var CteUseCounter = telemetry.GetCounterOnce("sql.plan.cte")

// SubqueryUseCounter is to be incremented every time a subquery is
// planned.
var SubqueryUseCounter = telemetry.GetCounterOnce("sql.plan.subquery")

// CorrelatedSubqueryUseCounter is to be incremented every time a
// correlated subquery has been processed during planning.
var CorrelatedSubqueryUseCounter = telemetry.GetCounterOnce("sql.plan.subquery.correlated")

// HashJoinHintUseCounter is to be incremented whenever a query specifies a
// hash join via a query hint.
var HashJoinHintUseCounter = telemetry.GetCounterOnce("sql.plan.hints.hash-join")

// MergeJoinHintUseCounter is to be incremented whenever a query specifies a
// merge join via a query hint.
var MergeJoinHintUseCounter = telemetry.GetCounterOnce("sql.plan.hints.merge-join")

// LookupJoinHintUseCounter is to be incremented whenever a query specifies a
// lookup join via a query hint.
var LookupJoinHintUseCounter = telemetry.GetCounterOnce("sql.plan.hints.lookup-join")

// IndexHintUseCounter is to be incremented whenever a query specifies an index
// hint.
var IndexHintUseCounter = telemetry.GetCounterOnce("sql.plan.hints.index")

// ExplainPlanUseCounter is to be incremented whenever vanilla EXPLAIN is run.
var ExplainPlanUseCounter = telemetry.GetCounterOnce("sql.plan.explain")

// ExplainDistSQLUseCounter is to be incremented whenever EXPLAIN (DISTSQL) is
// run.
var ExplainDistSQLUseCounter = telemetry.GetCounterOnce("sql.plan.explain-distsql")

// ExplainAnalyzeUseCounter is to be incremented whenever EXPLAIN ANALYZE is run.
var ExplainAnalyzeUseCounter = telemetry.GetCounterOnce("sql.plan.explain-analyze")

// ExplainOptUseCounter is to be incremented whenever EXPLAIN (OPT) is run.
var ExplainOptUseCounter = telemetry.GetCounterOnce("sql.plan.explain-opt")

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
