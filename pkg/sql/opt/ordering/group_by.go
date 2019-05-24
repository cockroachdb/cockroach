// Copyright 2018 The Cockroach Authors.
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

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

func scalarGroupByBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	if childIdx != 0 {
		return physical.OrderingChoice{}
	}
	// Scalar group by requires the ordering in its private.
	return parent.(*memo.ScalarGroupByExpr).Ordering
}

func groupByCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// GroupBy may require a certain ordering of its input, but can also pass
	// through a stronger ordering on the grouping columns.
	groupBy := expr.(*memo.GroupByExpr)
	return required.CanProjectCols(groupBy.GroupingCols) && required.Intersects(&groupBy.Ordering)
}

func groupByBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	if childIdx != 0 {
		return physical.OrderingChoice{}
	}
	groupBy := parent.(*memo.GroupByExpr)
	result := *required
	if !result.SubsetOfCols(groupBy.GroupingCols) {
		result = result.Copy()
		result.ProjectCols(groupBy.GroupingCols)
	}

	result = result.Intersection(&groupBy.Ordering)

	// The FD set of the input doesn't "pass through" to the GroupBy FD set;
	// check the ordering to see if it can be simplified with respect to the
	// input FD set.
	result.Simplify(&groupBy.Input.Relational().FuncDeps)

	return result
}

func groupByBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	groupBy := expr.(*memo.GroupByExpr)
	provided := groupBy.Input.ProvidedPhysical().Ordering
	inputFDs := &groupBy.Input.Relational().FuncDeps

	// GroupBy can only provide orderings on grouping columns. We retain the
	// longest prefix of grouping columns (or columns equivalent to any of them).
	groupingCols := inputFDs.ComputeEquivClosure(groupBy.GroupingCols)
	for i := range provided {
		if !groupingCols.Contains(int(provided[i].ID())) {
			provided = provided[:i]
			break
		}
	}
	provided = remapProvided(provided, inputFDs, groupBy.GroupingCols)
	// Since the input's provided ordering has to satisfy both <required> and the
	// GroupBy internal ordering, it may need to be trimmed.
	return trimProvided(provided, required, &expr.Relational().FuncDeps)
}

func distinctOnCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// DistinctOn may require a certain ordering of its input, but can also pass
	// through a stronger ordering on the grouping columns.
	return required.Intersects(&expr.(*memo.DistinctOnExpr).Ordering)
}

func distinctOnBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	if childIdx != 0 {
		return physical.OrderingChoice{}
	}
	return required.Intersection(&parent.(*memo.DistinctOnExpr).Ordering)
}

func distinctOnBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	// The input's provided ordering satisfies both <required> and the DistinctOn
	// internal ordering; it may need to be trimmed.
	d := expr.(*memo.DistinctOnExpr)
	return trimProvided(d.Input.ProvidedPhysical().Ordering, required, &d.Relational().FuncDeps)
}

// StreamingGroupingColOrdering returns an ordering on grouping columns that is
// guaranteed on the input of an aggregation operator. This ordering can be used
// perform a streaming aggregation.
func StreamingGroupingColOrdering(
	g *memo.GroupingPrivate, required *physical.OrderingChoice,
) opt.Ordering {
	inputOrdering := required.Intersection(&g.Ordering)
	ordering := make(opt.Ordering, len(inputOrdering.Columns))
	for i := range inputOrdering.Columns {
		// Get any grouping column from the set. Normally there would be at most one
		// because we have rules that remove redundant grouping columns.
		cols := inputOrdering.Columns[i].Group.Intersection(g.GroupingCols)
		colID, ok := cols.Next(0)
		if !ok {
			// This group refers to a column that is not a grouping column.
			// The rest of the ordering is not useful.
			return ordering[:i]
		}
		ordering[i] = opt.MakeOrderingColumn(opt.ColumnID(colID), inputOrdering.Columns[i].Descending)
	}
	return ordering
}
