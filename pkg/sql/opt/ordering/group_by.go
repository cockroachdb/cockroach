// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

func scalarGroupByBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	if childIdx != 0 {
		return props.OrderingChoice{}
	}
	// Scalar group by requires the ordering in its private.
	return parent.(*memo.ScalarGroupByExpr).Ordering
}

func groupByCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// GroupBy may require a certain ordering of its input, but can also pass
	// through a stronger ordering on the grouping columns.
	groupBy := expr.(*memo.GroupByExpr)
	return required.CanProjectCols(groupBy.GroupingCols) && required.Intersects(&groupBy.Ordering)
}

func groupByBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	if childIdx != 0 {
		return props.OrderingChoice{}
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

func groupByBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	groupBy := expr.(*memo.GroupByExpr)
	provided := groupBy.Input.ProvidedPhysical().Ordering
	inputFDs := &groupBy.Input.Relational().FuncDeps

	// Since the input's provided ordering has to satisfy both <required> and the
	// GroupBy internal ordering, it may need to be trimmed.
	provided = trimProvided(provided, required, inputFDs)
	return remapProvided(provided, inputFDs, groupBy.GroupingCols)
}

func distinctOnCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// DistinctOn may require a certain ordering of its input, but can also pass
	// through a stronger ordering on the grouping columns.
	return required.Intersects(&expr.Private().(*memo.GroupingPrivate).Ordering)
}

func distinctOnBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	if childIdx != 0 {
		return props.OrderingChoice{}
	}
	// The FD set of the input doesn't "pass through" to the DistinctOn FD set;
	// check the ordering to see if it can be simplified with respect to the input
	// FD set.
	result := required.Intersection(&parent.Private().(*memo.GroupingPrivate).Ordering)
	result.Simplify(&parent.Child(0).(memo.RelExpr).Relational().FuncDeps)
	return result
}

func distinctOnBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	input := expr.Child(0).(memo.RelExpr)
	provided := input.ProvidedPhysical().Ordering
	inputFDs := &input.Relational().FuncDeps
	// Since the input's provided ordering has to satisfy both <required> and the
	// DistinctOn internal ordering, it may need to be trimmed.
	provided = trimProvided(provided, required, inputFDs)
	return remapProvided(provided, inputFDs, expr.Relational().OutputCols)
}

// StreamingGroupingColOrdering returns an ordering on grouping columns that is
// guaranteed on the input of an aggregation operator. This ordering can be used
// perform a streaming aggregation.
func StreamingGroupingColOrdering(
	g *memo.GroupingPrivate, required *props.OrderingChoice,
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
		ordering[i] = opt.MakeOrderingColumn(colID, inputOrdering.Columns[i].Descending)
	}
	return ordering
}
