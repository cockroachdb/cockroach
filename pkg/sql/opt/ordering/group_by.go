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

func distinctOnCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// DistinctOn may require a certain ordering of its input, but can also pass
	// through a stronger ordering on the grouping columns.
	return required.Intersects(&expr.(*memo.DistinctOnExpr).Ordering)
}

func distinctOnBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	if childIdx != 0 {
		return props.OrderingChoice{}
	}
	return required.Intersection(&parent.(*memo.DistinctOnExpr).Ordering)
}
