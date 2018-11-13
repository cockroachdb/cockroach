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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

func projectCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// Project can pass through its ordering if the ordering depends only on
	// columns present in the input.
	return isOrderingBoundBy(expr.(*memo.ProjectExpr).Input, required)
}

func projectBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	if childIdx != 0 {
		return physical.OrderingChoice{}
	}

	// We may need to remove ordering columns that are not output by the input
	// expression.
	input := parent.(*memo.ProjectExpr).Input
	result := projectOrderingToInput(input, required)

	// Project can prune input columns, which can cause its FD set to be
	// pruned as well. Check the ordering to see if it can be simplified
	// with respect to the input FD set.
	fdSet := &input.Relational().FuncDeps
	if result.CanSimplify(fdSet) {
		result = result.Copy()
		result.Simplify(fdSet)
	}
	return result
}

// isOrderingBoundBy returns true if the given ordering can be satisfied using
// only the columns produced by input.
func isOrderingBoundBy(input memo.RelExpr, ordering *physical.OrderingChoice) bool {
	inputCols := input.Relational().OutputCols
	return ordering.CanProjectCols(inputCols)
}

// projectOrderingToInput projects out columns from an ordering (if necessary);
// can only be used if isOrderingBoundBy is true for the ordering. If projection
// is not necessary, returns a shallow copy of the ordering.
func projectOrderingToInput(
	input memo.RelExpr, ordering *physical.OrderingChoice,
) physical.OrderingChoice {
	childOutCols := input.Relational().OutputCols
	if ordering.SubsetOfCols(childOutCols) {
		return *ordering
	}
	result := ordering.Copy()
	result.ProjectCols(childOutCols)
	return result
}
