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

func projectBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	p := expr.(*memo.ProjectExpr)
	// Project can only satisfy required orderings that refer to pass-through
	// columns; it should always be possible to remap the columns in the input's
	// provided ordering.
	return remapProvided(
		p.Input.ProvidedPhysical().Ordering,
		&p.Input.Relational().FuncDeps,
		p.Passthrough,
	)
}
