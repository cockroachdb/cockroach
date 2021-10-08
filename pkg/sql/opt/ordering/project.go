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

func projectCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// Project can pass through its ordering if the ordering depends only on
	// columns present in the input.
	proj := expr.(*memo.ProjectExpr)
	inputCols := proj.Input.Relational().OutputCols

	// Use a simplified ordering if it exists. This must be kept consistent with
	// projectBuildChildReqOrdering, which always simplifies the ordering if
	// possible.
	simplified := *required
	if fdSet := proj.InternalFDs(); simplified.CanSimplify(fdSet) {
		simplified = required.Copy()
		simplified.Simplify(fdSet)
	}
	return simplified.CanProjectCols(inputCols)
}

func projectBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	if childIdx != 0 {
		return props.OrderingChoice{}
	}

	// Project can prune input columns, which can cause its FD set to be
	// pruned as well. Check the ordering to see if it can be simplified
	// with respect to the internal FD set.
	proj := parent.(*memo.ProjectExpr)
	simplified := *required
	if fdSet := proj.InternalFDs(); simplified.CanSimplify(fdSet) {
		simplified = simplified.Copy()
		simplified.Simplify(fdSet)
	}

	// We may need to remove ordering columns that are not output by the input
	// expression.
	result := projectOrderingToInput(proj.Input, &simplified)

	return result
}

// projectOrderingToInput projects out columns from an ordering (if necessary);
// can only be used if the ordering can be expressed in terms of the input
// columns. If projection is not necessary, returns a shallow copy of the
// ordering.
func projectOrderingToInput(
	input memo.RelExpr, ordering *props.OrderingChoice,
) props.OrderingChoice {
	childOutCols := input.Relational().OutputCols
	if ordering.SubsetOfCols(childOutCols) {
		return *ordering
	}
	result := ordering.Copy()
	result.ProjectCols(childOutCols)
	return result
}

func projectBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	p := expr.(*memo.ProjectExpr)
	// Project can only satisfy required orderings that refer to projected
	// columns; it should always be possible to remap the columns in the input's
	// provided ordering.
	return remapProvided(
		p.Input.ProvidedPhysical().Ordering,
		p.InternalFDs(),
		p.Relational().OutputCols,
	)
}
