// Copyright 2022 The Cockroach Authors.
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

func projectSetCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// ProjectSet can pass through its ordering if the ordering depends only on
	// columns present in the input.
	prjs := expr.(*memo.ProjectSetExpr)
	inputCols := prjs.Input.Relational().OutputCols

	// Use a simplified ordering if it exists. This must be kept consistent with
	// projectSetBuildChildReqOrdering, which always simplifies the ordering if
	// possible.
	simplified := *required
	if fdSet := prjs.InternalFDs(); simplified.CanSimplify(fdSet) {
		simplified = required.Copy()
		simplified.Simplify(fdSet)
	}
	return simplified.CanProjectCols(inputCols)
}

func projectSetBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	if childIdx != 0 {
		return props.OrderingChoice{}
	}

	// ProjectSet can prune input columns, which can cause its FD set to be pruned
	// as well. Check the ordering to see if it can be simplified with respect to
	// the internal FD set.
	prjs := parent.(*memo.ProjectSetExpr)
	simplified := *required
	if fdSet := prjs.InternalFDs(); simplified.CanSimplify(fdSet) {
		simplified = simplified.Copy()
		simplified.Simplify(fdSet)
	}

	// We may need to remove ordering columns that are not output by the input
	// expression.
	result := projectOrderingToInput(prjs.Input, &simplified)

	return result
}

func projectSetBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	prjs := expr.(*memo.ProjectSetExpr)
	// ProjectSet can only satisfy required orderings that refer to projected
	// columns; it should always be possible to remap the columns in the input's
	// provided ordering.
	return remapProvided(
		prjs.Input.ProvidedPhysical().Ordering,
		prjs.InternalFDs(),
		prjs.Relational().OutputCols,
	)
}
