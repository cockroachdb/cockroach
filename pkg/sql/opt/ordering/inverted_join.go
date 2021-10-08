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

func invertedJoinCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// InvertedJoin can pass through its ordering if the ordering
	// depends only on columns present in the input.
	inputCols := expr.Child(0).(memo.RelExpr).Relational().OutputCols
	return required.CanProjectCols(inputCols)
}

func invertedJoinBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	if childIdx != 0 {
		return props.OrderingChoice{}
	}

	// We may need to remove ordering columns that are not output by the input
	// expression.
	child := parent.Child(0).(memo.RelExpr)
	res := projectOrderingToInput(child, required)
	// It is in principle possible that the inverted join has an ON condition that
	// forces an equality on two columns in the input. In this case we need to
	// trim the column groups to keep the ordering valid w.r.t the child FDs
	// (similar to Select).
	//
	// This case indicates that we didn't do a good job pushing down equalities
	// (see #36219), but it should be handled correctly here nevertheless.
	return trimColumnGroups(&res, &child.Relational().FuncDeps)
}

func invertedJoinBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	invertedJoin := expr.(*memo.InvertedJoinExpr)
	childProvided := invertedJoin.Input.ProvidedPhysical().Ordering

	// The inverted join includes an implicit projection (invertedJoin.Cols);
	// some of the input columns might not be output columns so we may need to
	// remap them. First check if we need to.
	needsRemap := false
	for i := range childProvided {
		if !invertedJoin.Cols.Contains(childProvided[i].ID()) {
			needsRemap = true
			break
		}
	}
	if !needsRemap {
		// Fast path: we don't need to remap any columns.
		return childProvided
	}

	// Because of the implicit projection, the FDs of the InvertedJoin don't
	// include all the columns we care about; we have to recreate the FDs of
	// the join before the projection. These are the FDs of the input plus any
	// equality constraints in the ON condition.
	var fds props.FuncDepSet
	fds.CopyFrom(&invertedJoin.Input.Relational().FuncDeps)
	for i := range invertedJoin.On {
		fds.AddEquivFrom(&invertedJoin.On[i].ScalarProps().FuncDeps)
	}

	return remapProvided(childProvided, &fds, invertedJoin.Cols)
}
