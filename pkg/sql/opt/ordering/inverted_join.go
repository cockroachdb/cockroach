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
