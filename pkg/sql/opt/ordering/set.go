// Copyright 2021 The Cockroach Authors.
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

func setOpCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// Set operations can provide any ordering by requiring that both inputs have
	// the same ordering.
	return true
}

func setOpBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	private := parent.Private().(*memo.SetPrivate)
	var childReq physical.OrderingChoice
	switch childIdx {
	case 0:
		childReq = required.RemapColumns(private.OutCols, private.LeftCols)

	case 1:
		childReq = required.RemapColumns(private.OutCols, private.RightCols)

	default:
		return physical.OrderingChoice{}
	}

	// Try to simplify the required ordering in case some of the ordering columns
	// are constant in the input.
	fds := &parent.Child(childIdx).(memo.RelExpr).Relational().FuncDeps
	if childReq.CanSimplify(fds) {
		childReq.Simplify(fds)
	}
	return childReq
}

func setOpBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	// Set operations can always provide the required ordering. Don't use the
	// provided ordering from the inputs in case they were trimmed to remove
	// constant columns. Call remapProvided to remove columns that are now
	// unnecessary (e.g. because the set op is guaranteed to provide at most one
	// row).
	rel := expr.Relational()
	return remapProvided(required.ToOrdering(), &rel.FuncDeps, rel.OutputCols)
}
