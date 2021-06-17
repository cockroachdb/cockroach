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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

func setOpCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// Set operations can provide the required ordering if it intersects with the
	// set private ordering.
	private := expr.Private().(*memo.SetPrivate)
	return required.Intersects(&private.Ordering)
}

func setOpBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	if childIdx != 0 && childIdx != 1 {
		return props.OrderingChoice{}
	}

	required = setOpBuildRequired(parent, required)
	private := parent.Private().(*memo.SetPrivate)
	var childReq props.OrderingChoice
	switch childIdx {
	case 0:
		childReq = required.RemapColumns(private.OutCols, private.LeftCols)

	case 1:
		childReq = required.RemapColumns(private.OutCols, private.RightCols)

	default:
		return props.OrderingChoice{}
	}

	// Try to simplify the required ordering in case some of the ordering columns
	// are constant in the input.
	fds := &parent.Child(childIdx).(memo.RelExpr).Relational().FuncDeps
	if childReq.CanSimplify(fds) {
		childReq.Simplify(fds)
	}
	return childReq
}

func setOpBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	// Don't use the provided ordering from the inputs in case they were trimmed
	// to remove constant columns. Call remapProvided to remove columns that are
	// now unnecessary (e.g. because the set op is guaranteed to produce at most
	// one row).
	rel := expr.Relational()
	return remapProvided(required.ToOrdering(), &rel.FuncDeps, rel.OutputCols)
}

// setOpBuildRequired pads the required ordering if needed to ensure that it
// includes all output columns of the set operation. This is necessary because
// the execution engine can only use a streaming (merge join or distinct)
// operation if the ordering involves all columns.
func setOpBuildRequired(expr memo.RelExpr, required *props.OrderingChoice) *props.OrderingChoice {
	private := expr.Private().(*memo.SetPrivate)
	if required.Any() {
		return &private.Ordering
	}

	result := required.Intersection(&private.Ordering)
	fds := &expr.Relational().FuncDeps
	if result.CanSimplify(fds) {
		result.Simplify(fds)
	}

	// UNION ALL is implemented with only an ordered synchronizer, so there is no
	// need to add extra ordering columns.
	if expr.Op() == opt.UnionAllOp {
		return &result
	}

	// If required includes some columns but not all, add the remaining columns in
	// an arbitrary (but deterministic) order.
	missing := expr.Relational().OutputCols.Difference(result.ColSet())
	if !missing.Empty() {
		missing.ForEach(func(col opt.ColumnID) {
			result.AppendCol(col, false /* descending */)
		})
		if result.CanSimplify(fds) {
			result.Simplify(fds)
		}
	}

	return &result
}

// StreamingSetOpOrdering returns an ordering on the set operation output
// columns that is guaranteed on both inputs. This ordering can be used to
// perform a streaming set operation.
func StreamingSetOpOrdering(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	required = setOpBuildRequired(expr, required)
	ordering := required.ToOrdering()
	if ordering.Empty() {
		return ordering
	}

	// UNION ALL is implemented with only an ordered synchronizer, so there is no
	// need to add extra ordering columns.
	if expr.Op() == opt.UnionAllOp {
		return ordering
	}

	// Pad the ordering to make sure every column is accounted for in the
	// ordering. This won't change the order of data (setOpBuildRequired already
	// ensured the required ordering was fully specified according to the FDs),
	// but it's necessary for the execution engine to plan a streaming operation.
	// TODO(rytaft): Consider changing the execution engine to accept the
	// optimizer's decision to plan a streaming operation even if all columns are
	// not included in the ordering.
	missing := expr.Relational().OutputCols.Difference(ordering.ColSet())
	missing.ForEach(func(col opt.ColumnID) {
		ordering = append(ordering, opt.MakeOrderingColumn(col, false /* descending */))
	})
	return ordering
}
