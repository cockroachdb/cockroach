// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// joinerBase is the common core of all joiners.
type joinerBase struct {
	execinfra.ProcessorBase

	joinType    descpb.JoinType
	onCond      execinfrapb.ExprHelper
	emptyLeft   rowenc.EncDatumRow
	emptyRight  rowenc.EncDatumRow
	combinedRow rowenc.EncDatumRow

	// EqCols contains the indices of the columns that are constrained to be
	// equal. Specifically column EqCols[0][i] on the left side must match the
	// column EqCols[1][i] on the right side.
	eqCols [2][]uint32
}

// Init initializes the joinerBase.
//
// opts is passed along to the underlying ProcessorBase. The zero value is used
// if the processor using the joinerBase is not implementing RowSource.
func (jb *joinerBase) init(
	self execinfra.RowSource,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	leftTypes []*types.T,
	rightTypes []*types.T,
	jType descpb.JoinType,
	onExpr execinfrapb.Expression,
	leftEqColumns []uint32,
	rightEqColumns []uint32,
	outputContinuationColumn bool,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
	opts execinfra.ProcStateOpts,
) error {
	jb.joinType = jType

	if jb.joinType.IsSetOpJoin() {
		if !onExpr.Empty() {
			return errors.Errorf("expected empty onExpr, got %v", onExpr)
		}
	}

	jb.emptyLeft = make(rowenc.EncDatumRow, len(leftTypes))
	for i := range jb.emptyLeft {
		jb.emptyLeft[i] = rowenc.DatumToEncDatum(leftTypes[i], tree.DNull)
	}
	jb.emptyRight = make(rowenc.EncDatumRow, len(rightTypes))
	for i := range jb.emptyRight {
		jb.emptyRight[i] = rowenc.DatumToEncDatum(rightTypes[i], tree.DNull)
	}

	jb.eqCols[leftSide] = leftEqColumns
	jb.eqCols[rightSide] = rightEqColumns

	rowSize := len(leftTypes) + len(rightTypes)
	if outputContinuationColumn {
		// NB: Can only be true for inner joins and left outer joins.
		rowSize++
	}
	jb.combinedRow = make(rowenc.EncDatumRow, rowSize)

	onCondTypes := make([]*types.T, 0, len(leftTypes)+len(rightTypes))
	onCondTypes = append(onCondTypes, leftTypes...)
	onCondTypes = append(onCondTypes, rightTypes...)

	outputTypes := jType.MakeOutputTypes(leftTypes, rightTypes)
	if outputContinuationColumn {
		outputTypes = append(outputTypes, types.Bool)
	}

	if err := jb.ProcessorBase.Init(
		self, post, outputTypes, flowCtx, processorID, output, nil /* memMonitor */, opts,
	); err != nil {
		return err
	}
	semaCtx := flowCtx.TypeResolverFactory.NewSemaContext(flowCtx.EvalCtx.Txn)
	return jb.onCond.Init(onExpr, onCondTypes, semaCtx, jb.EvalCtx)
}

// joinSide is the utility type to distinguish between two sides of the join.
type joinSide uint8

const (
	// leftSide indicates the left side of the join.
	leftSide joinSide = 0
	// rightSide indicates the right side of the join.
	rightSide joinSide = 1
)

func (j joinSide) String() string {
	if j == leftSide {
		return "left"
	}
	return "right"
}

// renderUnmatchedRow creates a result row given an unmatched row on either
// side. Only used for outer and anti joins. Note that if the join is outputting
// a continuation column, the returned slice does not include the continuation
// column, but has the capacity for it.
func (jb *joinerBase) renderUnmatchedRow(row rowenc.EncDatumRow, side joinSide) rowenc.EncDatumRow {
	lrow, rrow := jb.emptyLeft, jb.emptyRight
	if side == leftSide {
		lrow = row
	} else {
		rrow = row
	}

	return jb.renderForOutput(lrow, rrow)
}

// shouldEmitUnmatchedRow determines if we should emit an unmatched row (with
// NULLs for the columns of the other stream). This happens in FULL OUTER joins
// and LEFT or RIGHT OUTER joins and ANTI joins (depending on which stream is
// stored).
func shouldEmitUnmatchedRow(side joinSide, joinType descpb.JoinType) bool {
	switch joinType {
	case descpb.InnerJoin, descpb.LeftSemiJoin, descpb.RightSemiJoin, descpb.IntersectAllJoin:
		return false
	case descpb.RightOuterJoin:
		return side == rightSide
	case descpb.LeftOuterJoin:
		return side == leftSide
	case descpb.LeftAntiJoin:
		return side == leftSide
	case descpb.RightAntiJoin:
		return side == rightSide
	case descpb.ExceptAllJoin:
		return side == leftSide
	case descpb.FullOuterJoin:
		return true
	default:
		panic(errors.AssertionFailedf("unexpected join type %s", joinType))
	}
}

// render constructs a row according to the join type (for semi/anti and set-op
// joins only the columns of one side are included). The ON condition is
// evaluated; if it fails, returns nil. Note that if the join is outputting a
// continuation column, the returned slice does not include the continuation
// column, but has the capacity for it.
func (jb *joinerBase) render(lrow, rrow rowenc.EncDatumRow) (rowenc.EncDatumRow, error) {
	outputRow := jb.renderForOutput(lrow, rrow)

	if jb.onCond.Expr != nil {
		// We need to evaluate the ON condition which can refer to the columns
		// from both sides of the join regardless of the join type, so we need
		// to have the combined row.
		var combinedRow rowenc.EncDatumRow
		if jb.joinType.ShouldIncludeLeftColsInOutput() && jb.joinType.ShouldIncludeRightColsInOutput() {
			// When columns from both sides are needed in the output, we can
			// just reuse the output row since it is the combined row too.
			combinedRow = outputRow
		} else {
			combinedRow = jb.combine(lrow, rrow)
		}
		res, err := jb.onCond.EvalFilter(combinedRow)
		if !res || err != nil {
			return nil, err
		}
	}

	return outputRow, nil
}

// combine combines lrow and rrow together.
func (jb *joinerBase) combine(lrow, rrow rowenc.EncDatumRow) rowenc.EncDatumRow {
	jb.combinedRow = jb.combinedRow[:len(lrow)+len(rrow)]
	// If either of the rows is of length 1, it is faster to use direct
	// assignment instead of copy.
	if len(lrow) == 1 {
		jb.combinedRow[0] = lrow[0]
	} else {
		copy(jb.combinedRow, lrow)
	}
	if len(rrow) == 1 {
		jb.combinedRow[len(lrow)] = rrow[0]
	} else {
		copy(jb.combinedRow[len(lrow):], rrow)
	}
	return jb.combinedRow
}

// renderForOutput combines lrow and rrow together depending on the join type
// and returns the row that can be used as the output of the join.
func (jb *joinerBase) renderForOutput(lrow, rrow rowenc.EncDatumRow) rowenc.EncDatumRow {
	if !jb.joinType.ShouldIncludeLeftColsInOutput() {
		return rrow
	}
	if !jb.joinType.ShouldIncludeRightColsInOutput() {
		return lrow
	}
	return jb.combine(lrow, rrow)
}
