// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// joinerBase is the common core of all joiners.
type joinerBase struct {
	execinfra.ProcessorBase

	joinType descpb.JoinType
	onCond   execinfrapb.ExprHelper
	// combinedRow has enough space for both the left and right input rows even
	// if only columns from one side of the join are included in the output.
	// This allows storing values for columns referenced in the ON condition,
	// which can originate from either side of the join. combinedRow also
	// includes space for the value of the continuation column, if necessary.
	combinedRow rowenc.EncDatumRow
	// outputSize is the number of columns in the output row, not including the
	// continuation column. It is only equal to the length of combinedRow when
	// both the left and right columns are included in the output, and when
	// there is no continuation column.
	outputSize int
}

// Init initializes the joinerBase.
//
// opts is passed along to the underlying ProcessorBase. The zero value is used
// if the processor using the joinerBase is not implementing RowSource.
func (jb *joinerBase) init(
	ctx context.Context,
	self execinfra.RowSource,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	leftTypes []*types.T,
	rightTypes []*types.T,
	jType descpb.JoinType,
	onExpr execinfrapb.Expression,
	outputContinuationColumn bool,
	post *execinfrapb.PostProcessSpec,
	opts execinfra.ProcStateOpts,
) (*eval.Context, error) {
	jb.joinType = jType

	if jb.joinType.IsSetOpJoin() {
		if !onExpr.Empty() {
			return nil, errors.AssertionFailedf("expected empty onExpr, got %v", onExpr)
		}
	}

	// Allocate enough space to fit the left row, right row, and continuation
	// column.
	combinedRowSize := len(leftTypes) + len(rightTypes)
	if outputContinuationColumn {
		// NB: Can only be true for inner joins and left outer joins.
		combinedRowSize++
	}
	jb.combinedRow = make(rowenc.EncDatumRow, combinedRowSize)

	onCondTypes := make([]*types.T, 0, len(leftTypes)+len(rightTypes))
	onCondTypes = append(onCondTypes, leftTypes...)
	onCondTypes = append(onCondTypes, rightTypes...)

	var outputTypes []*types.T
	if outputContinuationColumn {
		outputTypes = jType.MakeOutputTypesWithContinuationColumn(leftTypes, rightTypes)
		jb.outputSize = len(outputTypes) - 1
	} else {
		outputTypes = jType.MakeOutputTypes(leftTypes, rightTypes)
		jb.outputSize = len(outputTypes)
	}

	evalCtx := flowCtx.EvalCtx
	if !onExpr.Empty() {
		// Only make a copy if we need to evaluate ON expression.
		evalCtx = flowCtx.NewEvalCtx()
	}
	if err := jb.ProcessorBase.InitWithEvalCtx(
		ctx, self, post, outputTypes, flowCtx, evalCtx, processorID, nil /* memMonitor */, opts,
	); err != nil {
		return nil, err
	}
	semaCtx := flowCtx.NewSemaContext(flowCtx.Txn)
	return evalCtx, jb.onCond.Init(ctx, onExpr, onCondTypes, semaCtx, evalCtx)
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

// emptyRow is used for rendering unmatched rows in outer and anti-joins.
var emptyRow = [16]rowenc.EncDatum{
	rowenc.NullEncDatum(), rowenc.NullEncDatum(), rowenc.NullEncDatum(), rowenc.NullEncDatum(),
	rowenc.NullEncDatum(), rowenc.NullEncDatum(), rowenc.NullEncDatum(), rowenc.NullEncDatum(),
	rowenc.NullEncDatum(), rowenc.NullEncDatum(), rowenc.NullEncDatum(), rowenc.NullEncDatum(),
	rowenc.NullEncDatum(), rowenc.NullEncDatum(), rowenc.NullEncDatum(), rowenc.NullEncDatum(),
}

// copyEmptyRow copies NULL values into dst.
func copyEmptyRow(dst []rowenc.EncDatum) {
	for len(dst) > 0 {
		n := copy(dst, emptyRow[:])
		dst = dst[n:]
	}
}

// renderUnmatchedRow creates a result row given an unmatched row on either
// side. Only used for outer and anti joins. Note that if the join is outputting
// a continuation column, the returned slice does not include the continuation
// column, but has the capacity for it.
func (jb *joinerBase) renderUnmatchedRow(row rowenc.EncDatumRow, side joinSide) rowenc.EncDatumRow {
	// Return row if the empty side of the join should not be rendered.
	if (side == leftSide && !jb.joinType.ShouldIncludeRightColsInOutput()) ||
		(side == rightSide && !jb.joinType.ShouldIncludeLeftColsInOutput()) {
		return row
	}

	// Return an empty row if the non-empty side of the join should not be
	// rendered.
	if (side == leftSide && !jb.joinType.ShouldIncludeLeftColsInOutput()) ||
		(side == rightSide && !jb.joinType.ShouldIncludeRightColsInOutput()) {
		copyEmptyRow(jb.combinedRow[:jb.outputSize])
		return jb.combinedRow[:jb.outputSize]
	}

	// Otherwise, combine row and NULLs in combined row. First, determine which
	// side of the row should be filled with NULLs.
	var nonEmpty rowenc.EncDatumRow
	var empty rowenc.EncDatumRow
	if side == leftSide {
		nonEmpty = jb.combinedRow[:len(row)]
		empty = jb.combinedRow[len(row):jb.outputSize]
	} else {
		split := jb.outputSize - len(row)
		empty = jb.combinedRow[:split]
		nonEmpty = jb.combinedRow[split:jb.outputSize]
	}

	// Copy the non-empty side of the row.
	if len(nonEmpty) == 1 {
		// If the slice is of length 1, it is faster to use direct assignment
		// instead of copy.
		nonEmpty[0] = row[0]
	} else {
		copy(nonEmpty, row)
	}

	// Copy the empty side of the row.
	if len(empty) == 1 {
		// If the slice is of length 1, it is faster to use direct assignment
		// instead of copyEmptyRow.
		empty[0] = rowenc.NullEncDatum()
	} else {
		copyEmptyRow(empty)
	}

	return jb.combinedRow[:jb.outputSize]
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

	if jb.onCond.Expr() != nil {
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
		res, err := jb.onCond.EvalFilter(jb.Ctx(), combinedRow)
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
