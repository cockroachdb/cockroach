// Copyright 2016 The Cockroach Authors.
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

package distsqlrun

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

type joinerBase struct {
	processorBase

	joinType    sqlbase.JoinType
	onCond      exprHelper
	emptyLeft   sqlbase.EncDatumRow
	emptyRight  sqlbase.EncDatumRow
	combinedRow sqlbase.EncDatumRow

	// eqCols contains the indices of the columns that are constrained to be
	// equal. Specifically column eqCols[0][i] on the left side must match the
	// column eqCols[1][i] on the right side.
	eqCols [2]columns

	// numMergedEqualityColumns specifies how many of the equality
	// columns must be merged at the beginning of each result row. This
	// is the desired behavior for USING and NATURAL JOIN.
	numMergedEqualityColumns int
}

func (jb *joinerBase) init(
	flowCtx *FlowCtx,
	leftTypes []sqlbase.ColumnType,
	rightTypes []sqlbase.ColumnType,
	jType sqlbase.JoinType,
	onExpr Expression,
	leftEqColumns []uint32,
	rightEqColumns []uint32,
	numMergedColumns uint32,
	post *PostProcessSpec,
	output RowReceiver,
) error {
	jb.joinType = jType

	numLeftCols, numRightCols := len(leftTypes), len(rightTypes)
	if post != nil {
		if post.Projection {
			numLeftCols, numRightCols = len(post.OutputColumns), len(post.OutputColumns)
		} else if post.RenderExprs != nil {
			numLeftCols, numRightCols = len(post.RenderExprs), len(post.RenderExprs)
		}
	}
	if isSetOpJoin(jb.joinType) {
		if err := isValidSetOpJoin(
			onExpr, numLeftCols, numRightCols, len(leftEqColumns),
		); err != nil {
			return err
		}
	}

	jb.emptyLeft = make(sqlbase.EncDatumRow, len(leftTypes))
	for i := range jb.emptyLeft {
		jb.emptyLeft[i] = sqlbase.DatumToEncDatum(leftTypes[i], tree.DNull)
	}
	jb.emptyRight = make(sqlbase.EncDatumRow, len(rightTypes))
	for i := range jb.emptyRight {
		jb.emptyRight[i] = sqlbase.DatumToEncDatum(rightTypes[i], tree.DNull)
	}

	jb.eqCols[leftSide] = columns(leftEqColumns)
	jb.eqCols[rightSide] = columns(rightEqColumns)
	jb.numMergedEqualityColumns = int(numMergedColumns)

	size := len(leftTypes) + jb.numMergedEqualityColumns + len(rightTypes)
	jb.combinedRow = make(sqlbase.EncDatumRow, size)

	condTypes := make([]sqlbase.ColumnType, 0, size)
	for idx := 0; idx < jb.numMergedEqualityColumns; idx++ {
		ltype := leftTypes[jb.eqCols[leftSide][idx]]
		rtype := rightTypes[jb.eqCols[rightSide][idx]]
		var ctype sqlbase.ColumnType
		if ltype.SemanticType != sqlbase.ColumnType_NULL {
			ctype = ltype
		} else {
			ctype = rtype
		}
		condTypes = append(condTypes, ctype)
	}
	condTypes = append(condTypes, leftTypes...)
	condTypes = append(condTypes, rightTypes...)

	outputSize := len(leftTypes) + jb.numMergedEqualityColumns
	if shouldIncludeRightColsInOutput(jb.joinType) {
		outputSize += len(rightTypes)
	}
	outputTypes := condTypes[:outputSize]

	evalCtx := flowCtx.NewEvalCtx()
	if err := jb.onCond.init(onExpr, condTypes, evalCtx); err != nil {
		return err
	}
	return jb.processorBase.init(post, outputTypes, flowCtx, evalCtx, output)
}

type joinSide uint8

const (
	leftSide  joinSide = 0
	rightSide joinSide = 1
)

func otherSide(s joinSide) joinSide {
	return joinSide(1 - uint8(s))
}

// renderUnmatchedRow creates a result row given an unmatched row on either
// side. Only used for outer joins.
func (jb *joinerBase) renderUnmatchedRow(
	row sqlbase.EncDatumRow, side joinSide,
) sqlbase.EncDatumRow {
	lrow, rrow := jb.emptyLeft, jb.emptyRight
	if side == leftSide {
		lrow = row
	} else {
		rrow = row
	}

	// If there are merged columns, they take first positions in a row
	// Values are taken from non-empty row
	jb.combinedRow = jb.combinedRow[:0]
	for idx := 0; idx < jb.numMergedEqualityColumns; idx++ {
		jb.combinedRow = append(jb.combinedRow, row[jb.eqCols[side][idx]])
	}
	jb.combinedRow = append(jb.combinedRow, lrow...)
	jb.combinedRow = append(jb.combinedRow, rrow...)
	return jb.combinedRow
}

func shouldIncludeRightColsInOutput(joinType sqlbase.JoinType) bool {
	switch joinType {
	case sqlbase.LeftSemiJoin, sqlbase.LeftAntiJoin, sqlbase.IntersectAllJoin, sqlbase.ExceptAllJoin:
		return false
	default:
		return true
	}
}

func isSetOpJoin(joinType sqlbase.JoinType) bool {
	return joinType == sqlbase.IntersectAllJoin || joinType == sqlbase.ExceptAllJoin
}

func isValidSetOpJoin(onExpr Expression, numLeftCols int, numRightCols int, numEqCols int) error {
	if onExpr.Expr != "" {
		return errors.Errorf("expected empty onExpr, got %v", onExpr.Expr)
	}
	if numLeftCols != numEqCols || numRightCols != numEqCols {
		return errors.Errorf(
			"expected %v left and right columns, got %v left and %v right columns",
			numEqCols, numLeftCols, numRightCols)
	}
	return nil
}

// shouldEmitUnmatchedRow determines if we should emit am ummatched row (with
// NULLs for the columns of the other stream). This happens in FULL OUTER joins
// and LEFT or RIGHT OUTER joins and ANTI joins (depending on which stream is
// stored).
func shouldEmitUnmatchedRow(side joinSide, joinType sqlbase.JoinType) bool {
	switch joinType {
	case sqlbase.LeftSemiJoin, sqlbase.InnerJoin, sqlbase.IntersectAllJoin:
		return false
	case sqlbase.RightOuterJoin:
		return side == rightSide
	case sqlbase.LeftOuterJoin:
		return side == leftSide
	case sqlbase.LeftAntiJoin:
		return side == leftSide
	case sqlbase.ExceptAllJoin:
		return side == leftSide
	case sqlbase.FullOuterJoin:
		return true
	default:
		return true
	}
}

// maybeEmitUnmatchedRow is used for rows that don't match anything in the
// other table; we emit them if it's called for given the type of join,
// otherwise we discard them.
//
// Returns false if no more rows are needed. Also returns any error occurring
// when emitting the row (i.e. filtering or rendering errors). If false or an
// error are returned, the inputs still need to be drained and closed and the
// output needs to be closed. If an error is returned, it is the caller's
// responsibility to pushed the error to the output.
func (jb *joinerBase) maybeEmitUnmatchedRow(
	ctx context.Context, row sqlbase.EncDatumRow, side joinSide,
) (bool, error) {
	if !shouldEmitUnmatchedRow(side, jb.joinType) {
		return true, nil
	}

	renderedRow := jb.renderUnmatchedRow(row, side)
	consumerStatus, err := jb.out.EmitRow(ctx, renderedRow)
	if err != nil {
		return false, err
	}
	return consumerStatus == NeedMoreRows, nil
}

// render constructs a row with columns from both sides. The ON condition is
// evaluated; if it fails, returns nil.
// Note the left and right merged equality columns (i.e. from a USING clause
// or after simplifying ON left.x = right.x) are NOT checked for equality.
// See CompareEncDatumRowForMerge.
func (jb *joinerBase) render(lrow, rrow sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	n := jb.numMergedEqualityColumns
	jb.combinedRow = jb.combinedRow[:n+len(lrow)+len(rrow)]
	for i := 0; i < n; i++ {
		// This function is called only when lrow and rrow match on the equality
		// columns which can never happen if there are any NULLs in these
		// columns. So we know for sure the lrow value is not null
		jb.combinedRow[i] = lrow[jb.eqCols[leftSide][i]]
	}
	copy(jb.combinedRow[n:], lrow)
	copy(jb.combinedRow[n+len(lrow):], rrow)

	if jb.onCond.expr != nil {
		res, err := jb.onCond.evalFilter(jb.combinedRow)
		if !res || err != nil {
			return nil, err
		}
	}
	return jb.combinedRow, nil
}
