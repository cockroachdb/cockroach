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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// joinerBase is the common core of all joiners.
type joinerBase struct {
	execinfra.ProcessorBase

	joinType    sqlbase.JoinType
	onCond      execinfra.ExprHelper
	emptyLeft   sqlbase.EncDatumRow
	emptyRight  sqlbase.EncDatumRow
	combinedRow sqlbase.EncDatumRow

	// EqCols contains the indices of the columns that are constrained to be
	// equal. Specifically column EqCols[0][i] on the left side must match the
	// column EqCols[1][i] on the right side.
	eqCols [2][]uint32

	// numMergedEqualityColumns specifies how many of the equality
	// columns must be merged at the beginning of each result row. This
	// is the desired behavior for USING and NATURAL JOIN.
	numMergedEqualityColumns int
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
	jType sqlbase.JoinType,
	onExpr execinfrapb.Expression,
	leftEqColumns []uint32,
	rightEqColumns []uint32,
	numMergedColumns uint32,
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

	jb.emptyLeft = make(sqlbase.EncDatumRow, len(leftTypes))
	for i := range jb.emptyLeft {
		jb.emptyLeft[i] = sqlbase.DatumToEncDatum(leftTypes[i], tree.DNull)
	}
	jb.emptyRight = make(sqlbase.EncDatumRow, len(rightTypes))
	for i := range jb.emptyRight {
		jb.emptyRight[i] = sqlbase.DatumToEncDatum(rightTypes[i], tree.DNull)
	}

	jb.eqCols[leftSide] = leftEqColumns
	jb.eqCols[rightSide] = rightEqColumns
	jb.numMergedEqualityColumns = int(numMergedColumns)

	size := len(leftTypes) + jb.numMergedEqualityColumns + len(rightTypes)
	jb.combinedRow = make(sqlbase.EncDatumRow, size)

	condTypes := make([]*types.T, 0, size)
	for idx := 0; idx < jb.numMergedEqualityColumns; idx++ {
		ltype := leftTypes[jb.eqCols[leftSide][idx]]
		rtype := rightTypes[jb.eqCols[rightSide][idx]]
		var ctype *types.T
		if ltype.Family() != types.UnknownFamily {
			ctype = ltype
		} else {
			ctype = rtype
		}
		condTypes = append(condTypes, ctype)
	}
	condTypes = append(condTypes, leftTypes...)
	condTypes = append(condTypes, rightTypes...)

	outputSize := len(leftTypes) + jb.numMergedEqualityColumns
	if jb.joinType.ShouldIncludeRightColsInOutput() {
		outputSize += len(rightTypes)
	}
	outputTypes := condTypes[:outputSize]

	if err := jb.ProcessorBase.Init(
		self, post, outputTypes, flowCtx, processorID, output, nil /* memMonitor */, opts,
	); err != nil {
		return err
	}
	return jb.onCond.Init(onExpr, condTypes, jb.EvalCtx)
}

// joinSide is the utility type to distinguish between two sides of the join.
type joinSide uint8

const (
	// leftSide indicates the left side of the join.
	leftSide joinSide = 0
	// rightSide indicates the right side of the join.
	rightSide joinSide = 1
)

// otherSide returns the opposite to s side.
func otherSide(s joinSide) joinSide {
	return joinSide(1 - uint8(s))
}

func (j joinSide) String() string {
	if j == leftSide {
		return "left"
	}
	return "right"
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

	if jb.onCond.Expr != nil {
		res, err := jb.onCond.EvalFilter(jb.combinedRow)
		if !res || err != nil {
			return nil, err
		}
	}
	return jb.combinedRow, nil
}
