// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/pkg/errors"
)

// JoinerBase is the common core of all joiners.
type JoinerBase struct {
	ProcessorBase

	JoinType    sqlbase.JoinType
	onCond      ExprHelper
	emptyLeft   sqlbase.EncDatumRow
	emptyRight  sqlbase.EncDatumRow
	combinedRow sqlbase.EncDatumRow

	// EqCols contains the indices of the columns that are constrained to be
	// equal. Specifically column EqCols[0][i] on the left side must match the
	// column EqCols[1][i] on the right side.
	EqCols [2][]uint32

	// numMergedEqualityColumns specifies how many of the equality
	// columns must be merged at the beginning of each result row. This
	// is the desired behavior for USING and NATURAL JOIN.
	numMergedEqualityColumns int
}

// Init initializes the JoinerBase.
//
// opts is passed along to the underlying ProcessorBase. The zero value is used
// if the processor using the JoinerBase is not implementing RowSource.
func (jb *JoinerBase) Init(
	self RowSource,
	flowCtx *FlowCtx,
	processorID int32,
	leftTypes []types.T,
	rightTypes []types.T,
	jType sqlbase.JoinType,
	onExpr execinfrapb.Expression,
	leftEqColumns []uint32,
	rightEqColumns []uint32,
	numMergedColumns uint32,
	post *execinfrapb.PostProcessSpec,
	output RowReceiver,
	opts ProcStateOpts,
) error {
	jb.JoinType = jType

	if jb.JoinType.IsSetOpJoin() {
		if !onExpr.Empty() {
			return errors.Errorf("expected empty onExpr, got %v", onExpr.Expr)
		}
	}

	jb.emptyLeft = make(sqlbase.EncDatumRow, len(leftTypes))
	for i := range jb.emptyLeft {
		jb.emptyLeft[i] = sqlbase.DatumToEncDatum(&leftTypes[i], tree.DNull)
	}
	jb.emptyRight = make(sqlbase.EncDatumRow, len(rightTypes))
	for i := range jb.emptyRight {
		jb.emptyRight[i] = sqlbase.DatumToEncDatum(&rightTypes[i], tree.DNull)
	}

	jb.EqCols[LeftSide] = leftEqColumns
	jb.EqCols[RightSide] = rightEqColumns
	jb.numMergedEqualityColumns = int(numMergedColumns)

	size := len(leftTypes) + jb.numMergedEqualityColumns + len(rightTypes)
	jb.combinedRow = make(sqlbase.EncDatumRow, size)

	condTypes := make([]types.T, 0, size)
	for idx := 0; idx < jb.numMergedEqualityColumns; idx++ {
		ltype := leftTypes[jb.EqCols[LeftSide][idx]]
		rtype := rightTypes[jb.EqCols[RightSide][idx]]
		var ctype types.T
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
	if shouldIncludeRightColsInOutput(jb.JoinType) {
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

// JoinSide is the utility type to dinstinguish between two sides of the join.
type JoinSide uint8

const (
	// LeftSide indicates the left side of the join.
	LeftSide JoinSide = 0
	// RightSide indicates the right side of the join.
	RightSide JoinSide = 1
)

// OtherSide returns the opposite to s side.
func OtherSide(s JoinSide) JoinSide {
	return JoinSide(1 - uint8(s))
}

func (j JoinSide) String() string {
	if j == LeftSide {
		return "left"
	}
	return "right"
}

// RenderUnmatchedRow creates a result row given an unmatched row on either
// side. Only used for outer joins.
func (jb *JoinerBase) RenderUnmatchedRow(
	row sqlbase.EncDatumRow, side JoinSide,
) sqlbase.EncDatumRow {
	lrow, rrow := jb.emptyLeft, jb.emptyRight
	if side == LeftSide {
		lrow = row
	} else {
		rrow = row
	}

	// If there are merged columns, they take first positions in a row
	// Values are taken from non-empty row
	jb.combinedRow = jb.combinedRow[:0]
	for idx := 0; idx < jb.numMergedEqualityColumns; idx++ {
		jb.combinedRow = append(jb.combinedRow, row[jb.EqCols[side][idx]])
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

// ShouldEmitUnmatchedRow determines if we should emit am ummatched row (with
// NULLs for the columns of the other stream). This happens in FULL OUTER joins
// and LEFT or RIGHT OUTER joins and ANTI joins (depending on which stream is
// stored).
func ShouldEmitUnmatchedRow(side JoinSide, joinType sqlbase.JoinType) bool {
	switch joinType {
	case sqlbase.LeftSemiJoin, sqlbase.InnerJoin, sqlbase.IntersectAllJoin:
		return false
	case sqlbase.RightOuterJoin:
		return side == RightSide
	case sqlbase.LeftOuterJoin:
		return side == LeftSide
	case sqlbase.LeftAntiJoin:
		return side == LeftSide
	case sqlbase.ExceptAllJoin:
		return side == LeftSide
	case sqlbase.FullOuterJoin:
		return true
	default:
		return true
	}
}

// Render constructs a row with columns from both sides. The ON condition is
// evaluated; if it fails, returns nil.
// Note the left and right merged equality columns (i.e. from a USING clause
// or after simplifying ON left.x = right.x) are NOT checked for equality.
// See CompareEncDatumRowForMerge.
func (jb *JoinerBase) Render(lrow, rrow sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	n := jb.numMergedEqualityColumns
	jb.combinedRow = jb.combinedRow[:n+len(lrow)+len(rrow)]
	for i := 0; i < n; i++ {
		// This function is called only when lrow and rrow match on the equality
		// columns which can never happen if there are any NULLs in these
		// columns. So we know for sure the lrow value is not null
		jb.combinedRow[i] = lrow[jb.EqCols[LeftSide][i]]
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
