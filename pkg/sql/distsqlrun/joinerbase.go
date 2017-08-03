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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type joinerBase struct {
	processorBase

	leftSource, rightSource RowSource

	joinType    joinType
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
	leftSource RowSource,
	rightSource RowSource,
	jType JoinType,
	onExpr Expression,
	leftEqColumns []uint32,
	rightEqColumns []uint32,
	numMergedColumns uint32,
	post *PostProcessSpec,
	output RowReceiver,
) error {
	jb.leftSource = leftSource
	jb.rightSource = rightSource
	jb.joinType = joinType(jType)

	leftTypes := leftSource.Types()
	jb.emptyLeft = make(sqlbase.EncDatumRow, len(leftTypes))
	for i := range jb.emptyLeft {
		jb.emptyLeft[i] = sqlbase.DatumToEncDatum(leftTypes[i], parser.DNull)
	}
	rightTypes := rightSource.Types()
	jb.emptyRight = make(sqlbase.EncDatumRow, len(rightTypes))
	for i := range jb.emptyRight {
		jb.emptyRight[i] = sqlbase.DatumToEncDatum(rightTypes[i], parser.DNull)
	}

	jb.eqCols[leftSide] = columns(leftEqColumns)
	jb.eqCols[rightSide] = columns(rightEqColumns)
	jb.numMergedEqualityColumns = int(numMergedColumns)

	jb.combinedRow = make(sqlbase.EncDatumRow, 0, len(leftTypes)+len(rightTypes)+jb.numMergedEqualityColumns)

	types := make([]sqlbase.ColumnType, 0, len(leftTypes)+len(rightTypes)+jb.numMergedEqualityColumns)
	for idx := 0; idx < jb.numMergedEqualityColumns; idx++ {
		ltype := leftTypes[jb.eqCols[leftSide][idx]]
		rtype := rightTypes[jb.eqCols[rightSide][idx]]
		var ctype sqlbase.ColumnType
		if ltype.SemanticType != sqlbase.ColumnType_NULL {
			ctype = ltype
		} else {
			ctype = rtype
		}
		types = append(types, ctype)
	}
	types = append(types, leftTypes...)
	types = append(types, rightTypes...)

	if err := jb.onCond.init(onExpr, types, &flowCtx.EvalCtx); err != nil {
		return err
	}
	return jb.out.Init(post, types, &flowCtx.EvalCtx, output)
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

// shouldEmitUnmatchedRow determines if we should emit am ummatched row (with
// NULLs for the columns of the other stream). This happens in FULL OUTER joins
// and LEFT or RIGHT OUTER joins (depending on which stream).
func shouldEmitUnmatchedRow(side joinSide, joinType joinType) bool {
	switch joinType {
	case innerJoin:
		return false
	case rightOuter:
		if side == leftSide {
			return false
		}
	case leftOuter:
		if side == rightSide {
			return false
		}
	}
	return true
}

// maybeEmitUnmatchedRow is used for rows that don't match anything in the other
// table; we emit them if it's called for given the type of join, otherwise we
// discard them.
//
// Returns false if no more rows are needed (in which case the inputs still need
// to be drained and closed).
func (jb *joinerBase) maybeEmitUnmatchedRow(
	ctx context.Context, row sqlbase.EncDatumRow, side joinSide,
) bool {
	if !shouldEmitUnmatchedRow(side, jb.joinType) {
		return true
	}

	renderedRow := jb.renderUnmatchedRow(row, side)
	consumerStatus, err := jb.out.EmitRow(ctx, renderedRow)
	if err != nil {
		jb.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
		return false
	}
	return consumerStatus == NeedMoreRows
}

// render constructs a row with columns from both sides. The ON condition is
// evaluated; if it fails, returns nil.
func (jb *joinerBase) render(lrow, rrow sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	jb.combinedRow = jb.combinedRow[:0]
	for idx := 0; idx < jb.numMergedEqualityColumns; idx++ {
		// this function is called only when lrow and rrow match on the equality
		// columns which can never happen if there are any NULLs in these
		// columns. So we know for sure the lrow value is not null
		value := lrow[jb.eqCols[leftSide][idx]]
		jb.combinedRow = append(jb.combinedRow, value)
	}
	jb.combinedRow = append(jb.combinedRow, lrow...)
	jb.combinedRow = append(jb.combinedRow, rrow...)
	res, err := jb.onCond.evalFilter(jb.combinedRow)
	if !res || err != nil {
		return nil, err
	}
	return jb.combinedRow, nil
}
