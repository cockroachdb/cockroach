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
//
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsqlrun

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type joinerBase struct {
	leftSource, rightSource RowSource

	joinType    joinType
	onCond      exprHelper
	emptyLeft   sqlbase.EncDatumRow
	emptyRight  sqlbase.EncDatumRow
	combinedRow sqlbase.EncDatumRow

	out procOutputHelper
}

func (jb *joinerBase) init(
	flowCtx *FlowCtx,
	leftSource RowSource,
	rightSource RowSource,
	jType JoinType,
	onExpr Expression,
	post *PostProcessSpec,
	output RowReceiver,
) error {
	jb.leftSource = leftSource
	jb.rightSource = rightSource
	jb.joinType = joinType(jType)

	leftTypes := leftSource.Types()
	jb.emptyLeft = make(sqlbase.EncDatumRow, len(leftTypes))
	for i := range jb.emptyLeft {
		jb.emptyLeft[i].Datum = parser.DNull
	}
	rightTypes := rightSource.Types()
	jb.emptyRight = make(sqlbase.EncDatumRow, len(rightTypes))
	for i := range jb.emptyRight {
		jb.emptyRight[i].Datum = parser.DNull
	}

	jb.combinedRow = make(sqlbase.EncDatumRow, 0, len(leftTypes)+len(rightTypes))

	types := make([]sqlbase.ColumnType, 0, len(leftTypes)+len(rightTypes))
	types = append(types, leftTypes...)
	types = append(types, rightTypes...)

	if err := jb.onCond.init(onExpr, types, &flowCtx.evalCtx); err != nil {
		return err
	}
	return jb.out.init(post, types, &flowCtx.evalCtx, output)
}

// renderUnmatchedRow creates a result row given an unmatched row on either
// side. Only used for outer joins.
func (jb *joinerBase) renderUnmatchedRow(
	row sqlbase.EncDatumRow, leftSide bool,
) sqlbase.EncDatumRow {
	lrow, rrow := jb.emptyLeft, jb.emptyRight
	if leftSide {
		lrow = row
	} else {
		rrow = row
	}
	jb.combinedRow = append(jb.combinedRow[:0], lrow...)
	jb.combinedRow = append(jb.combinedRow, rrow...)
	return jb.combinedRow
}

// shouldEmitUnmatchedRow determines if we should emit am ummatched row (with
// NULLs for the columns of the other stream). This happens in FULL OUTER joins
// and LEFT or RIGHT OUTER joins (depending on which stream).
func shouldEmitUnmatchedRow(leftSide bool, joinType joinType) bool {
	switch joinType {
	case innerJoin:
		return false
	case rightOuter:
		if leftSide {
			return false
		}
	case leftOuter:
		if !leftSide {
			return false
		}
	}
	return true
}

// maybeEmitUnmatchedRow is used for rows that don't match anything in the other
// table; we emit them if it's called for given the type of join, otherwise we
// discard them.
//
// Returns false if no more rows are needed (in which case the inputs and the
// output has been properly closed).
func (jb *joinerBase) maybeEmitUnmatchedRow(
	ctx context.Context, row sqlbase.EncDatumRow, leftSide bool,
) bool {
	if !shouldEmitUnmatchedRow(leftSide, jb.joinType) {
		return true
	}

	renderedRow := jb.renderUnmatchedRow(row, leftSide)
	return emitHelper(ctx, &jb.out, renderedRow, ProducerMetadata{}, jb.leftSource, jb.rightSource)
}

// render constructs a row with columns from both sides. The ON condition is
// evaluated; if it fails, returns nil.
func (jb *joinerBase) render(lrow, rrow sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	jb.combinedRow = append(jb.combinedRow[:0], lrow...)
	jb.combinedRow = append(jb.combinedRow, rrow...)
	res, err := jb.onCond.evalFilter(jb.combinedRow)
	if !res || err != nil {
		return nil, err
	}
	return jb.combinedRow, nil
}
