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

package distsql

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type joinerBase struct {
	inputs []RowSource
	output RowReceiver
	ctx    context.Context

	joinType    joinType
	outputCols  columns
	filter      exprHelper
	rowAlloc    sqlbase.EncDatumRowAlloc
	emptyLeft   sqlbase.EncDatumRow
	emptyRight  sqlbase.EncDatumRow
	combinedRow sqlbase.EncDatumRow
}

// err := init(flowCtx, inputs, output, spec.OutputColumns,
// 	spec.Type, spec.LeftTypes, spec.RightTypes, spec.Expr)
func (jb *joinerBase) init(
	flowCtx *FlowCtx,
	inputs []RowSource,
	output RowReceiver,
	outputCols []uint32,
	jType JoinType,
	leftTypes []sqlbase.ColumnType_Kind,
	rightTypes []sqlbase.ColumnType_Kind,
	expr Expression,
) error {
	jb.inputs = inputs
	jb.output = output
	jb.ctx = log.WithLogTag(flowCtx.Context, "Joiner", nil)
	jb.outputCols = columns(outputCols)
	jb.joinType = joinType(jType)
	jb.emptyLeft = make(sqlbase.EncDatumRow, len(leftTypes))
	for i := range jb.emptyLeft {
		jb.emptyLeft[i].Datum = parser.DNull
	}

	jb.emptyRight = make(sqlbase.EncDatumRow, len(rightTypes))
	for i := range jb.emptyRight {
		jb.emptyRight[i].Datum = parser.DNull
	}

	return jb.filter.init(expr, append(leftTypes, rightTypes...), flowCtx.evalCtx)
}

// render evaluates the provided filter and constructs a row with columns from
// both rows as specified by the provided output columns. We expect left or
// right to be nil if there was no explicit "join" match, the filter is then
// evaluated on a combinedRow with null values for the columns of the nil row.
func (jb *joinerBase) render(lrow, rrow sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	switch jb.joinType {
	case innerJoin:
		if lrow == nil || rrow == nil {
			return nil, nil
		}
	case fullOuter:
		if lrow == nil {
			lrow = jb.emptyLeft
		} else if rrow == nil {
			rrow = jb.emptyRight
		}
	case leftOuter:
		if rrow == nil {
			rrow = jb.emptyRight
		}
	case rightOuter:
		if lrow == nil {
			lrow = jb.emptyLeft
		}
	}
	jb.combinedRow = append(jb.combinedRow[:0], lrow...)
	jb.combinedRow = append(jb.combinedRow, rrow...)
	res, err := jb.filter.evalFilter(jb.combinedRow)
	if !res || err != nil {
		return nil, err
	}

	row := jb.rowAlloc.AllocRow(len(jb.outputCols))
	for i, col := range jb.outputCols {
		row[i] = jb.combinedRow[col]
	}
	return row, nil
}
