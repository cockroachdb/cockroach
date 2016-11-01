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
	"errors"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

type joinType int

const (
	innerJoin joinType = iota
	leftOuter
	rightOuter
	fullOuter
)

// mergeJoiner performs merge join, it has two input row sources with the same
// ordering on the columns that have equality constraints.
//
// It is guaranteed that the results preserve this ordering.
type mergeJoiner struct {
	inputs     []RowSource
	output     RowReceiver
	joinType   joinType
	filter     exprHelper
	outputCols columns

	ctx          context.Context
	combinedRow  sqlbase.EncDatumRow
	rowAlloc     sqlbase.EncDatumRowAlloc
	streamMerger streamMerger
	emptyRight   sqlbase.EncDatumRow
	emptyLeft    sqlbase.EncDatumRow
}

var _ processor = &mergeJoiner{}

func newMergeJoiner(
	flowCtx *FlowCtx, spec *MergeJoinerSpec, inputs []RowSource, output RowReceiver,
) (*mergeJoiner, error) {
	for i, c := range spec.LeftOrdering.Columns {
		if spec.RightOrdering.Columns[i].Direction != c.Direction {
			return nil, errors.New("Unmatched column orderings")
		}
	}

	m := &mergeJoiner{
		inputs:     inputs,
		output:     output,
		ctx:        log.WithLogTag(flowCtx.Context, "Merge Joiner", nil),
		outputCols: columns(spec.OutputColumns),
		joinType:   joinType(spec.Type),
		filter:     exprHelper{},
		emptyLeft:  make(sqlbase.EncDatumRow, len(spec.LeftTypes)),
		emptyRight: make(sqlbase.EncDatumRow, len(spec.RightTypes)),
	}

	for i := range m.emptyLeft {
		m.emptyLeft[i].Datum = parser.DNull
	}
	for i := range m.emptyRight {
		m.emptyRight[i].Datum = parser.DNull
	}

	var err error
	m.streamMerger, err = makeStreamMerger(
		[]sqlbase.ColumnOrdering{
			convertToColumnOrdering(spec.LeftOrdering),
			convertToColumnOrdering(spec.RightOrdering),
		}, inputs)
	if err != nil {
		return nil, err
	}

	err = m.filter.init(spec.Expr, append(spec.LeftTypes, spec.RightTypes...), flowCtx.evalCtx)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// Run is part of the processor interface.
func (m *mergeJoiner) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx, span := tracing.ChildSpan(m.ctx, "merge joiner")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting merge joiner run")
		defer log.Infof(ctx, "exiting merge joiner run")
	}

	for {
		batch, err := m.streamMerger.NextBatch()
		if err != nil || len(batch) == 0 {
			m.output.Close(err)
			return
		}
		for _, rowPair := range batch {
			row, err := m.render(rowPair[0], rowPair[1])
			if err != nil {
				m.output.Close(err)
				return
			}
			if row != nil && !m.output.PushRow(row) {
				if log.V(2) {
					log.Infof(ctx, "no more rows required")
				}
				m.output.Close(nil)
				return
			}
		}
	}
}

// render evaluates the provided filter and constructs a row with columns from
// both rows as specified by the provided output columns. We expect left or
// right to be nil if there was no explicit "join" match, the filter is then
// evaluated on a combinedRow with null values for the columns of the nil row.
func (m *mergeJoiner) render(left, right sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	switch m.joinType {
	case innerJoin:
		if left == nil || right == nil {
			return nil, nil
		}
	case fullOuter:
		if left == nil {
			left = m.emptyLeft
		} else if right == nil {
			right = m.emptyRight
		}
	case leftOuter:
		if right == nil {
			right = m.emptyRight
		}
	case rightOuter:
		if left == nil {
			left = m.emptyLeft
		}
	}
	m.combinedRow = append(m.combinedRow[:0], left...)
	m.combinedRow = append(m.combinedRow, right...)
	res, err := m.filter.evalFilter(m.combinedRow)
	if !res || err != nil {
		return nil, err
	}

	row := m.rowAlloc.AllocRow(len(m.outputCols))
	for i, col := range m.outputCols {
		row[i] = m.combinedRow[col]
	}
	return row, nil
}
