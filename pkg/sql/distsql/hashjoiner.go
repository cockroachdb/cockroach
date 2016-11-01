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
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// TODO(irfansharif): Document this.
// TODO(irfansharif): It's trivial to use the grace hash join algorithm by using
// hashrouters and hash joiners to parallelize hash joins. We would begin by
// 'partitioning' both tables via a hash function. Given the 'partitions' are
// formed by hashing on the join key any join output tuples must belong to the
// same 'partition', each 'partition' would then undergo the standard build and
// probe hash join algorithm, the computation of the partial joins is
// parallelizable.
type bucket struct {
	rows sqlbase.EncDatumRows
	seen bool
}

type hashJoiner struct {
	left        RowSource
	right       RowSource
	output      RowReceiver
	ctx         context.Context
	joinType    joinType
	filter      exprHelper
	leftEqCols  columns
	rightEqCols columns
	outputCols  columns
	buckets     map[string]bucket

	emptyRight  sqlbase.EncDatumRow
	emptyLeft   sqlbase.EncDatumRow
	combinedRow sqlbase.EncDatumRow
	rowAlloc    sqlbase.EncDatumRowAlloc
	datumAlloc  sqlbase.DatumAlloc
}

var _ processor = &hashJoiner{}

func newHashJoiner(
	flowCtx *FlowCtx, spec *HashJoinerSpec, inputs []RowSource, output RowReceiver,
) (*hashJoiner, error) {
	h := &hashJoiner{
		left:        inputs[0],
		right:       inputs[1],
		output:      output,
		ctx:         log.WithLogTag(flowCtx.Context, "Hash Joiner", nil),
		leftEqCols:  columns(spec.LeftEqColumns),
		rightEqCols: columns(spec.RightEqColumns),
		outputCols:  columns(spec.OutputColumns),
		joinType:    joinType(spec.Type),
		buckets:     make(map[string]bucket),
		emptyLeft:   make(sqlbase.EncDatumRow, len(spec.LeftTypes)),
		emptyRight:  make(sqlbase.EncDatumRow, len(spec.RightTypes)),
	}

	for i := range h.emptyLeft {
		h.emptyLeft[i].Datum = parser.DNull
	}
	for i := range h.emptyRight {
		h.emptyRight[i].Datum = parser.DNull
	}

	err := h.filter.init(spec.Expr, append(spec.LeftTypes, spec.RightTypes...), flowCtx.evalCtx)
	if err != nil {
		return nil, err
	}

	return h, nil
}

// Run is part of the processor interface.
func (h *hashJoiner) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx, span := tracing.ChildSpan(h.ctx, "hash joiner")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting hash joiner run")
		defer log.Infof(ctx, "exiting hash joiner run")
	}

	if err := h.buildPhase(); err != nil {
		h.output.Close(err)
		return
	}
	if err := h.probePhase(); err != nil {
		h.output.Close(err)
		return
	}
}

func (h *hashJoiner) buildPhase() error {
	var scratch []byte
	for {
		lrow, err := h.left.NextRow()
		if err != nil || lrow == nil {
			return err
		}

		encoded, err := h.encode(scratch, lrow, h.leftEqCols)
		if err != nil {
			return err
		}
		b, _ := h.buckets[string(encoded)]
		b.rows = append(b.rows, lrow)
		h.buckets[string(encoded)] = b

		scratch = encoded[:0]
	}
	return nil
}

func (h *hashJoiner) probePhase() error {
	var scratch []byte
	for {
		rrow, err := h.right.NextRow()
		if err != nil {
			return err
		}
		if rrow == nil {
			break
		}

		encoded, err := h.encode(scratch, rrow, h.rightEqCols)
		if err != nil {
			return err
		}

		b, ok := h.buckets[string(encoded)]
		if !ok {
			r, err := h.render(nil, rrow)
			if err != nil {
				return err
			}
			if !h.output.PushRow(r) {
				return nil
			}
		} else {
			b.seen = true
			h.buckets[string(encoded)] = b
			for _, lrow := range b.rows {
				r, err := h.render(lrow, rrow)
				if err != nil {
					return err
				}
				if r != nil && !h.output.PushRow(r) {
					return nil
				}
			}
		}
		scratch = encoded[:0]
	}

	if h.joinType == innerJoin || h.joinType == rightOuter {
		return nil
	}

	for _, b := range h.buckets {
		if !b.seen {
			for _, lrow := range b.rows {
				r, err := h.render(lrow, nil)
				if err != nil {
					return err
				}
				if r != nil && !h.output.PushRow(r) {
					return nil
				}
			}

		}
	}

	return nil
}

// encode returns the encoding for the grouping columns, this is then used as
// our group key to determine which bucket to add to.
func (h *hashJoiner) encode(
	appendTo []byte, row sqlbase.EncDatumRow, cols columns,
) (encoding []byte, err error) {
	for _, colIdx := range cols {
		appendTo, err = row[colIdx].Encode(&h.datumAlloc, sqlbase.DatumEncoding_VALUE, appendTo)
		if err != nil {
			return appendTo, err
		}
	}
	return appendTo, nil
}

// render evaluates the provided filter and constructs a row with columns from
// both rows as specified by the provided output columns. We expect left or
// right to be nil if there was no explicit "join" match, the filter is then
// evaluated on a combinedRow with null values for the columns of the nil row.
func (h *hashJoiner) render(lrow, rrow sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	switch h.joinType {
	case innerJoin:
		if lrow == nil || rrow == nil {
			return nil, nil
		}
	case fullOuter:
		if lrow == nil {
			lrow = h.emptyLeft
		} else if rrow == nil {
			rrow = h.emptyRight
		}
	case leftOuter:
		if rrow == nil {
			rrow = h.emptyRight
		}
	case rightOuter:
		if lrow == nil {
			lrow = h.emptyLeft
		}
	}
	h.combinedRow = append(h.combinedRow[:0], lrow...)
	h.combinedRow = append(h.combinedRow, rrow...)
	res, err := h.filter.evalFilter(h.combinedRow)
	if !res || err != nil {
		return nil, err
	}

	row := h.rowAlloc.AllocRow(len(h.outputCols))
	for i, col := range h.outputCols {
		row[i] = h.combinedRow[col]
	}
	return row, nil
}
