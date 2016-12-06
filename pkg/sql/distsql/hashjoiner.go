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

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// bucket here is the set of rows for a given group key (comprised of
// columns specified by the join constraints), 'seen' is used to determine if
// there was a matching group (with the same group key) in the opposite stream.
type bucket struct {
	rows sqlbase.EncDatumRows
	seen bool
}

// HashJoiner performs hash join, it has two input streams and one output.
//
// It works by reading the entire right stream and putting it in a hash
// table. Thus, there is no guarantee on the ordering of results that stem only
// from the right input (in the case of RIGHT OUTER, FULL OUTER). However, it is
// guaranteed that results that involve the left stream preserve the ordering;
// i.e. all results that stem from left row (i) precede results that stem from
// left row (i+1).
type hashJoiner struct {
	joinerBase

	leftEqCols  columns
	rightEqCols columns
	buckets     map[string]bucket
	datumAlloc  sqlbase.DatumAlloc
}

var _ processor = &hashJoiner{}

func newHashJoiner(
	flowCtx *FlowCtx, spec *HashJoinerSpec, inputs []RowSource, output RowReceiver,
) (*hashJoiner, error) {
	h := &hashJoiner{
		leftEqCols:  columns(spec.LeftEqColumns),
		rightEqCols: columns(spec.RightEqColumns),
		buckets:     make(map[string]bucket),
	}

	err := h.joinerBase.init(flowCtx, inputs, output, spec.OutputColumns,
		spec.Type, spec.LeftTypes, spec.RightTypes, spec.Expr)
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

// buildPhase constructs our internal hash map of rows seen, this is done
// entirely from the right stream with the encoding/group key generated using the
// left equality columns.
func (h *hashJoiner) buildPhase() error {
	var scratch []byte
	for {
		rrow, err := h.inputs[1].NextRow()
		if err != nil || rrow == nil {
			return err
		}

		encoded, hasNull, err := h.encode(scratch, rrow, h.rightEqCols)
		if err != nil {
			return err
		}

		scratch = encoded[:0]

		if hasNull {
			// A row that has a NULL in an equality column will not match anything.
			// Output it it or throw it away.
			if h.joinType == rightOuter || h.joinType == fullOuter {
				row, err := h.render(nil, rrow)
				if err != nil {
					return err
				}
				if row != nil && !h.output.PushRow(row) {
					return nil
				}
			}
			continue
		}

		b, _ := h.buckets[string(encoded)]
		b.rows = append(b.rows, rrow)
		h.buckets[string(encoded)] = b
	}
}

// probePhase uses our constructed hash map of rows seen from the right stream,
// we probe the map for each row retrieved from the left stream outputting the
// merging of the two rows if matched. Behaviour for outer joins is as expected,
// i.e. for RIGHT OUTER joins if no corresponding left row is seen an empty
// DNull row is emitted instead.
func (h *hashJoiner) probePhase() error {
	var scratch []byte
	for {
		lrow, err := h.inputs[0].NextRow()
		if err != nil {
			return err
		}
		if lrow == nil {
			break
		}

		encoded, hasNull, err := h.encode(scratch, lrow, h.leftEqCols)
		if err != nil {
			return err
		}
		scratch = encoded[:0]

		if hasNull {
			// A row that has a NULL in an equality column will not match anything.
			// Output it it or throw it away.
			if h.joinType == leftOuter || h.joinType == fullOuter {
				row, err := h.render(lrow, nil)
				if err != nil {
					return err
				}
				if row != nil && !h.output.PushRow(row) {
					return nil
				}
			}
			continue
		}

		b, ok := h.buckets[string(encoded)]
		if !ok {
			row, err := h.render(lrow, nil)
			if err != nil {
				return err
			}
			if row != nil && !h.output.PushRow(row) {
				return nil
			}
		} else {
			b.seen = true
			h.buckets[string(encoded)] = b
			for _, rrow := range b.rows {
				row, err := h.render(lrow, rrow)
				if err != nil {
					return err
				}
				if row != nil && !h.output.PushRow(row) {
					return nil
				}
			}
		}
	}

	if h.joinType == innerJoin || h.joinType == leftOuter {
		return nil
	}

	// Produce results for unmatched right rows (for RIGHT OUTER or FULL OUTER).
	for _, b := range h.buckets {
		if !b.seen {
			for _, rrow := range b.rows {
				row, err := h.render(nil, rrow)
				if err != nil {
					return err
				}
				if row != nil && !h.output.PushRow(row) {
					return nil
				}
			}
		}
	}

	return nil
}

// encode returns the encoding for the grouping columns, this is then used as
// our group key to determine which bucket to add to.
// If the row contains any NULLs, hasNull is true and no encoding is returned.
func (h *hashJoiner) encode(
	appendTo []byte, row sqlbase.EncDatumRow, cols columns,
) (encoding []byte, hasNull bool, err error) {
	for _, colIdx := range cols {
		if row[colIdx].IsNull() {
			return nil, true, nil
		}
		appendTo, err = row[colIdx].Encode(&h.datumAlloc, sqlbase.DatumEncoding_VALUE, appendTo)
		if err != nil {
			return appendTo, false, err
		}
	}
	return appendTo, false, nil
}
