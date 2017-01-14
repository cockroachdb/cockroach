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
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// bucket here is the set of rows for a given group key (comprised of
// columns specified by the join constraints), 'seen' is used to determine if
// there was a matching row in the opposite stream.
type bucket struct {
	rows sqlbase.EncDatumRows
	seen []bool
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
	flowCtx *FlowCtx,
	spec *HashJoinerSpec,
	leftSource RowSource,
	rightSource RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (*hashJoiner, error) {
	h := &hashJoiner{
		leftEqCols:  columns(spec.LeftEqColumns),
		rightEqCols: columns(spec.RightEqColumns),
		buckets:     make(map[string]bucket),
	}

	if err := h.joinerBase.init(
		flowCtx, leftSource, rightSource, spec.Type, spec.OnExpr, post, output,
	); err != nil {
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

	defer h.leftSource.NoMoreRows()
	defer h.rightSource.NoMoreRows()

	if err := h.buildPhase(ctx); err != nil {
		h.out.close(err)
		return
	}
	if h.joinType == rightOuter || h.joinType == fullOuter {
		for k, bucket := range h.buckets {
			bucket.seen = make([]bool, len(bucket.rows))
			h.buckets[k] = bucket
		}
	}
	log.VEventf(ctx, 1, "build phase complete")
	err := h.probePhase(ctx)
	h.out.close(err)
}

// buildPhase constructs our internal hash map of rows seen, this is done
// entirely from the right stream with the encoding/group key generated using the
// left equality columns.
func (h *hashJoiner) buildPhase(ctx context.Context) error {
	var scratch []byte
	for {
		rrow, err := h.rightSource.NextRow()
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
			// Output it or throw it away.
			if h.joinType == rightOuter || h.joinType == fullOuter {
				row, _, err := h.render(nil, rrow)
				if err != nil {
					return err
				}
				if row != nil && !h.out.emitRow(ctx, row) {
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
func (h *hashJoiner) probePhase(ctx context.Context) error {
	var scratch []byte

	renderAndEmit := func(lrow sqlbase.EncDatumRow, rrow sqlbase.EncDatumRow,
	) (rowsLeft bool, failedOnCond bool, err error) {
		row, failedOnCond, err := h.render(lrow, rrow)
		if err != nil {
			return false, failedOnCond, err
		}
		if row != nil {
			return h.out.emitRow(ctx, row), failedOnCond, nil
		}
		return true, failedOnCond, nil
	}

	for {
		lrow, err := h.leftSource.NextRow()
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
			// Output it or throw it away.
			if h.joinType == leftOuter || h.joinType == fullOuter {
				if rowsLeft, _, err := renderAndEmit(lrow, nil); err != nil {
					return err
				} else if !rowsLeft {
					return nil
				}
			}
			continue
		}

		b, ok := h.buckets[string(encoded)]
		if !ok {
			if rowsLeft, _, err := renderAndEmit(lrow, nil); err != nil {
				return err
			} else if !rowsLeft {
				return nil
			}
		} else {
			for idx, rrow := range b.rows {
				if rowsLeft, failedOnCond, err := renderAndEmit(lrow, rrow); err != nil {
					return err
				} else if !rowsLeft {
					return nil
				} else if !failedOnCond && (h.joinType == rightOuter || h.joinType == fullOuter) {
					b.seen[idx] = true
				}
			}
		}
	}

	if h.joinType == innerJoin || h.joinType == leftOuter {
		return nil
	}

	// Produce results for unmatched right rows (for RIGHT OUTER or FULL OUTER).
	for _, b := range h.buckets {
		for idx, rrow := range b.rows {
			if !b.seen[idx] {
				if rowsLeft, _, err := renderAndEmit(nil, rrow); err != nil {
					return err
				} else if !rowsLeft {
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
		// Note: we cannot compare VALUE encodings because they contain column IDs
		// which can vary.
		// TODO(radu): we should figure out what encoding is readily available and
		// use that (though it needs to be consistent across all rows). We could add
		// functionality to compare VALUE encodings ignoring the column ID.
		appendTo, err = row[colIdx].Encode(&h.datumAlloc, sqlbase.DatumEncoding_ASCENDING_KEY, appendTo)
		if err != nil {
			return appendTo, false, err
		}
	}
	return appendTo, false, nil
}
