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
func (h *hashJoiner) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTag(ctx, "HashJoiner", nil)
	ctx, span := tracing.ChildSpan(ctx, "hash joiner")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting hash joiner run")
		defer log.Infof(ctx, "exiting hash joiner run")
	}

	moreRows, err := h.buildPhase(ctx)
	if err != nil {
		// We got an error. We still want to drain. Any error encountered while
		// draining will be swallowed, and the original error will be forwarded to
		// the consumer.
		DrainAndClose(ctx, h.out.output, err /* cause */, h.leftSource, h.rightSource)
		return
	}
	if !moreRows {
		return
	}

	if h.joinType == rightOuter || h.joinType == fullOuter {
		for k, bucket := range h.buckets {
			bucket.seen = make([]bool, len(bucket.rows))
			h.buckets[k] = bucket
		}
	}
	log.VEventf(ctx, 1, "build phase complete")
	moreRows, err = h.probePhase(ctx)
	if moreRows || err != nil {
		// We got an error. We still want to drain. Any error encountered while
		// draining will be swallowed, and the original error will be forwarded to
		// the consumer. Note that rightSource has already been drained at this
		// point.
		DrainAndClose(ctx, h.out.output, err /* cause */, h.leftSource)
	}
}

// buildPhase constructs our internal hash map of rows seen. This is done
// entirely from the right stream with the encoding/group key generated using
// the left equality columns. If a row is found to have a NULL in an equality
// column (and thus will not match anything), it might be routed directly to the
// output (for outer joins). In such cases it is possible that the buildPhase
// will fully satisfy the consumer.
//
// Returns true if more rows are needed to be passed to the output, false
// otherwise. If it returns false, both the inputs and the output have been
// properly drained and/or closed.
// If true is returned, the right input has been drained.
// If an error is returned, the inputs/output have not been drained or closed.
func (h *hashJoiner) buildPhase(ctx context.Context) (bool, error) {
	var scratch []byte
	for {
		rrow, meta := h.rightSource.Next()
		if !meta.Empty() {
			if meta.Err != nil {
				return true, meta.Err
			}
			if !emitHelper(
				ctx, &h.out, nil /* row */, meta, h.leftSource, h.rightSource) {
				return false, nil
			}
			continue
		}
		if rrow == nil {
			return true, nil
		}

		encoded, hasNull, err := encodeColumnsOfRow(&h.datumAlloc, scratch, rrow, h.rightEqCols, false /* encodeNull */)
		if err != nil {
			return false, err
		}

		scratch = encoded[:0]

		if hasNull {
			// A row that has a NULL in an equality column will not match anything.
			// Output it or throw it away.
			if h.joinType == rightOuter || h.joinType == fullOuter {
				row, _, err := h.render(nil, rrow)
				if err != nil {
					return false, err
				}
				if row == nil {
					continue
				}
				if !emitHelper(ctx, &h.out, row, ProducerMetadata{}, h.leftSource, h.rightSource) {
					return false, nil
				}
			}
			continue
		}

		b := h.buckets[string(encoded)]
		b.rows = append(b.rows, rrow)
		h.buckets[string(encoded)] = b
	}
}

// probePhase uses our constructed hash map of rows seen from the right stream,
// we probe the map for each row retrieved from the left stream outputting the
// merging of the two rows if matched. Behaviour for outer joins is as expected,
// i.e. for RIGHT OUTER joins if no corresponding left row is seen an empty
// DNull row is emitted instead.
//
// Returns false is both the inputs and the output have been properly drained
// and/or closed. Returns true if the caller needs to do the draining.
// If an error is returned, the inputs/output have not been drained or closed.
// The return values are symmetric with buildPhase().
func (h *hashJoiner) probePhase(ctx context.Context) (bool, error) {
	var scratch []byte

	// If moreRowsNeeded is returned false, then both the input and the output
	// have been drained and closed.
	// If an error is returned, the input/output have not been drained and closed.
	renderAndEmit := func(lrow sqlbase.EncDatumRow, rrow sqlbase.EncDatumRow,
	) (moreRowsNeeded bool, failedOnCond bool, err error) {
		row, failedOnCond, err := h.render(lrow, rrow)
		if err != nil {
			return false, false, err
		}
		if row != nil {
			moreRowsNeeded := emitHelper(ctx, &h.out, row, ProducerMetadata{}, h.leftSource)
			return moreRowsNeeded, failedOnCond, nil
		}
		return true, failedOnCond, nil
	}

	for {
		lrow, meta := h.leftSource.Next()
		if !meta.Empty() {
			if meta.Err != nil {
				return true, meta.Err
			}
			if !emitHelper(
				ctx, &h.out, nil /* row */, meta, h.leftSource, h.rightSource) {
				return false, nil
			}
			continue
		}

		if lrow == nil {
			break
		}

		encoded, hasNull, err := encodeColumnsOfRow(&h.datumAlloc, scratch, lrow, h.leftEqCols, false /* encodeNull */)
		if err != nil {
			return true, err
		}
		scratch = encoded[:0]

		if hasNull {
			// A row that has a NULL in an equality column will not match anything.
			// Output it or throw it away.
			if h.joinType == leftOuter || h.joinType == fullOuter {
				moreRowsNeeded, _, err := renderAndEmit(lrow, nil)
				if !moreRowsNeeded || err != nil {
					return moreRowsNeeded, err
				}
			}
			continue
		}

		b, ok := h.buckets[string(encoded)]
		if !ok {
			if moreRowsNeeded, _, err := renderAndEmit(lrow, nil); !moreRowsNeeded || err != nil {
				return moreRowsNeeded, err
			}
		} else {
			for idx, rrow := range b.rows {
				if moreRowsNeeded, failedOnCond, err := renderAndEmit(lrow, rrow); !moreRowsNeeded || err != nil {
					return moreRowsNeeded, err
				} else if !failedOnCond && (h.joinType == rightOuter || h.joinType == fullOuter) {
					b.seen[idx] = true
				}
			}
		}
	}

	if h.joinType == innerJoin || h.joinType == leftOuter {
		return true, nil
	}

	// Produce results for unmatched right rows (for RIGHT OUTER or FULL OUTER).
	for _, b := range h.buckets {
		for idx, rrow := range b.rows {
			if !b.seen[idx] {
				if moreRowsNeeded, _, err := renderAndEmit(nil, rrow); !moreRowsNeeded || err != nil {
					return moreRowsNeeded, err
				}
			}
		}
	}
	h.out.close()
	return false, nil
}

// encodeColumnsOfRow returns the encoding for the grouping columns. This is
// then used as our group key to determine which bucket to add to.
// If the row contains any NULLs and encodeNull is false, hasNull is true and
// no encoding is returned. If encodeNull is true, hasNull is never set.
func encodeColumnsOfRow(
	da *sqlbase.DatumAlloc, appendTo []byte, row sqlbase.EncDatumRow, cols columns, encodeNull bool,
) (encoding []byte, hasNull bool, err error) {
	for _, colIdx := range cols {
		if row[colIdx].IsNull() && !encodeNull {
			return nil, true, nil
		}
		// Note: we cannot compare VALUE encodings because they contain column IDs
		// which can vary.
		// TODO(radu): we should figure out what encoding is readily available and
		// use that (though it needs to be consistent across all rows). We could add
		// functionality to compare VALUE encodings ignoring the column ID.
		appendTo, err = row[colIdx].Encode(da, sqlbase.DatumEncoding_ASCENDING_KEY, appendTo)
		if err != nil {
			return appendTo, false, err
		}
	}
	return appendTo, false, nil
}
