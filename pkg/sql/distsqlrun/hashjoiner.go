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
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlrun

import (
	"sync"
	"unsafe"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// bucket contains the set of rows for a given group key (comprised of
// columns specified by the join constraints).
type bucket struct {
	// rows holds indices of rows into the hashJoiner's rows container.
	rows []int
	// seen is only used for outer joins; there is an entry for each row in `rows`
	// indicating if that row had at least a matching row in the opposite stream
	// ("matching" meaning that the ON condition passed).
	seen []bool
}

// hashJoinerInitialBufferSize controls the size of the initial buffering phase
// (see hashJoiner).
const hashJoinerInitialBufferSize = 4 * 1024 * 1024

const sizeOfBucket = int64(unsafe.Sizeof(bucket{}))
const sizeOfRowIdx = int64(unsafe.Sizeof(int(0)))

// HashJoiner performs a hash join.
//
// It has two input streams and one output. It works in three phases:
//
//  1. Initial buffering: we read and store rows from both streams, up to a
//     certain amount of memory. If we find the end of a stream, this is the
//     stream we will build the buckets from in the next phase. If not, we
//     choose the right stream and read it and buffer it until the end.
//
//  2. Build phase: in this phase we build the buckets from the rows stored
//     in the first phase.
//
//  3. Probe phase: in this phase we process all the rows from the other stream
//     and look for matching rows from the stored stream using the map.
//
// There is no guarantee on the output ordering.
type hashJoiner struct {
	joinerBase

	// initialBufferSize is the maximum amount of data we buffer from each stream
	// as part of the initial buffering phase. Normally
	// hashJoinerInitialBufferSize, can be tweaked for tests.
	initialBufferSize int64

	// eqCols contains the indices of the columns that are constrained to be
	// equal. Specifically column eqCols[0][i] on the left side must match the
	// column eqCols[1][i] on the right side.
	eqCols [2]columns

	// We read a portion of both streams, in the hope that one is small. One of
	// the containers will contain the entire "stored" stream, the other just the
	// start of the other stream.
	rows [2]memRowContainer

	// storedSide is set by the initial buffering phase and indicates which stream
	// we store fully and build the hashtable from.
	storedSide joinSide

	// bucketsAcc is the memory account for the buckets. The datums themselves are
	// all in the rows container.
	bucketsAcc mon.BoundAccount

	scratch []byte

	buckets    map[string]bucket
	datumAlloc sqlbase.DatumAlloc
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
		initialBufferSize: hashJoinerInitialBufferSize,
		buckets:           make(map[string]bucket),
		bucketsAcc:        flowCtx.evalCtx.Mon.MakeBoundAccount(),
	}
	h.eqCols[leftSide] = columns(spec.LeftEqColumns)
	h.eqCols[rightSide] = columns(spec.RightEqColumns)
	h.rows[leftSide] = makeRowContainer(nil /* ordering */, leftSource.Types(), &flowCtx.evalCtx)
	h.rows[rightSide] = makeRowContainer(nil /* ordering */, rightSource.Types(), &flowCtx.evalCtx)

	if err := h.joinerBase.init(
		flowCtx, leftSource, rightSource, spec.Type, spec.OnExpr, post, output,
	); err != nil {
		return nil, err
	}

	return h, nil
}

const sizeOfBoolSlice = unsafe.Sizeof([]bool{})
const sizeOfBool = unsafe.Sizeof(true)

// Run is part of the processor interface.
func (h *hashJoiner) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTag(ctx, "HashJoiner", nil)
	ctx, span := processorSpan(ctx, "hash joiner")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting hash joiner run")
		defer log.Infof(ctx, "exiting hash joiner run")
	}

	defer h.rows[leftSide].Close(ctx)
	defer h.rows[rightSide].Close(ctx)
	defer h.bucketsAcc.Close(ctx)

	if earlyExit, err := h.bufferPhase(ctx); earlyExit || err != nil {
		if err != nil {
			// We got an error. We still want to drain. Any error encountered while
			// draining will be swallowed, and the original error will be forwarded to
			// the consumer.
			log.Infof(ctx, "initial buffering phase error %s", err)
		}
		DrainAndClose(ctx, h.out.output, err /* cause */, h.leftSource, h.rightSource)
		return
	}

	// From this point, we are done with the source for h.storedSide.
	srcToClose := h.leftSource
	if h.storedSide == leftSide {
		srcToClose = h.rightSource
	}

	if err := h.buildPhase(ctx); err != nil {
		log.Infof(ctx, "build phase error %s", err)
		DrainAndClose(ctx, h.out.output, err /* cause */, srcToClose)
		return
	}

	// Allocate seen slices to produce results for unmatched stored rows,
	// for FULL OUTER AND LEFT/RIGHT OUTER (depending on which stream we store).
	if shouldEmitUnmatchedRow(h.storedSide, h.joinType) {
		for k, bucket := range h.buckets {
			if err := h.bucketsAcc.Grow(
				ctx, int64(sizeOfBoolSlice+uintptr(len(bucket.rows))*sizeOfBool),
			); err != nil {
				DrainAndClose(ctx, h.out.output, err, srcToClose)
				return
			}
			bucket.seen = make([]bool, len(bucket.rows))
			h.buckets[k] = bucket
		}
	}
	log.VEventf(ctx, 1, "build phase complete")
	if earlyExit, err := h.probePhase(ctx); earlyExit || err != nil {
		if err != nil {
			// We got an error. We still want to drain. Any error encountered while
			// draining will be swallowed, and the original error will be forwarded to
			// the consumer. Note that rightSource has already been drained at this
			// point.
			log.Infof(ctx, "probe phase error %s", err)
		}
		DrainAndClose(ctx, h.out.output, err /* cause */, srcToClose)
	}
}

// receiveRow receives a row from either the left or right stream.
// It takes care of forwarding any metadata, and processes any rows that have
// NULL on an equality column - these rows will not match anything, they are
// routed directly to the output if appropriate (depending on the type of join)
// and then discarded.
// If earlyExit is set, the output doesn't need more rows.
func (h *hashJoiner) receiveRow(
	ctx context.Context, src RowSource, side joinSide,
) (_ sqlbase.EncDatumRow, earlyExit bool, _ error) {
	for {
		row, meta := src.Next()
		if row == nil {
			if meta.Empty() {
				// Done.
				return nil, false, nil
			}
			if meta.Err != nil {
				return nil, false, meta.Err
			}
			if h.out.output.Push(nil /* row */, meta) != NeedMoreRows {
				return nil, true, nil
			}
			continue
		}

		// See if we have NULLs on equality columns.
		hasNull := false
		for _, c := range h.eqCols[side] {
			if row[c].IsNull() {
				hasNull = true
				break
			}
		}
		if !hasNull {
			// Normal path.
			return row, false, nil
		}

		if !h.maybeEmitUnmatchedRow(ctx, row, side) {
			return nil, true, nil
		}
	}
}

// bufferPhase is an initial phase where we read a portion of both streams,
// in the hope that one of them is small.
//
// Rows that contain NULLs on equality columns go straight to the output if it's
// an outer join; otherwise they are discarded.
//
// A successful initial buffering phase sets h.storedSide.
//
// If earlyExit is set, the output doesn't need more rows.
func (h *hashJoiner) bufferPhase(ctx context.Context) (earlyExit bool, _ error) {
	srcs := [2]RowSource{h.leftSource, h.rightSource}
	for {
		leftUsage := h.rows[leftSide].MemUsage()
		rightUsage := h.rows[rightSide].MemUsage()
		if leftUsage >= h.initialBufferSize && rightUsage >= h.initialBufferSize {
			break
		}
		side := rightSide
		if leftUsage < rightUsage {
			side = leftSide
		}

		row, earlyExit, err := h.receiveRow(ctx, srcs[side], side)
		if row == nil {
			if err != nil {
				return false, err
			}
			if earlyExit {
				return true, nil
			}

			// This stream is done, great! We will build the hashtable using this
			// stream.
			h.storedSide = side
			return false, nil
		}
		// Add the row to the correct container.
		if err := h.rows[side].AddRow(ctx, row); err != nil {
			return false, err
		}
	}

	// We did not find a short stream. Stop reading for both streams, just
	// choose the right stream and consume it.
	h.storedSide = rightSide

	for {
		row, earlyExit, err := h.receiveRow(ctx, h.rightSource, rightSide)
		if row == nil {
			if err != nil {
				return false, err
			}
			return earlyExit, nil
		}
		if err := h.rows[rightSide].AddRow(ctx, row); err != nil {
			return false, err
		}
	}
}

// buildPhase constructs our internal hash map of rows seen. This is done
// entirely from one stream (chosen during initial buffering) with the
// encoding/group key generated using the equality columns. If a row is found to
// have a NULL in an equality column (and thus will not match anything), it
// might be routed directly to the output (for outer joins). In such cases it is
// possible that the buildPhase will fully satisfy the consumer.
func (h *hashJoiner) buildPhase(ctx context.Context) error {
	storedRows := &h.rows[h.storedSide]

	for rowIdx := 0; rowIdx < storedRows.Len(); rowIdx++ {
		row := storedRows.EncRow(rowIdx)

		encoded, hasNull, err := encodeColumnsOfRow(
			&h.datumAlloc, h.scratch, row, h.eqCols[h.storedSide], false, /* encodeNull */
		)
		if err != nil {
			return err
		}

		h.scratch = encoded[:0]

		if hasNull {
			panic("NULLs not detected during receive")
		}

		b, bucketExists := h.buckets[string(encoded)]

		// Acount for the memory usage of rowIdx, map key, and bucket.
		usage := sizeOfRowIdx
		if !bucketExists {
			usage += int64(len(encoded))
			usage += sizeOfBucket
		}

		if err := h.bucketsAcc.Grow(ctx, usage); err != nil {
			return err
		}

		b.rows = append(b.rows, rowIdx)
		h.buckets[string(encoded)] = b
	}
	return nil
}

func (h *hashJoiner) probeRow(
	ctx context.Context, row sqlbase.EncDatumRow,
) (earlyExit bool, _ error) {
	side := otherSide(h.storedSide)
	encoded, hasNull, err := encodeColumnsOfRow(
		&h.datumAlloc, h.scratch, row, h.eqCols[side], false, /* encodeNull */
	)
	if err != nil {
		return false, err
	}
	h.scratch = encoded[:0]

	if hasNull {
		panic("NULLs not detected during receive")
	}

	matched := false
	if b, ok := h.buckets[string(encoded)]; ok {
		for i, otherRowIdx := range b.rows {
			otherRow := h.rows[h.storedSide].EncRow(otherRowIdx)

			var renderedRow sqlbase.EncDatumRow
			var err error
			if h.storedSide == rightSide {
				renderedRow, err = h.render(row, otherRow)
			} else {
				renderedRow, err = h.render(otherRow, row)
			}

			if err != nil {
				return false, err
			}
			// If the ON condition failed, renderedRow is nil.
			if renderedRow != nil {
				matched = true
				if b.seen != nil {
					// Mark the right row as matched (for right/full outer join).
					b.seen[i] = true
				}
				consumerStatus, err := h.out.emitRow(ctx, renderedRow)
				if err != nil || consumerStatus != NeedMoreRows {
					return true, nil
				}
			}
		}
	}

	if !matched && !h.maybeEmitUnmatchedRow(ctx, row, otherSide(h.storedSide)) {
		return true, nil
	}
	return false, nil
}

// probePhase uses our constructed hash map of rows seen from the right stream,
// we probe the map for each row retrieved from the left stream outputting the
// merging of the two rows if matched. Behaviour for outer joins is as expected,
// i.e. for RIGHT OUTER joins if no corresponding left row is seen an empty
// DNull row is emitted instead.
//
// In error or earlyExit cases it is the caller's responsibility to drain the
// input stream and close the output stream.
func (h *hashJoiner) probePhase(ctx context.Context) (earlyExit bool, _ error) {
	side := otherSide(h.storedSide)

	src := h.leftSource
	if side == rightSide {
		src = h.rightSource
	}
	bufferedRows := &h.rows[side]
	// First process the rows that were already buffered.
	for i := 0; i < bufferedRows.Len(); i++ {
		row := bufferedRows.EncRow(i)
		earlyExit, err := h.probeRow(ctx, row)
		if earlyExit || err != nil {
			return earlyExit, err
		}
	}
	bufferedRows.Clear(ctx)

	for {
		row, earlyExit, err := h.receiveRow(ctx, src, side)
		if row == nil {
			if earlyExit || err != nil {
				return earlyExit, err
			}
			break
		}
		if earlyExit, err := h.probeRow(ctx, row); earlyExit || err != nil {
			return earlyExit, err
		}
	}

	if shouldEmitUnmatchedRow(h.storedSide, h.joinType) {
		// Produce results for unmatched rows, for FULL OUTER AND LEFT/RIGHT OUTER
		// (depending on which stream we use).
		storedRows := &h.rows[h.storedSide]
		for _, b := range h.buckets {
			for i, seen := range b.seen {
				if !seen && !h.maybeEmitUnmatchedRow(ctx, storedRows.EncRow(b.rows[i]), h.storedSide) {
					return true, nil
				}
			}
		}
	}

	sendTraceData(ctx, h.out.output)
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
