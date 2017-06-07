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
//     certain amount of memory. If we find the end of a stream, we choose to
//     store that stream in the next phase. If not, we choose the right stream.
//
//  2. Build phase: in this phase we store all the rows from the stored stream
//     (selected in the previous phase) and create a map from encoded equality
//     columns to row indices.
//
//  3. Probe phase: in this phase we process all the rows from the other stream
//     and look for matching rows from the stored stream using the map.
//
// There is no guarantee on the output ordering.
type hashJoiner struct {
	joinerBase

	// We read a portion of both streams, in the hope that one is small - in which
	// case we store that one in the map.
	leftRows  rowContainer
	rightRows rowContainer

	bufferedRows *rowContainer
	storedRows   *rowContainer

	// bucketsAcc is the memory account for the buckets. The datums themselves are
	// all in the rows container.
	bucketsAcc mon.BoundAccount

	scratch []byte

	// initialBufferSize is the maximum amount of data we buffer from each stream
	// as part of the initial buffering phase. Normally
	// hashJoinerInitialBufferSize, can be tweaked for tests.
	initialBufferSize int64

	// storeLeft is set by the initial buffering phase. If it is true, we store
	// the left stream in the map; otherwise we store the right stream.
	storeLeft bool
	// storeStreamConsumed is set by the initial buffering phase and indicates
	// that we have already reached the end of the stream we want to store.
	storeStreamConsumed bool

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
		leftEqCols:        columns(spec.LeftEqColumns),
		rightEqCols:       columns(spec.RightEqColumns),
		buckets:           make(map[string]bucket),
		bucketsAcc:        flowCtx.evalCtx.Mon.MakeBoundAccount(),
		leftRows:          makeRowContainer(nil /* ordering */, leftSource.Types(), &flowCtx.evalCtx),
		rightRows:         makeRowContainer(nil /* ordering */, rightSource.Types(), &flowCtx.evalCtx),
		initialBufferSize: hashJoinerInitialBufferSize,
	}

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
	ctx, span := tracing.ChildSpan(ctx, "hash joiner")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting hash joiner run")
		defer log.Infof(ctx, "exiting hash joiner run")
	}

	defer h.bucketsAcc.Close(ctx)
	defer h.leftRows.Close(ctx)
	defer h.rightRows.Close(ctx)

	if earlyExit, err := h.initialBuffering(ctx); err != nil {
		// We got an error. We still want to drain. Any error encountered while
		// draining will be swallowed, and the original error will be forwarded to
		// the consumer.
		log.Infof(ctx, "initial buffering phase error %s", err)
		DrainAndClose(ctx, h.out.output, err /* cause */, h.leftSource, h.rightSource)
		return
	} else if earlyExit {
		return
	}

	if earlyExit, err := h.buildPhase(ctx); err != nil {
		log.Infof(ctx, "build phase error %s", err)
		DrainAndClose(ctx, h.out.output, err /* cause */, h.leftSource, h.rightSource)
		return
	} else if earlyExit {
		return
	}

	// Allocate seen slices to produce results for unmatched stored rows,
	// for FULL OUTER AND LEFT/RIGHT OUTER (depending on which stream we store).
	if shouldEmitUnmatchedRow(h.storeLeft, h.joinType) {
		for k, bucket := range h.buckets {
			if err := h.bucketsAcc.Grow(
				ctx, int64(sizeOfBoolSlice+uintptr(len(bucket.rows))*sizeOfBool),
			); err != nil {
				DrainAndClose(ctx, h.out.output, err, h.leftSource)
				return
			}
			bucket.seen = make([]bool, len(bucket.rows))
			h.buckets[k] = bucket
		}
	}
	log.VEventf(ctx, 1, "build phase complete")
	if err := h.probePhase(ctx); err != nil {
		// We got an error. We still want to drain. Any error encountered while
		// draining will be swallowed, and the original error will be forwarded to
		// the consumer. Note that rightSource has already been drained at this
		// point.
		log.Infof(ctx, "probe phase error %s", err)
		DrainAndClose(ctx, h.out.output, err /* cause */, h.leftSource)
	}
}

// initialBuffering is an initial phase where we read a portion of both streams,
// in the hope that one of them is small.
//
// A successful initial buffering phase sets the storeLeft, storeConsumed,
// storedRows, bufferedRows fields.
//
// The inputsDrained return flag is set if there was no error and our consumer
// indicated it does not need more data; in this case both inputs have been
// drained and closed. In all other cases it is the caller responsibility to
// drain and close the inputs.
func (h *hashJoiner) initialBuffering(ctx context.Context) (inputsDrained bool, _ error) {
	for {
		leftUsage := h.leftRows.MemUsage()
		rightUsage := h.rightRows.MemUsage()
		if leftUsage >= h.initialBufferSize && rightUsage >= h.initialBufferSize {
			// We did not find a short stream. Store the right side.
			h.storeLeft = false
			h.storeStreamConsumed = false
			h.storedRows, h.bufferedRows = &h.rightRows, &h.leftRows
			return false, nil
		}
		leftSide := leftUsage < rightUsage
		src, container := h.rightSource, &h.rightRows
		if leftSide {
			src, container = h.leftSource, &h.leftRows
		}

		row, meta := src.Next()
		if !meta.Empty() {
			if meta.Err != nil {
				return false, meta.Err
			}
			if !emitHelper(ctx, &h.out, nil /* row */, meta, h.leftSource, h.rightSource) {
				return true, nil
			}
			continue
		}
		if row == nil {
			// This stream is done, great! We will store this stream in the hashtable.
			h.storeLeft = leftSide
			h.storeStreamConsumed = true
			if leftSide {
				h.storedRows, h.bufferedRows = &h.leftRows, &h.rightRows
			} else {
				h.storedRows, h.bufferedRows = &h.rightRows, &h.leftRows
			}
			return false, nil
		}

		if err := container.AddRow(ctx, row); err != nil {
			return false, err
		}
	}
}

// addRow adds a row to the buckets. If rowIdx is not -1, the row is already in
// the container at this index; otherwise the row is added to the container.
func (h *hashJoiner) addRow(
	ctx context.Context, row sqlbase.EncDatumRow, isLeftStream bool, rowIdx int,
) (inputsDrained bool, _ error) {
	eqCols := h.rightEqCols
	if isLeftStream {
		eqCols = h.leftEqCols
	}

	encoded, hasNull, err := encodeColumnsOfRow(
		&h.datumAlloc, h.scratch, row, eqCols, false, /* encodeNull */
	)
	if err != nil {
		return false, err
	}

	h.scratch = encoded[:0]

	if hasNull {
		inputsDrained := !h.maybeEmitUnmatchedRow(ctx, row, isLeftStream)
		return inputsDrained, nil
	}

	if rowIdx == -1 {
		rowIdx = h.storedRows.Len()
		if err := h.storedRows.AddRow(ctx, row); err != nil {
			return false, err
		}
	}

	b, bucketExists := h.buckets[string(encoded)]

	// Acount for the memory usage of rowIdx, map key, and bucket.
	usage := sizeOfRowIdx
	if !bucketExists {
		usage += int64(len(encoded))
		usage += sizeOfBucket
	}

	if err := h.bucketsAcc.Grow(ctx, usage); err != nil {
		return false, err
	}

	b.rows = append(b.rows, rowIdx)
	h.buckets[string(encoded)] = b
	return false, nil
}

// buildPhase constructs our internal hash map of rows seen. This is done
// entirely from one stream (chosen during initial buffering) with the
// encoding/group key generated using the equality columns. If a row is found to
// have a NULL in an equality column (and thus will not match anything), it
// might be routed directly to the output (for outer joins). In such cases it is
// possible that the buildPhase will fully satisfy the consumer.
//
// The inputsDrained return flag is set if there was no error and our consumer
// indicated it does not need more data; in this case both inputs have been
// drained and closed. In all other cases it is the caller's responsibility to
// drain and close the inputs.
func (h *hashJoiner) buildPhase(ctx context.Context) (inputsDrained bool, _ error) {
	isLeftStream := h.storeLeft
	// First process the rows that were already buffered.
	for i := 0; i < h.storedRows.Len(); i++ {
		row := h.storedRows.EncRow(i)
		inputsDrained, err := h.addRow(ctx, row, isLeftStream, i)
		if inputsDrained || err != nil {
			return inputsDrained, err
		}
	}
	// Add the rest of the stream.
	if h.storeStreamConsumed {
		return
	}
	src := h.rightSource
	if isLeftStream {
		src = h.leftSource
	}
	for {
		row, meta := src.Next()
		if !meta.Empty() {
			if meta.Err != nil {
				return false, meta.Err
			}
			if !emitHelper(ctx, &h.out, nil /* row */, meta, h.leftSource, h.rightSource) {
				return true, nil
			}
			continue
		}
		if row == nil {
			return false, nil
		}

		inputsDrained, err := h.addRow(ctx, row, isLeftStream, -1)
		if inputsDrained || err != nil {
			return inputsDrained, err
		}
	}
}

func (h *hashJoiner) probeRow(
	ctx context.Context, row sqlbase.EncDatumRow, isLeftStream bool,
) (inputsDrained bool, _ error) {
	eqCols := h.rightEqCols
	if isLeftStream {
		eqCols = h.leftEqCols
	}

	encoded, hasNull, err := encodeColumnsOfRow(&h.datumAlloc, h.scratch, row, eqCols, false /* encodeNull */)
	if err != nil {
		return false, err
	}
	h.scratch = encoded[:0]

	matched := false

	// A row that has a NULL in an equality column cannot match anything.
	if !hasNull {
		if b, ok := h.buckets[string(encoded)]; ok {
			for i, otherRowIdx := range b.rows {
				otherRow := h.storedRows.EncRow(otherRowIdx)

				var renderedRow sqlbase.EncDatumRow
				var err error
				if isLeftStream {
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
					if !emitHelper(ctx, &h.out, renderedRow, ProducerMetadata{}, h.leftSource) {
						return true, nil
					}
				}
			}
		}
	}

	if !matched && !h.maybeEmitUnmatchedRow(ctx, row, isLeftStream) {
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
// In error cases it is the caller's responsibility to drain the input stream and
// close the output stream.
func (h *hashJoiner) probePhase(ctx context.Context) error {
	isLeftStream := !h.storeLeft
	src := h.rightSource
	if isLeftStream {
		src = h.leftSource
	}
	// First process the rows that were already buffered.
	for i := 0; i < h.bufferedRows.Len(); i++ {
		row := h.bufferedRows.EncRow(i)
		inputsDrained, err := h.probeRow(ctx, row, isLeftStream)
		if inputsDrained || err != nil {
			return err
		}
	}
	h.bufferedRows.Clear(ctx)

	for {
		row, meta := src.Next()
		if !meta.Empty() {
			if meta.Err != nil {
				return meta.Err
			}
			if !emitHelper(
				ctx, &h.out, nil /* row */, meta, h.leftSource, h.rightSource) {
				return nil
			}
			continue
		}

		if row == nil {
			break
		}
		inputsDrained, err := h.probeRow(ctx, row, isLeftStream)
		if inputsDrained || err != nil {
			return err
		}
	}

	if shouldEmitUnmatchedRow(h.storeLeft, h.joinType) {
		// Produce results for unmatched rows, for FULL OUTER AND LEFT/RIGHT OUTER
		// (depending on which stream we use).
		for _, b := range h.buckets {
			for i, seen := range b.seen {
				if !seen && !h.maybeEmitUnmatchedRow(ctx, h.storedRows.EncRow(b.rows[i]), h.storeLeft) {
					return nil
				}
			}
		}
	}

	h.out.close()
	return nil
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
