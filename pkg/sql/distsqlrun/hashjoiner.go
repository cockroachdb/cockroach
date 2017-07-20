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
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// hashJoinerInitialBufferSize controls the size of the initial buffering phase
// (see hashJoiner).
const hashJoinerInitialBufferSize = 4 * 1024 * 1024

// hashJoinPhases are used to describe phases of work in the hashJoiner. Used
// in tests to specify a phase in which the hashJoiner should error out.
type hashJoinPhase int

const (
	unset hashJoinPhase = iota
	buffer
	build
	probe
)

func (p hashJoinPhase) String() string {
	switch p {
	case unset:
		return ""
	case buffer:
		return "BufferPhase"
	case build:
		return "BuildPhase"
	case probe:
		return "ProbePhase"
	default:
		panic(fmt.Sprintf("invalid test fail point %d", p))
	}
}

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

	flowCtx *FlowCtx

	// initialBufferSize is the maximum amount of data we buffer from each stream
	// as part of the initial buffering phase. Normally
	// hashJoinerInitialBufferSize, can be tweaked for tests.
	initialBufferSize int64

	// We read a portion of both streams, in the hope that one is small. One of
	// the containers will contain the entire "stored" stream, the other just the
	// start of the other stream.
	// TODO(asubiotto): This does not need to be part of the hashJoiner struct.
	rows [2]memRowContainer

	// storedSide is set by the initial buffering phase and indicates which
	// stream we store fully and build the hashRowContainer from.
	storedSide joinSide

	// testingKnobMemLimit is used in testing to set a limit on the memory that
	// should be used by the hashJoiner. Minimum value to enable is 1.
	testingKnobMemLimit int64
	// If set to buffer, build, or probe, the hashJoiner will error out with a
	// memory error after the call that would increase memory usage (there is
	// only one per phase). This is used by testing to verify that a memory
	// failure in specific phases will fall back to disk correctly.
	testingKnobMemFailPoint hashJoinPhase
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
		flowCtx:           flowCtx,
		initialBufferSize: hashJoinerInitialBufferSize,
	}

	numMergedColumns := 0
	if spec.MergedColumns {
		numMergedColumns = len(spec.LeftEqColumns)
	}
	if err := h.joinerBase.init(
		flowCtx, leftSource, rightSource, spec.Type, spec.OnExpr, spec.LeftEqColumns, spec.RightEqColumns, uint32(numMergedColumns), post, output,
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
	ctx, span := processorSpan(ctx, "hash joiner")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting hash joiner run")
		defer log.Infof(ctx, "exiting hash joiner run")
	}

	useTempStorage := distSQLUseTempStorage.Get() ||
		h.testingKnobMemLimit > 0 ||
		h.testingKnobMemFailPoint != unset
	evalCtx := h.flowCtx.evalCtx
	if useTempStorage {
		// Limit the memory use by creating a child monitor with a hard limit.
		// The hashJoiner will overflow to disk if this limit is not enough.
		limit := h.testingKnobMemLimit
		if limit <= 0 {
			limit = workMemBytes
		}
		limitedMon := mon.MakeMonitorInheritWithLimit("hashjoiner-limited", limit, evalCtx.Mon)
		limitedMon.Start(ctx, evalCtx.Mon, mon.BoundAccount{})
		defer limitedMon.Stop(ctx)

		evalCtx.Mon = &limitedMon
	}

	h.rows[leftSide] = makeRowContainer(nil /* ordering */, h.leftSource.Types(), &evalCtx)
	h.rows[rightSide] = makeRowContainer(nil /* ordering */, h.rightSource.Types(), &evalCtx)
	defer h.rows[leftSide].Close(ctx)
	defer h.rows[rightSide].Close(ctx)

	var storedRows hashRowContainer
	defer func() {
		if storedRows != nil {
			storedRows.Close(ctx)
		}
	}()
	// processMemoryError is a helper function that processes an error. If this
	// error is a memory error, this function determines whether external
	// storage can be used to continue the Run() and returns (true, nil) if so.
	// If external storage cannot be used, the given error is wrapped with the
	// reason why not.
	// If the given error is not a memory error, the same error is returned.
	processMemoryError := func(err error) (bool, error) {
		if err == nil {
			return false, nil
		}
		if pgErr, ok := err.(*pgerror.Error); !(ok && pgErr.Code == pgerror.CodeOutOfMemoryError) {
			return false, err
		}
		if !useTempStorage {
			return false, errors.Wrap(err, "external storage for large queries disabled")
		}
		if h.flowCtx.tempStorage == nil {
			return false, errors.Wrap(err, "external storage not provided on this cockroach node")
		}
		return true, nil
	}
	// fallBackToDisk is a helper function that creates a hashDiskRowContainer
	// from h.rows[h.storedSide] and sets storedRows to it, closing the
	// previous storedRows hashRowContainer if any. The given row is added
	// to the container.
	fallBackToDisk := func(row sqlbase.EncDatumRow) error {
		if h.testingKnobMemFailPoint != unset {
			h.testingKnobMemFailPoint = unset
		}

		storedDiskRows := makeHashDiskRowContainer(&h.rows[h.storedSide], h.flowCtx.tempStorage)
		if err := storedDiskRows.Init(
			ctx, h.eqCols[h.storedSide], h.eqCols[otherSide(h.storedSide)],
		); err != nil {
			return err
		}
		if storedRows != nil {
			storedRows.Close(ctx)
		}
		storedRows = &storedDiskRows

		if row != nil {
			if err := storedRows.AddRow(ctx, row); err != nil {
				return err
			}
		}
		return nil
	}

	streamConsumed, earlyExit, err := func() (bool, bool, error) {
		row, streamConsumed, earlyExit, err := h.bufferPhase(ctx)
		if earlyExit {
			return streamConsumed, earlyExit, err
		}
		if useDisk, err := processMemoryError(err); !useDisk {
			return streamConsumed, earlyExit, err
		}

		log.VEventf(ctx, 2, "buffer phase falling back to disk")
		return streamConsumed, false, fallBackToDisk(row)
	}()
	if earlyExit || err != nil {
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

	// If storedRows is not nil, there was a memory limit reached in the buffer
	// phase so we have already fallen back to disk.
	if storedRows == nil {
		storedMemRows := makeHashMemRowContainer(ctx, &h.rows[h.storedSide])
		if err := storedMemRows.Init(
			ctx, h.eqCols[h.storedSide], h.eqCols[otherSide(h.storedSide)],
		); err != nil {
			// We got an error. We still want to drain. Any error encountered
			// while draining will be swallowed, and the original error will be
			// forwarded to the consumer.
			err = errors.Wrap(err, "error creating hash row container")
			log.Info(ctx, err)
			DrainAndClose(ctx, h.out.output, err /* cause */, h.leftSource, h.rightSource)
			return
		}
		storedRows = &storedMemRows
	}

	// If the buffer phase did not fully consume the chosen stream, we proceed
	// to a build phase which will do so.
	if !streamConsumed {
		if earlyExit, err := func() (bool, error) {
			row, earlyExit, err := h.buildPhase(ctx, storedRows)
			if earlyExit {
				return earlyExit, err
			}
			if useDisk, err := processMemoryError(err); !useDisk {
				return false, err
			}

			log.VEventf(ctx, 2, "build phase falling back to disk")
			if err := fallBackToDisk(row); err != nil {
				return false, err
			}

			if _, earlyExit, err := h.buildPhase(ctx, storedRows); earlyExit || err != nil {
				return earlyExit, err
			}
			return false, nil
		}(); earlyExit || err != nil {
			if err != nil {
				log.Infof(ctx, "build phase error %s", err)
			}
			DrainAndClose(ctx, h.out.output, err /* cause */, srcToClose)
			return
		}
	}
	log.VEventf(ctx, 1, "build phase complete")

	if earlyExit, err := func() (bool, error) {
		// The probe phase can run out of memory because of the data structure
		// used to keep track of marked rows.
		row, earlyExit, err := h.probePhase(ctx, storedRows)
		if earlyExit {
			return earlyExit, err
		}
		if useDisk, err := processMemoryError(err); !useDisk {
			return earlyExit, err
		}

		// NOTE: Due to the current hashMemRowContainer implementation, the
		// probe phase can only run out of memory once the first row to mark
		// is marked. Therefore, there is no marking information to transfer
		// to the hashDiskRowContainer.
		log.VEventf(ctx, 2, "probe phase falling back to disk")
		if err := fallBackToDisk(nil); err != nil {
			return false, err
		}

		// Probe with the row that caused the memory error.
		if earlyExit, err := h.probeRow(ctx, row, storedRows); earlyExit || err != nil {
			return earlyExit, err
		}

		if _, earlyExit, err := h.probePhase(ctx, storedRows); earlyExit || err != nil {
			return earlyExit, err
		}
		return false, nil
	}(); earlyExit || err != nil {
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
// A successful initial buffering phase or an error while adding a row sets
// h.storedSide.
//
// If an error occurs while adding a row to a container, the row is returned in
// order to not lose it. In this case, h.storedSide is set to the side that this
// row would have been added to.
//
// If streamConsumed is set, the chosen stream has been fully consumed.
//
// If earlyExit is set, the output doesn't need more rows.
func (h *hashJoiner) bufferPhase(
	ctx context.Context,
) (row sqlbase.EncDatumRow, streamConsumed bool, earlyExit bool, _ error) {
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
				return nil, false, false, err
			}
			if earlyExit {
				return nil, false, true, nil
			}

			// This stream is done, great! We will build the hashtable using this
			// stream.
			h.storedSide = side
			return nil, true, false, nil
		}
		// Add the row to the correct container.
		if err := h.rows[side].AddRow(ctx, row); err != nil {
			h.storedSide = side
			return row, false, false, err
		}
		if h.testingKnobMemFailPoint == buffer {
			return nil, false, false, pgerror.NewErrorf(
				pgerror.CodeOutOfMemoryError,
				"%s test induced error",
				h.testingKnobMemFailPoint,
			)
		}
	}

	// We did not find a short stream. Stop reading for both streams, just
	// choose the right stream.
	h.storedSide = rightSide
	return nil, false, false, nil
}

// buildPhase constructs our internal hash map of rows seen. This is done
// entirely from one stream (chosen during initial buffering) with the
// encoding/group key generated using the equality columns. If a row is found to
// have a NULL in an equality column (and thus will not match anything), it
// might be routed directly to the output (for outer joins). In such cases it is
// possible that the buildPhase will fully satisfy the consumer.
//
// If an error occurs while adding a row to the given container, the row is
// returned in order to not lose it.
func (h *hashJoiner) buildPhase(
	ctx context.Context, storedRows hashRowContainer,
) (_ sqlbase.EncDatumRow, earlyExit bool, _ error) {
	// Consume the rest of the stream chosen to be stored.
	source := h.rightSource
	if h.storedSide == leftSide {
		source = h.leftSource
	}

	for {
		row, earlyExit, err := h.receiveRow(ctx, source, h.storedSide)
		if row == nil {
			if err != nil {
				return nil, false, err
			}
			return nil, earlyExit, nil
		}
		if err := storedRows.AddRow(ctx, row); err != nil {
			return row, false, err
		}
		if h.testingKnobMemFailPoint == build {
			return nil, false, pgerror.NewErrorf(
				pgerror.CodeOutOfMemoryError,
				"%s test induced error",
				h.testingKnobMemFailPoint,
			)
		}
	}
}

func (h *hashJoiner) probeRow(
	ctx context.Context, row sqlbase.EncDatumRow, storedRows hashRowContainer,
) (earlyExit bool, _ error) {
	// probeMatched specifies whether the row we are probing with has at least
	// one match.
	probeMatched := false
	i, err := storedRows.NewBucketIterator(ctx, row)
	if err != nil {
		return false, err
	}
	defer i.Close()
	for i.Rewind(); ; i.Next() {
		if ok, err := i.Valid(); err != nil {
			return false, err
		} else if !ok {
			break
		}
		otherRow, err := i.Row()
		if err != nil {
			return false, err
		}

		var renderedRow sqlbase.EncDatumRow
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
			probeMatched = true
			// Mark the row on the stored side. The unmarked rows can then be
			// iterated over for {right, left} outer joins (depending on
			// storedSide) and full outer joins.
			if err := i.Mark(ctx, true); err != nil {
				return false, nil
			}
			if h.testingKnobMemFailPoint == probe {
				return false, pgerror.NewErrorf(
					pgerror.CodeOutOfMemoryError,
					"%s test induced error",
					h.testingKnobMemFailPoint,
				)
			}
			consumerStatus, err := h.out.emitRow(ctx, renderedRow)
			if err != nil || consumerStatus != NeedMoreRows {
				return true, nil
			}
		}
	}

	if !probeMatched && !h.maybeEmitUnmatchedRow(ctx, row, otherSide(h.storedSide)) {
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
//
// If an error occurs while probing a row, the row is returned in order to not
// lose it.
func (h *hashJoiner) probePhase(
	ctx context.Context, storedRows hashRowContainer,
) (_ sqlbase.EncDatumRow, earlyExit bool, _ error) {
	side := otherSide(h.storedSide)

	src := h.leftSource
	if side == rightSide {
		src = h.rightSource
	}
	bufferedRows := &h.rows[side]
	// First process the rows that were already buffered.
	probeIterator := bufferedRows.NewIterator(ctx)
	defer probeIterator.Close()
	for probeIterator.Rewind(); ; probeIterator.Next() {
		if ok, err := probeIterator.Valid(); err != nil {
			return nil, false, err
		} else if !ok {
			break
		}
		row, err := probeIterator.Row()
		if err != nil {
			return nil, false, err
		}
		earlyExit, err := h.probeRow(ctx, row, storedRows)
		if earlyExit || err != nil {
			if pgErr, ok := err.(*pgerror.Error); ok && pgErr.Code == pgerror.CodeOutOfMemoryError {
				// A memory error in the probe phase only occurs after the
				// probe has happened (i.e. when setting the matching row's
				// marks). Remove the row from the memRowContainer to not
				// process this row again if the hashJoiner falls back to disk
				// and re-runs the probe phase.
				bufferedRows.PopFirst()
			}
			return row, earlyExit, err
		}
	}

	for {
		row, earlyExit, err := h.receiveRow(ctx, src, side)
		if row == nil {
			if earlyExit || err != nil {
				return nil, earlyExit, err
			}
			break
		}
		if earlyExit, err := h.probeRow(ctx, row, storedRows); earlyExit || err != nil {
			return row, earlyExit, err
		}
	}

	if shouldEmitUnmatchedRow(h.storedSide, h.joinType) {
		// Produce results for unmatched rows, for FULL OUTER AND LEFT/RIGHT OUTER
		// (depending on which stream we use).
		i := storedRows.NewUnmarkedIterator(ctx)
		defer i.Close()
		for i.Rewind(); ; i.Next() {
			if ok, err := i.Valid(); err != nil {
				return nil, false, err
			} else if !ok {
				break
			}
			row, err := i.Row()
			if err != nil {
				return nil, false, err
			}
			if !h.maybeEmitUnmatchedRow(ctx, row, h.storedSide) {
				return nil, true, nil
			}

		}
	}

	sendTraceData(ctx, h.out.output)
	h.out.close()
	return nil, false, nil
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
