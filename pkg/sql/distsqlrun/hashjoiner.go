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

package distsqlrun

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// hashJoinerInitialBufferSize controls the size of the initial buffering phase
// (see hashJoiner). This only applies when falling back to disk is disabled.
const hashJoinerInitialBufferSize = 4 * 1024 * 1024

// hashJoinerState represents the state of the processor.
type hashJoinerState int

const (
	hjStateUnknown hashJoinerState = iota
	// hjBuilding represents the state the hashJoiner is in when it is trying to
	// determine which side to store (i.e. which side is smallest).
	// At most hashJoinerInitialBufferSize is used to buffer rows from either
	// side. The first input to be finished within this limit is the smallest
	// side. If both inputs still have rows, the hashJoiner will default to
	// storing the right side. When a side is stored, a hash map is also
	// constructed from the equality columns to the rows.
	hjBuilding
	// hjConsumingStoredSide represents the state the hashJoiner is in if a small
	// side was not found. In this case, the hashJoiner will fully consume the
	// right side. This state is skipped if the hashJoiner determined the smallest
	// side, since it must have fully consumed that side.
	hjConsumingStoredSide
	// hjReadingProbeSide represents the state the hashJoiner is in when it reads
	// rows from the input that wasn't chosen to be stored.
	hjReadingProbeSide
	// hjProbingRow represents the state the hashJoiner is in when it uses a row
	// read in hjReadingProbeSide to probe the stored hash map with.
	hjProbingRow
	// hjEmittingUnmatched represents the state the hashJoiner is in when it is
	// emitting unmatched rows from its stored side after having consumed the
	// other side. This only happens when executing a FULL OUTER, LEFT/RIGHT
	// OUTER and ANTI joins (depending on which side we store).
	hjEmittingUnmatched
)

// hashJoiner performs a hash join. There is no guarantee on the output
// ordering.
type hashJoiner struct {
	joinerBase

	runningState hashJoinerState

	diskMonitor *mon.BytesMonitor

	leftSource, rightSource RowSource

	// initialBufferSize is the maximum amount of data we buffer from each stream
	// as part of the initial buffering phase. Normally
	// hashJoinerInitialBufferSize, can be tweaked for tests.
	initialBufferSize int64

	// We read a portion of both streams, in the hope that one is small. One of
	// the containers will contain the entire "stored" stream, the other just the
	// start of the other stream.
	rows [2]memRowContainer

	// storedSide is set by the initial buffering phase and indicates which
	// stream we store fully and build the hashRowContainer from.
	storedSide joinSide

	// nullEquality indicates that NULL = NULL should be considered true. Used for
	// INTERSECT and EXCEPT.
	nullEquality bool

	useTempStorage bool
	storedRows     hashRowContainer

	// Used by tests to force a storedSide.
	forcedStoredSide *joinSide

	// probingRowState is state used when hjProbingRow.
	probingRowState struct {
		// row is the row being probed with.
		row sqlbase.EncDatumRow
		// iter is an iterator over the bucket that matches row on the equality
		// columns.
		iter rowMarkerIterator
		// matched represents whether any row that matches row on equality columns
		// has also passed the ON condition.
		matched bool
	}

	// emittingUnmatchedState is used when hjEmittingUnmatched.
	emittingUnmatchedState struct {
		iter rowIterator
	}

	// testingKnobMemFailPoint specifies a state in which the hashJoiner will
	// fail at a random point during this phase.
	testingKnobMemFailPoint hashJoinerState
	// testingKnobFailProbability is a value in the range [0, 1] that specifies
	// a probability of failure at each possible failure point in a phase
	// specified by testingKnobMemFailPoint. Note that it becomes less likely
	// to hit a specific failure point as execution in the phase continues.
	testingKnobFailProbability float64

	// Context cancellation checker.
	cancelChecker *sqlbase.CancelChecker
}

var _ Processor = &hashJoiner{}
var _ RowSource = &hashJoiner{}

const hashJoinerProcName = "hash joiner"

func newHashJoiner(
	flowCtx *FlowCtx,
	processorID int32,
	spec *HashJoinerSpec,
	leftSource RowSource,
	rightSource RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (*hashJoiner, error) {
	h := &hashJoiner{
		initialBufferSize: hashJoinerInitialBufferSize,
		leftSource:        leftSource,
		rightSource:       rightSource,
	}

	numMergedColumns := 0
	if spec.MergedColumns {
		numMergedColumns = len(spec.LeftEqColumns)
	}
	if err := h.joinerBase.init(
		h,
		flowCtx,
		processorID,
		leftSource.OutputTypes(),
		rightSource.OutputTypes(),
		spec.Type,
		spec.OnExpr,
		spec.LeftEqColumns,
		spec.RightEqColumns,
		uint32(numMergedColumns),
		post,
		output,
		ProcStateOpts{
			InputsToDrain: []RowSource{h.leftSource, h.rightSource},
			TrailingMetaCallback: func(context.Context) []ProducerMetadata {
				h.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	st := h.flowCtx.Settings
	ctx := h.flowCtx.EvalCtx.Ctx()
	h.useTempStorage = settingUseTempStorageJoins.Get(&st.SV) ||
		h.flowCtx.testingKnobs.MemoryLimitBytes > 0 ||
		h.testingKnobMemFailPoint != hjStateUnknown
	if h.useTempStorage {
		// Limit the memory use by creating a child monitor with a hard limit.
		// The hashJoiner will overflow to disk if this limit is not enough.
		limit := h.flowCtx.testingKnobs.MemoryLimitBytes
		if limit <= 0 {
			limit = settingWorkMemBytes.Get(&st.SV)
		}
		limitedMon := mon.MakeMonitorInheritWithLimit("hashjoiner-limited", limit, flowCtx.EvalCtx.Mon)
		limitedMon.Start(ctx, flowCtx.EvalCtx.Mon, mon.BoundAccount{})
		h.MemMonitor = &limitedMon
		h.diskMonitor = NewMonitor(ctx, flowCtx.diskMonitor, "hashjoiner-disk")
		// Override initialBufferSize to be half of this processor's memory
		// limit. We consume up to h.initialBufferSize bytes from each input
		// stream.
		h.initialBufferSize = limit / 2
	} else {
		h.MemMonitor = NewMonitor(ctx, flowCtx.EvalCtx.Mon, "hashjoiner-mem")
	}

	// If the trace is recording, instrument the hashJoiner to collect stats.
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		h.leftSource = NewInputStatCollector(h.leftSource)
		h.rightSource = NewInputStatCollector(h.rightSource)
		h.finishTrace = h.outputStatsToTrace
	}

	h.rows[leftSide].initWithMon(
		nil /* ordering */, h.leftSource.OutputTypes(), h.evalCtx, h.MemMonitor,
	)
	h.rows[rightSide].initWithMon(
		nil /* ordering */, h.rightSource.OutputTypes(), h.evalCtx, h.MemMonitor,
	)

	if h.joinType == sqlbase.IntersectAllJoin || h.joinType == sqlbase.ExceptAllJoin {
		h.nullEquality = true
	}

	return h, nil
}

// Start is part of the RowSource interface.
func (h *hashJoiner) Start(ctx context.Context) context.Context {
	h.leftSource.Start(ctx)
	h.rightSource.Start(ctx)
	ctx = h.StartInternal(ctx, hashJoinerProcName)
	h.cancelChecker = sqlbase.NewCancelChecker(ctx)
	h.runningState = hjBuilding
	return ctx
}

// Next is part of the RowSource interface.
func (h *hashJoiner) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for h.State == StateRunning {
		var row sqlbase.EncDatumRow
		var meta *ProducerMetadata
		switch h.runningState {
		case hjBuilding:
			h.runningState, row, meta = h.build()
		case hjConsumingStoredSide:
			h.runningState, row, meta = h.consumeStoredSide()
		case hjReadingProbeSide:
			h.runningState, row, meta = h.readProbeSide()
		case hjProbingRow:
			h.runningState, row, meta = h.probeRow()
		case hjEmittingUnmatched:
			h.runningState, row, meta = h.emitUnmatched()
		default:
			log.Fatalf(h.Ctx, "unsupported state: %d", h.runningState)
		}

		if row == nil && meta == nil {
			continue
		}
		if meta != nil {
			return nil, meta
		}
		if outRow := h.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, h.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (h *hashJoiner) ConsumerClosed() {
	h.close()
}

func (h *hashJoiner) build() (hashJoinerState, sqlbase.EncDatumRow, *ProducerMetadata) {
	// setStoredSideTransition is a helper function that sets storedSide on the
	// hashJoiner and performs initialization before a transition to
	// hjConsumingStoredSide.
	setStoredSideTransition := func(
		side joinSide,
	) (hashJoinerState, sqlbase.EncDatumRow, *ProducerMetadata) {
		h.storedSide = side
		if err := h.initStoredRows(); err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}
		return hjConsumingStoredSide, nil, nil
	}

	if h.forcedStoredSide != nil {
		return setStoredSideTransition(*h.forcedStoredSide)
	}

	for {
		leftUsage := h.rows[leftSide].MemUsage()
		rightUsage := h.rows[rightSide].MemUsage()

		if leftUsage >= h.initialBufferSize && rightUsage >= h.initialBufferSize {
			// Both sides have reached the buffer size limit. Move on to storing and
			// fully consuming the right side.
			log.VEventf(h.Ctx, 1, "buffer phase found no short stream with buffer size %d", h.initialBufferSize)
			return setStoredSideTransition(rightSide)
		}

		side := rightSide
		if leftUsage < rightUsage {
			side = leftSide
		}

		row, meta, emitDirectly, err := h.receiveNext(side)
		if err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		} else if meta != nil {
			if meta.Err != nil {
				h.MoveToDraining(nil /* err */)
				return hjStateUnknown, nil, meta
			}
			return hjBuilding, nil, meta
		} else if emitDirectly {
			return hjBuilding, row, nil
		}

		if row == nil {
			// This side has been fully consumed, it is the shortest side.
			// If storedSide is empty, we might be able to short-circuit.
			if h.rows[side].Len() == 0 &&
				(h.joinType == sqlbase.InnerJoin ||
					(h.joinType == sqlbase.LeftOuterJoin && side == leftSide) ||
					(h.joinType == sqlbase.RightOuterJoin && side == rightSide)) {
				h.MoveToDraining(nil /* err */)
				return hjStateUnknown, nil, h.DrainHelper()
			}
			// We could skip hjConsumingStoredSide and move straight to
			// hjReadingProbeSide apart from the fact that hjConsumingStoredSide
			// pre-reserves mark memory. To keep the code simple and avoid
			// duplication, we move to hjConsumingStoredSide.
			return setStoredSideTransition(side)
		}

		// Add the row to the correct container.
		if err := h.rows[side].AddRow(h.Ctx, row); err != nil {
			// If this error is a memory limit error, move to hjConsumingStoredSide.
			h.storedSide = side
			if spilled, spillErr := h.maybeSpillToDisk(err); spilled {
				addErr := h.storedRows.AddRow(h.Ctx, row)
				if addErr == nil {
					return hjConsumingStoredSide, nil, nil
				}
				err = errors.Wrap(err, addErr.Error())
			} else if spillErr != nil {
				err = errors.Wrap(err, spillErr.Error())
			}
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}
	}
}

// consumeStoredSide fully consumes the stored side and adds the rows to
// h.storedRows. It assumes that h.storedRows has been initialized through
// h.initStoredRows().
func (h *hashJoiner) consumeStoredSide() (hashJoinerState, sqlbase.EncDatumRow, *ProducerMetadata) {
	side := h.storedSide
	for {
		row, meta, emitDirectly, err := h.receiveNext(side)
		if err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		} else if meta != nil {
			if meta.Err != nil {
				h.MoveToDraining(nil /* err */)
				return hjStateUnknown, nil, meta
			}
			return hjConsumingStoredSide, nil, meta
		} else if emitDirectly {
			return hjConsumingStoredSide, row, nil
		}

		if row == nil {
			// The stored side has been fully consumed, move on to hjReadingProbeSide.
			// If storedRows is in-memory, pre-reserve the memory needed to mark.
			if rc, ok := h.storedRows.(*hashMemRowContainer); ok {
				err = h.maybeMakeMemErr("reserving mark memory")
				if err == nil {
					err = rc.reserveMarkMemoryMaybe(h.Ctx)
				}
				if err != nil {
					if spilled, spillErr := h.maybeSpillToDisk(err); spilled {
						return hjReadingProbeSide, nil, nil
					} else if spillErr != nil {
						err = errors.Wrap(err, spillErr.Error())
					}
					h.MoveToDraining(err)
					return hjStateUnknown, nil, h.DrainHelper()
				}
			}
			return hjReadingProbeSide, nil, nil
		}

		err = h.maybeMakeMemErr("consuming stored side")
		if err == nil {
			err = h.storedRows.AddRow(h.Ctx, row)
		}
		if err != nil {
			if spilled, spillErr := h.maybeSpillToDisk(err); spilled {
				if err := h.storedRows.AddRow(h.Ctx, row); err != nil {
					h.MoveToDraining(err)
					return hjStateUnknown, nil, h.DrainHelper()
				}
				continue
			} else if spillErr != nil {
				err = errors.Wrap(err, spillErr.Error())
			}
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}

	}
}

func (h *hashJoiner) readProbeSide() (hashJoinerState, sqlbase.EncDatumRow, *ProducerMetadata) {
	side := otherSide(h.storedSide)

	var row sqlbase.EncDatumRow
	// First process the rows that were already buffered.
	if h.rows[side].Len() > 0 {
		row = h.rows[side].EncRow(0)
		h.rows[side].PopFirst()
	} else {
		var meta *ProducerMetadata
		var emitDirectly bool
		var err error
		row, meta, emitDirectly, err = h.receiveNext(side)
		if err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		} else if meta != nil {
			if meta.Err != nil {
				h.MoveToDraining(nil /* err */)
				return hjStateUnknown, nil, meta
			}
			return hjReadingProbeSide, nil, meta
		} else if emitDirectly {
			return hjReadingProbeSide, row, nil
		}

		if row == nil {
			// The probe side has been fully consumed. Move on to hjEmittingUnmatched
			// if unmatched rows on the stored side need to be emitted, otherwise
			// finish.
			if shouldEmitUnmatchedRow(h.storedSide, h.joinType) {
				i := h.storedRows.NewUnmarkedIterator(h.Ctx)
				i.Rewind()
				h.emittingUnmatchedState.iter = i
				return hjEmittingUnmatched, nil, nil
			}
			h.MoveToDraining(nil /* err */)
			return hjStateUnknown, nil, h.DrainHelper()
		}
	}

	// Probe with this row. Get the iterator over the matching bucket ready for
	// hjProbingRow.
	h.probingRowState.row = row
	h.probingRowState.matched = false
	if h.probingRowState.iter == nil {
		i, err := h.storedRows.NewBucketIterator(h.Ctx, row, h.eqCols[side])
		if err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}
		h.probingRowState.iter = i
	} else {
		if err := h.probingRowState.iter.Reset(h.Ctx, row); err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}
	}
	h.probingRowState.iter.Rewind()
	return hjProbingRow, nil, nil
}

func (h *hashJoiner) probeRow() (hashJoinerState, sqlbase.EncDatumRow, *ProducerMetadata) {
	i := h.probingRowState.iter
	if ok, err := i.Valid(); err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	} else if !ok {
		// In this case we have reached the end of the matching bucket. Check if any
		// rows passed the ON condition. If they did, move back to
		// hjReadingProbeSide to get the next probe row.
		if h.probingRowState.matched {
			return hjReadingProbeSide, nil, nil
		}
		// If not, this probe row is unmatched. Check if it needs to be emitted.
		if renderedRow, shouldEmit := h.shouldEmitUnmatched(
			h.probingRowState.row, otherSide(h.storedSide),
		); shouldEmit {
			return hjReadingProbeSide, renderedRow, nil
		}
		return hjReadingProbeSide, nil, nil
	}

	if err := h.cancelChecker.Check(); err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	}

	row := h.probingRowState.row
	otherRow, err := i.Row()
	if err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	}
	defer i.Next()

	var renderedRow sqlbase.EncDatumRow
	if h.storedSide == rightSide {
		renderedRow, err = h.render(row, otherRow)
	} else {
		renderedRow, err = h.render(otherRow, row)
	}
	if err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	}

	// If the ON condition failed, renderedRow is nil.
	if renderedRow == nil {
		return hjProbingRow, nil, nil
	}

	h.probingRowState.matched = true
	shouldEmit := h.joinType != sqlbase.LeftAntiJoin && h.joinType != sqlbase.ExceptAllJoin
	if shouldMark(h.storedSide, h.joinType) {
		// Matched rows are marked on the stored side for 2 reasons.
		// 1: For outer joins, anti joins, and EXCEPT ALL to iterate through
		// the unmarked rows.
		// 2: For semi-joins and INTERSECT ALL where the left-side is stored,
		// multiple rows from the right may match to the same row on the left.
		// The rows on the left should only be emitted the first time
		// a right row matches it, then marked to not be emitted again.
		// (Note: an alternative is to remove the entry from the stored
		// side, but our containers do not support that today).
		// TODO(peter): figure out a way to reduce this special casing below.
		if i.IsMarked(h.Ctx) {
			switch h.joinType {
			case sqlbase.LeftSemiJoin:
				shouldEmit = false
			case sqlbase.IntersectAllJoin:
				shouldEmit = false
			case sqlbase.ExceptAllJoin:
				// We want to mark a stored row if possible, so move on to the next
				// match. Reset h.probingRowState.matched in case we don't find any more
				// matches and want to emit this row.
				h.probingRowState.matched = false
				return hjProbingRow, nil, nil
			}
		} else if err := i.Mark(h.Ctx, true); err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}
	}
	nextState := hjProbingRow
	if shouldShortCircuit(h.storedSide, h.joinType) {
		nextState = hjReadingProbeSide
	}
	if shouldEmit {
		if h.joinType == sqlbase.IntersectAllJoin {
			// We found a match, so we are done with this row.
			return hjReadingProbeSide, renderedRow, nil
		}
		return nextState, renderedRow, nil
	}

	return nextState, nil, nil
}

func (h *hashJoiner) emitUnmatched() (hashJoinerState, sqlbase.EncDatumRow, *ProducerMetadata) {
	i := h.emittingUnmatchedState.iter
	if ok, err := i.Valid(); err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	} else if !ok {
		// Done.
		h.MoveToDraining(nil /* err */)
		return hjStateUnknown, nil, h.DrainHelper()
	}

	if err := h.cancelChecker.Check(); err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	}

	row, err := i.Row()
	if err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	}
	defer i.Next()

	return hjEmittingUnmatched, h.renderUnmatchedRow(row, h.storedSide), nil
}

func (h *hashJoiner) close() {
	if h.InternalClose() {
		h.rows[leftSide].Close(h.Ctx)
		h.rows[rightSide].Close(h.Ctx)
		if h.storedRows != nil {
			h.storedRows.Close(h.Ctx)
		}
		if h.probingRowState.iter != nil {
			h.probingRowState.iter.Close()
		}
		if h.emittingUnmatchedState.iter != nil {
			h.emittingUnmatchedState.iter.Close()
		}
		h.MemMonitor.Stop(h.Ctx)
		if h.diskMonitor != nil {
			h.diskMonitor.Stop(h.Ctx)
		}
	}
}

// receiveNext reads from the source specified by side and returns the next row
// or metadata to be processed by the hashJoiner. Unless h.nullEquality is true,
// rows with NULLs in their equality columns are only returned if the joinType
// specifies that unmatched rows should be returned for the given side. In this
// case, a rendered row and true is returned, notifying the caller that the
// returned row may be emitted directly.
func (h *hashJoiner) receiveNext(
	side joinSide,
) (sqlbase.EncDatumRow, *ProducerMetadata, bool, error) {
	source := h.leftSource
	if side == rightSide {
		source = h.rightSource
	}
	for {
		if err := h.cancelChecker.Check(); err != nil {
			return nil, nil, false, err
		}
		row, meta := source.Next()
		if meta != nil {
			return nil, meta, false, nil
		} else if row == nil {
			return nil, nil, false, nil
		}
		hasNull := false
		for _, c := range h.eqCols[side] {
			if row[c].IsNull() {
				hasNull = true
				break
			}
		}
		// row has no NULLs in its equality columns (or we are considering NULLs to
		// be equal), so it might match a row from the other side.
		if !hasNull || h.nullEquality {
			return row, nil, false, nil
		}

		if renderedRow, shouldEmit := h.shouldEmitUnmatched(row, side); shouldEmit {
			return renderedRow, nil, true, nil
		}

		// If this point is reached, row had NULLs in its equality columns but
		// should not be emitted. Throw it away and get the next row.
	}
}

// shouldEmitUnmatched returns whether this row should be emitted if it doesn't
// match. If this is the case, a rendered row ready for emitting is returned as
// well.
func (h *hashJoiner) shouldEmitUnmatched(
	row sqlbase.EncDatumRow, side joinSide,
) (sqlbase.EncDatumRow, bool) {
	if !shouldEmitUnmatchedRow(side, h.joinType) {
		return nil, false
	}
	return h.renderUnmatchedRow(row, side), true
}

// initStoredRows initializes a hashRowContainer and sets h.storedRows.
func (h *hashJoiner) initStoredRows() error {
	storedMemRows := makeHashMemRowContainer(&h.rows[h.storedSide])
	err := storedMemRows.Init(
		h.Ctx,
		shouldMark(h.storedSide, h.joinType),
		h.rows[h.storedSide].types,
		h.eqCols[h.storedSide],
		h.nullEquality,
	)
	if err == nil {
		err = h.maybeMakeMemErr("initializing mem rows")
		// Close the container on an artificial error.
		if err != nil {
			storedMemRows.Close(h.Ctx)
		}
	}
	if err != nil {
		if spilled, spillErr := h.maybeSpillToDisk(err); spilled {
			return nil
		} else if spillErr != nil {
			return errors.Wrap(err, spillErr.Error())
		}
		return err
	}

	h.storedRows = &storedMemRows
	return nil
}

// maybeSpillToDisk checks err and spills h.rows[h.storedSide] to disk if the
// error is a memory error and h.storedRows is not a hashDiskRowContainer. On
// a successful disk spill, maybeSpillToDisk Close()s whatever h.storedRows
// previously pointed to and sets h.storedRows a hashDiskRowContainer.
// Returns whether the hashJoiner spilled to disk and an error if one occurred
// while doing so.
// TODO(asubiotto): This should be behind an auto-fallback hashDiskRowContainer.
func (h *hashJoiner) maybeSpillToDisk(err error) (bool, error) {
	newDiskSpillErr := func(errString string) error {
		return fmt.Errorf("error while attempting hashJoiner disk spill: %s", errString)
	}
	if !h.useTempStorage {
		return false, newDiskSpillErr("temp storage disabled")
	}
	if pgErr, ok := pgerror.GetPGCause(err); !(ok && pgErr.Code == pgerror.CodeOutOfMemoryError) {
		return false, nil
	}
	if _, ok := h.storedRows.(*hashDiskRowContainer); ok {
		return false, newDiskSpillErr("already using disk")
	}

	storedDiskRows := makeHashDiskRowContainer(h.diskMonitor, h.flowCtx.TempStorage)
	if err := storedDiskRows.Init(
		h.Ctx,
		shouldMark(h.storedSide, h.joinType),
		h.rows[h.storedSide].types,
		h.eqCols[h.storedSide],
		h.nullEquality,
	); err != nil {
		return false, newDiskSpillErr(err.Error())
	}

	if h.storedRows != nil {
		h.storedRows.Close(h.Ctx)
	}
	h.storedRows = &storedDiskRows

	// Transfer rows from memory.
	i := h.rows[h.storedSide].NewFinalIterator(h.Ctx)
	defer i.Close()
	for i.Rewind(); ; i.Next() {
		if err := h.cancelChecker.Check(); err != nil {
			return false, newDiskSpillErr(err.Error())
		}
		if ok, err := i.Valid(); err != nil {
			return false, newDiskSpillErr(err.Error())
		} else if !ok {
			break
		}
		memRow, err := i.Row()
		if err != nil {
			return false, newDiskSpillErr(err.Error())
		}
		if err := storedDiskRows.AddRow(h.Ctx, memRow); err != nil {
			return false, newDiskSpillErr(err.Error())
		}
	}
	return true, nil
}

// maybeMakeMemErr is a utility function that returns a memory error with a
// probability of h.testingKnobMemFailProbability if h.runningState matches the
// h.testingKnobMemFailPoint.
func (h *hashJoiner) maybeMakeMemErr(action string) error {
	if h.testingKnobMemFailPoint == h.runningState &&
		rand.Float64() < h.testingKnobFailProbability {
		// Unset the testing knob fail point.
		h.testingKnobMemFailPoint = hjStateUnknown
		return pgerror.NewErrorf(
			pgerror.CodeOutOfMemoryError, fmt.Sprintf("%s test induced error", action),
		)
	}
	return nil
}

var _ DistSQLSpanStats = &HashJoinerStats{}

const hashJoinerTagPrefix = "hashjoiner."

// Stats implements the SpanStats interface.
func (hjs *HashJoinerStats) Stats() map[string]string {
	// statsMap starts off as the left input stats map.
	statsMap := hjs.LeftInputStats.Stats(hashJoinerTagPrefix + "left.")
	rightInputStatsMap := hjs.RightInputStats.Stats(hashJoinerTagPrefix + "right.")
	// Merge the two input maps.
	for k, v := range rightInputStatsMap {
		statsMap[k] = v
	}
	statsMap[hashJoinerTagPrefix+"stored_side"] = hjs.StoredSide
	statsMap[hashJoinerTagPrefix+maxMemoryTagSuffix] = humanizeutil.IBytes(hjs.MaxAllocatedMem)
	statsMap[hashJoinerTagPrefix+maxDiskTagSuffix] = humanizeutil.IBytes(hjs.MaxAllocatedDisk)
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (hjs *HashJoinerStats) StatsForQueryPlan() []string {
	leftInputStats := hjs.LeftInputStats.StatsForQueryPlan("left ")
	leftInputStats = append(leftInputStats, hjs.RightInputStats.StatsForQueryPlan("right ")...)
	return append(
		leftInputStats,
		fmt.Sprintf("stored side: %s", hjs.StoredSide),
		fmt.Sprintf("%s: %s", maxMemoryQueryPlanSuffix, humanizeutil.IBytes(hjs.MaxAllocatedMem)),
		fmt.Sprintf("%s: %s", maxDiskQueryPlanSuffix, humanizeutil.IBytes(hjs.MaxAllocatedDisk)),
	)
}

// outputStatsToTrace outputs the collected hashJoiner stats to the trace. Will
// fail silently if the hashJoiner is not collecting stats.
func (h *hashJoiner) outputStatsToTrace() {
	lis, ok := getInputStats(h.flowCtx, h.leftSource)
	if !ok {
		return
	}
	ris, ok := getInputStats(h.flowCtx, h.rightSource)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(h.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp,
			&HashJoinerStats{
				LeftInputStats:   lis,
				RightInputStats:  ris,
				StoredSide:       h.storedSide.String(),
				MaxAllocatedMem:  h.MemMonitor.MaximumBytes(),
				MaxAllocatedDisk: h.diskMonitor.MaximumBytes(),
			},
		)
	}
}

// Some types of joins need to mark rows that matched.
func shouldMark(storedSide joinSide, joinType sqlbase.JoinType) bool {
	switch {
	case joinType == sqlbase.LeftSemiJoin && storedSide == leftSide:
		return true
	case joinType == sqlbase.LeftAntiJoin && storedSide == leftSide:
		return true
	case joinType == sqlbase.ExceptAllJoin:
		return true
	case joinType == sqlbase.IntersectAllJoin:
		return true
	case shouldEmitUnmatchedRow(storedSide, joinType):
		return true
	default:
		return false
	}
}

// Some types of joins only need to know of the existence of a matching row in
// the storedSide, depending on the storedSide, and don't need to know all the
// rows. These can 'short circuit' to avoid iterating through them all.
func shouldShortCircuit(storedSide joinSide, joinType sqlbase.JoinType) bool {
	switch joinType {
	case sqlbase.LeftSemiJoin:
		return storedSide == rightSide
	case sqlbase.ExceptAllJoin:
		return true
	default:
		return false
	}
}

// encodeColumnsOfRow returns the encoding for the grouping columns. This is
// then used as our group key to determine which bucket to add to.
// If the row contains any NULLs and encodeNull is false, hasNull is true and
// no encoding is returned. If encodeNull is true, hasNull is never set.
func encodeColumnsOfRow(
	da *sqlbase.DatumAlloc,
	appendTo []byte,
	row sqlbase.EncDatumRow,
	cols columns,
	colTypes []sqlbase.ColumnType,
	encodeNull bool,
) (encoding []byte, hasNull bool, err error) {
	for i, colIdx := range cols {
		if row[colIdx].IsNull() && !encodeNull {
			return nil, true, nil
		}
		// Note: we cannot compare VALUE encodings because they contain column IDs
		// which can vary.
		// TODO(radu): we should figure out what encoding is readily available and
		// use that (though it needs to be consistent across all rows). We could add
		// functionality to compare VALUE encodings ignoring the column ID.
		appendTo, err = row[colIdx].Encode(&colTypes[i], da, sqlbase.DatumEncoding_ASCENDING_KEY, appendTo)
		if err != nil {
			return appendTo, false, err
		}
	}
	return appendTo, false, nil
}
