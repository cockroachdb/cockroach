// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
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
	execinfra.JoinerBase

	runningState hashJoinerState

	diskMonitor *mon.BytesMonitor

	leftSource, rightSource execinfra.RowSource

	// initialBufferSize is the maximum amount of data we buffer from each stream
	// as part of the initial buffering phase. Normally
	// hashJoinerInitialBufferSize, can be tweaked for tests.
	initialBufferSize int64

	// We read a portion of both streams, in the hope that one is small. One of
	// the containers will contain the entire "stored" stream, the other just the
	// start of the other stream.
	rows [2]rowcontainer.MemRowContainer

	// storedSide is set by the initial buffering phase and indicates which
	// stream we store fully and build the hashRowContainer from.
	storedSide execinfra.JoinSide

	// nullEquality indicates that NULL = NULL should be considered true. Used for
	// INTERSECT and EXCEPT.
	nullEquality bool

	useTempStorage bool
	storedRows     rowcontainer.HashRowContainer

	// Used by tests to force a storedSide.
	forcedStoredSide *execinfra.JoinSide

	// probingRowState is state used when hjProbingRow.
	probingRowState struct {
		// row is the row being probed with.
		row sqlbase.EncDatumRow
		// iter is an iterator over the bucket that matches row on the equality
		// columns.
		iter rowcontainer.RowMarkerIterator
		// matched represents whether any row that matches row on equality columns
		// has also passed the ON condition.
		matched bool
	}

	// emittingUnmatchedState is used when hjEmittingUnmatched.
	emittingUnmatchedState struct {
		iter rowcontainer.RowIterator
	}

	// testingKnobMemFailPoint specifies a state in which the hashJoiner will
	// fail at a random point during this phase.
	testingKnobMemFailPoint hashJoinerState

	// Context cancellation checker.
	cancelChecker *sqlbase.CancelChecker
}

var _ execinfra.Processor = &hashJoiner{}
var _ execinfra.RowSource = &hashJoiner{}

const hashJoinerProcName = "hash joiner"

func newHashJoiner(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.HashJoinerSpec,
	leftSource execinfra.RowSource,
	rightSource execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
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
	if err := h.JoinerBase.Init(
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
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{h.leftSource, h.rightSource},
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
				h.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	st := h.FlowCtx.Cfg.Settings
	ctx := h.FlowCtx.EvalCtx.Ctx()
	h.useTempStorage = execinfra.SettingUseTempStorageJoins.Get(&st.SV) ||
		h.FlowCtx.Cfg.TestingKnobs.MemoryLimitBytes > 0 ||
		h.testingKnobMemFailPoint != hjStateUnknown
	if h.useTempStorage {
		// Limit the memory use by creating a child monitor with a hard limit.
		// The hashJoiner will overflow to disk if this limit is not enough.
		limit := h.FlowCtx.Cfg.TestingKnobs.MemoryLimitBytes
		if limit <= 0 {
			limit = execinfra.SettingWorkMemBytes.Get(&st.SV)
		}
		h.MemMonitor = execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg, "hashjoiner-limited")
		h.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, "hashjoiner-disk")
		// Override initialBufferSize to be half of this processor's memory
		// limit. We consume up to h.initialBufferSize bytes from each input
		// stream.
		h.initialBufferSize = limit / 2
	} else {
		h.MemMonitor = execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "hashjoiner-mem")
	}

	// If the trace is recording, instrument the hashJoiner to collect stats.
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		h.leftSource = execinfra.NewInputStatCollector(h.leftSource)
		h.rightSource = execinfra.NewInputStatCollector(h.rightSource)
		h.FinishTrace = h.outputStatsToTrace
	}

	h.rows[execinfra.LeftSide].InitWithMon(
		nil /* ordering */, h.leftSource.OutputTypes(), h.EvalCtx, h.MemMonitor, 0, /* rowCapacity */
	)
	h.rows[execinfra.RightSide].InitWithMon(
		nil /* ordering */, h.rightSource.OutputTypes(), h.EvalCtx, h.MemMonitor, 0, /* rowCapacity */
	)

	if h.JoinType == sqlbase.IntersectAllJoin || h.JoinType == sqlbase.ExceptAllJoin {
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
func (h *hashJoiner) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for h.State == execinfra.StateRunning {
		var row sqlbase.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
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

func (h *hashJoiner) build() (hashJoinerState, sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// setStoredSideTransition is a helper function that sets storedSide on the
	// hashJoiner and performs initialization before a transition to
	// hjConsumingStoredSide.
	setStoredSideTransition := func(
		side execinfra.JoinSide,
	) (hashJoinerState, sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
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
		leftUsage := h.rows[execinfra.LeftSide].MemUsage()
		rightUsage := h.rows[execinfra.RightSide].MemUsage()

		if leftUsage >= h.initialBufferSize && rightUsage >= h.initialBufferSize {
			// Both sides have reached the buffer size limit. Move on to storing and
			// fully consuming the right side.
			log.VEventf(h.Ctx, 1, "buffer phase found no short stream with buffer size %d", h.initialBufferSize)
			return setStoredSideTransition(execinfra.RightSide)
		}

		side := execinfra.RightSide
		if leftUsage < rightUsage {
			side = execinfra.LeftSide
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
				(h.JoinType == sqlbase.InnerJoin ||
					(h.JoinType == sqlbase.LeftOuterJoin && side == execinfra.LeftSide) ||
					(h.JoinType == sqlbase.RightOuterJoin && side == execinfra.RightSide)) {
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
			if sqlbase.IsOutOfMemoryError(err) {
				if !h.useTempStorage {
					err = pgerror.Wrapf(err, pgcode.OutOfMemory,
						"error while attempting hashJoiner disk spill: temp storage disabled")
				} else {
					if err := h.initStoredRows(); err != nil {
						h.MoveToDraining(err)
						return hjStateUnknown, nil, h.DrainHelper()
					}
					addErr := h.storedRows.AddRow(h.Ctx, row)
					if addErr == nil {
						return hjConsumingStoredSide, nil, nil
					}
					err = pgerror.Wrapf(addErr, pgcode.OutOfMemory, "while spilling: %v", err)
				}
			}
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}
	}
}

// consumeStoredSide fully consumes the stored side and adds the rows to
// h.storedRows. It assumes that h.storedRows has been initialized through
// h.initStoredRows().
func (h *hashJoiner) consumeStoredSide() (
	hashJoinerState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
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
			if rc, ok := h.storedRows.(*rowcontainer.HashMemRowContainer); ok {
				// h.storedRows is hashMemRowContainer and not a disk backed one, so
				// h.useTempStorage is false and we cannot spill to disk, so we simply
				// will return an error if it occurs.
				err = rc.ReserveMarkMemoryMaybe(h.Ctx)
			} else if hdbrc, ok := h.storedRows.(*rowcontainer.HashDiskBackedRowContainer); ok {
				err = hdbrc.ReserveMarkMemoryMaybe(h.Ctx)
			} else {
				panic("unexpected type of storedRows in hashJoiner")
			}
			if err != nil {
				h.MoveToDraining(err)
				return hjStateUnknown, nil, h.DrainHelper()
			}
			return hjReadingProbeSide, nil, nil
		}

		err = h.storedRows.AddRow(h.Ctx, row)
		// Regardless of the underlying row container (disk backed or in-memory
		// only), we cannot do anything about an error if it occurs.
		if err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}
	}
}

func (h *hashJoiner) readProbeSide() (
	hashJoinerState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	side := execinfra.OtherSide(h.storedSide)

	var row sqlbase.EncDatumRow
	// First process the rows that were already buffered.
	if h.rows[side].Len() > 0 {
		row = h.rows[side].EncRow(0)
		h.rows[side].PopFirst()
	} else {
		var meta *execinfrapb.ProducerMetadata
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
			if execinfra.ShouldEmitUnmatchedRow(h.storedSide, h.JoinType) {
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
		i, err := h.storedRows.NewBucketIterator(h.Ctx, row, h.EqCols[side])
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

func (h *hashJoiner) probeRow() (
	hashJoinerState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
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
			h.probingRowState.row, execinfra.OtherSide(h.storedSide),
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
	if h.storedSide == execinfra.RightSide {
		renderedRow, err = h.Render(row, otherRow)
	} else {
		renderedRow, err = h.Render(otherRow, row)
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
	shouldEmit := h.JoinType != sqlbase.LeftAntiJoin && h.JoinType != sqlbase.ExceptAllJoin
	if shouldMark(h.storedSide, h.JoinType) {
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
			switch h.JoinType {
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
	if shouldShortCircuit(h.storedSide, h.JoinType) {
		nextState = hjReadingProbeSide
	}
	if shouldEmit {
		if h.JoinType == sqlbase.IntersectAllJoin {
			// We found a match, so we are done with this row.
			return hjReadingProbeSide, renderedRow, nil
		}
		return nextState, renderedRow, nil
	}

	return nextState, nil, nil
}

func (h *hashJoiner) emitUnmatched() (
	hashJoinerState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
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

	return hjEmittingUnmatched, h.RenderUnmatchedRow(row, h.storedSide), nil
}

func (h *hashJoiner) close() {
	if h.InternalClose() {
		// We need to close only memRowContainer of the probe side because the
		// stored side container will be closed by closing h.storedRows.
		if h.storedSide == execinfra.RightSide {
			h.rows[execinfra.LeftSide].Close(h.Ctx)
		} else {
			h.rows[execinfra.RightSide].Close(h.Ctx)
		}
		if h.storedRows != nil {
			h.storedRows.Close(h.Ctx)
		} else {
			// h.storedRows has not been initialized, so we need to close the stored
			// side container explicitly.
			h.rows[h.storedSide].Close(h.Ctx)
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
	side execinfra.JoinSide,
) (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata, bool, error) {
	source := h.leftSource
	if side == execinfra.RightSide {
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
		// We make the explicit check for whether or not the row contained a NULL value
		// on an equality column. The reasoning here is because of the way we expect
		// NULL equality checks to behave (i.e. NULL != NULL) and the fact that we
		// use the encoding of any given row as key into our bucket. Thus if we
		// encountered a NULL row when building the hashmap we have to store in
		// order to use it for RIGHT OUTER joins but if we encounter another
		// NULL row when going through the left stream (probing phase), matching
		// this with the first NULL row would be incorrect.
		//
		// If we have have the following:
		// CREATE TABLE t(x INT); INSERT INTO t(x) VALUES (NULL);
		//    |  x   |
		//     ------
		//    | NULL |
		//
		// For the following query:
		// SELECT * FROM t AS a FULL OUTER JOIN t AS b USING(x);
		//
		// We expect:
		//    |  x   |
		//     ------
		//    | NULL |
		//    | NULL |
		//
		// The following examples illustrates the behavior when joining on two
		// or more columns, and only one of them contains NULL.
		// If we have have the following:
		// CREATE TABLE t(x INT, y INT);
		// INSERT INTO t(x, y) VALUES (44,51), (NULL,52);
		//    |  x   |  y   |
		//     ------
		//    |  44  |  51  |
		//    | NULL |  52  |
		//
		// For the following query:
		// SELECT * FROM t AS a FULL OUTER JOIN t AS b USING(x, y);
		//
		// We expect:
		//    |  x   |  y   |
		//     ------
		//    |  44  |  51  |
		//    | NULL |  52  |
		//    | NULL |  52  |
		hasNull := false
		for _, c := range h.EqCols[side] {
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
	row sqlbase.EncDatumRow, side execinfra.JoinSide,
) (sqlbase.EncDatumRow, bool) {
	if !execinfra.ShouldEmitUnmatchedRow(side, h.JoinType) {
		return nil, false
	}
	return h.RenderUnmatchedRow(row, side), true
}

// initStoredRows initializes a hashRowContainer and sets h.storedRows.
func (h *hashJoiner) initStoredRows() error {
	if h.useTempStorage {
		hrc := rowcontainer.NewHashDiskBackedRowContainer(
			&h.rows[h.storedSide],
			h.EvalCtx,
			h.MemMonitor,
			h.diskMonitor,
			h.FlowCtx.Cfg.TempStorage,
		)
		h.storedRows = hrc
	} else {
		hrc := rowcontainer.MakeHashMemRowContainer(&h.rows[h.storedSide])
		h.storedRows = &hrc
	}
	return h.storedRows.Init(
		h.Ctx,
		shouldMark(h.storedSide, h.JoinType),
		h.rows[h.storedSide].Types(),
		h.EqCols[h.storedSide],
		h.nullEquality,
	)
}

var _ execinfrapb.DistSQLSpanStats = &HashJoinerStats{}

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
	statsMap[hashJoinerTagPrefix+execinfra.MaxMemoryTagSuffix] = humanizeutil.IBytes(hjs.MaxAllocatedMem)
	statsMap[hashJoinerTagPrefix+execinfra.MaxDiskTagSuffix] = humanizeutil.IBytes(hjs.MaxAllocatedDisk)
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (hjs *HashJoinerStats) StatsForQueryPlan() []string {
	leftInputStats := hjs.LeftInputStats.StatsForQueryPlan("left ")
	leftInputStats = append(leftInputStats, hjs.RightInputStats.StatsForQueryPlan("right ")...)
	return append(
		leftInputStats,
		fmt.Sprintf("stored side: %s", hjs.StoredSide),
		fmt.Sprintf("%s: %s", execinfra.MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(hjs.MaxAllocatedMem)),
		fmt.Sprintf("%s: %s", execinfra.MaxDiskQueryPlanSuffix, humanizeutil.IBytes(hjs.MaxAllocatedDisk)),
	)
}

// outputStatsToTrace outputs the collected hashJoiner stats to the trace. Will
// fail silently if the hashJoiner is not collecting stats.
func (h *hashJoiner) outputStatsToTrace() {
	lis, ok := execinfra.GetInputStats(h.FlowCtx, h.leftSource)
	if !ok {
		return
	}
	ris, ok := execinfra.GetInputStats(h.FlowCtx, h.rightSource)
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
func shouldMark(storedSide execinfra.JoinSide, joinType sqlbase.JoinType) bool {
	switch {
	case joinType == sqlbase.LeftSemiJoin && storedSide == execinfra.LeftSide:
		return true
	case joinType == sqlbase.LeftAntiJoin && storedSide == execinfra.LeftSide:
		return true
	case joinType == sqlbase.ExceptAllJoin:
		return true
	case joinType == sqlbase.IntersectAllJoin:
		return true
	case execinfra.ShouldEmitUnmatchedRow(storedSide, joinType):
		return true
	default:
		return false
	}
}

// Some types of joins only need to know of the existence of a matching row in
// the storedSide, depending on the storedSide, and don't need to know all the
// rows. These can 'short circuit' to avoid iterating through them all.
func shouldShortCircuit(storedSide execinfra.JoinSide, joinType sqlbase.JoinType) bool {
	switch joinType {
	case sqlbase.LeftSemiJoin:
		return storedSide == execinfra.RightSide
	case sqlbase.ExceptAllJoin:
		return true
	default:
		return false
	}
}
