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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/errors"
)

// hashJoinerState represents the state of the hash join processor.
type hashJoinerState int

const (
	hjStateUnknown hashJoinerState = iota
	// hjBuilding represents the state the hashJoiner is in when it is building
	// the hash table based on the equality columns of the rows from the right
	// side.
	hjBuilding
	// hjReadingLeftSide represents the state the hashJoiner is in when it
	// reads rows from the left side.
	hjReadingLeftSide
	// hjProbingRow represents the state the hashJoiner is in when it uses a
	// row read in hjReadingLeftSide to probe the stored hash map with.
	hjProbingRow
	// hjEmittingRightUnmatched represents the state the hashJoiner is in when
	// it is emitting unmatched rows from the right side after having consumed
	// the left side. This only happens when executing a FULL OUTER, RIGHT
	// OUTER, and RIGHT ANTI joins.
	hjEmittingRightUnmatched
)

// hashJoiner performs a hash join. There is no guarantee on the output
// ordering.
type hashJoiner struct {
	joinerBase

	runningState hashJoinerState

	diskMonitor *mon.BytesMonitor

	leftSource, rightSource execinfra.RowSource

	// nullEquality indicates that NULL = NULL should be considered true. Used for
	// INTERSECT and EXCEPT.
	nullEquality bool

	hashTable *rowcontainer.HashDiskBackedRowContainer

	// probingRowState is state used when hjProbingRow.
	probingRowState struct {
		// row is the row being probed with.
		row rowenc.EncDatumRow
		// iter is an iterator over the bucket that matches row on the equality
		// columns.
		iter rowcontainer.RowMarkerIterator
		// matched represents whether any row that matches row on equality columns
		// has also passed the ON condition.
		matched bool
	}

	// emittingRightUnmatchedState is used when hjEmittingRightUnmatched.
	emittingRightUnmatchedState struct {
		iter rowcontainer.RowIterator
	}

	// Context cancellation checker.
	cancelChecker *cancelchecker.CancelChecker
}

var _ execinfra.Processor = &hashJoiner{}
var _ execinfra.RowSource = &hashJoiner{}
var _ execinfra.OpNode = &hashJoiner{}

const hashJoinerProcName = "hash joiner"

// newHashJoiner creates a new hash join processor.
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
		leftSource:   leftSource,
		rightSource:  rightSource,
		nullEquality: spec.Type.IsSetOpJoin(),
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
		false, /* outputContinuationColumn */
		post,
		output,
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{h.leftSource, h.rightSource},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				h.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	ctx := h.FlowCtx.EvalCtx.Ctx()
	// Limit the memory use by creating a child monitor with a hard limit.
	// The hashJoiner will overflow to disk if this limit is not enough.
	h.MemMonitor = execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx, "hashjoiner-limited")
	h.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.DiskMonitor, "hashjoiner-disk")
	h.hashTable = rowcontainer.NewHashDiskBackedRowContainer(
		h.EvalCtx, h.MemMonitor, h.diskMonitor, h.FlowCtx.Cfg.TempStorage,
	)

	// If the trace is recording, instrument the hashJoiner to collect stats.
	if execinfra.ShouldCollectStats(ctx, flowCtx) {
		h.leftSource = newInputStatCollector(h.leftSource)
		h.rightSource = newInputStatCollector(h.rightSource)
		h.ExecStatsForTrace = h.execStatsForTrace
	}

	return h, h.hashTable.Init(
		h.Ctx,
		shouldMarkRightSide(h.joinType),
		h.rightSource.OutputTypes(),
		h.eqCols[rightSide],
		h.nullEquality,
	)
}

// Start is part of the RowSource interface.
func (h *hashJoiner) Start(ctx context.Context) {
	ctx = h.StartInternal(ctx, hashJoinerProcName)
	h.leftSource.Start(ctx)
	h.rightSource.Start(ctx)
	h.cancelChecker = cancelchecker.NewCancelChecker(ctx)
	h.runningState = hjBuilding
}

// Next is part of the RowSource interface.
func (h *hashJoiner) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for h.State == execinfra.StateRunning {
		var row rowenc.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch h.runningState {
		case hjBuilding:
			h.runningState, row, meta = h.build()
		case hjReadingLeftSide:
			h.runningState, row, meta = h.readLeftSide()
		case hjProbingRow:
			h.runningState, row, meta = h.probeRow()
		case hjEmittingRightUnmatched:
			h.runningState, row, meta = h.emitRightUnmatched()
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

func (h *hashJoiner) build() (hashJoinerState, rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for {
		row, meta, emitDirectly, err := h.receiveNext(rightSide)
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
			// Right side has been fully consumed, so move on to
			// hjReadingLeftSide. If the hash table is empty, we might be able
			// to short-circuit.
			if h.hashTable.IsEmpty() && h.joinType.IsEmptyOutputWhenRightIsEmpty() {
				h.MoveToDraining(nil /* err */)
				return hjStateUnknown, nil, h.DrainHelper()
			}
			// If hashTable is in-memory, pre-reserve the memory needed to mark.
			if err = h.hashTable.ReserveMarkMemoryMaybe(h.Ctx); err != nil {
				h.MoveToDraining(err)
				return hjStateUnknown, nil, h.DrainHelper()
			}
			return hjReadingLeftSide, nil, nil
		}

		err = h.hashTable.AddRow(h.Ctx, row)
		// Regardless of the underlying row container (disk backed or in-memory
		// only), we cannot do anything about an error if it occurs.
		if err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}
	}
}

func (h *hashJoiner) readLeftSide() (
	hashJoinerState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	row, meta, emitDirectly, err := h.receiveNext(leftSide)
	if err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	} else if meta != nil {
		if meta.Err != nil {
			h.MoveToDraining(nil /* err */)
			return hjStateUnknown, nil, meta
		}
		return hjReadingLeftSide, nil, meta
	} else if emitDirectly {
		return hjReadingLeftSide, row, nil
	}

	if row == nil {
		// The left side has been fully consumed. Move on to
		// hjEmittingRightUnmatched if unmatched rows on the right side need to
		// be emitted, otherwise finish.
		if shouldEmitUnmatchedRow(rightSide, h.joinType) {
			i := h.hashTable.NewUnmarkedIterator(h.Ctx)
			i.Rewind()
			h.emittingRightUnmatchedState.iter = i
			return hjEmittingRightUnmatched, nil, nil
		}
		h.MoveToDraining(nil /* err */)
		return hjStateUnknown, nil, h.DrainHelper()
	}

	// Probe with this row. Get the iterator over the matching bucket ready for
	// hjProbingRow.
	h.probingRowState.row = row
	h.probingRowState.matched = false
	if h.probingRowState.iter == nil {
		i, err := h.hashTable.NewBucketIterator(h.Ctx, row, h.eqCols[leftSide])
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
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	i := h.probingRowState.iter
	if ok, err := i.Valid(); err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	} else if !ok {
		// In this case we have reached the end of the matching bucket. Check
		// if any rows passed the ON condition. If they did, move back to
		// hjReadingLeftSide to get the next probe row.
		if h.probingRowState.matched {
			return hjReadingLeftSide, nil, nil
		}
		// If not, this probe row is unmatched. Check if it needs to be emitted.
		if renderedRow, shouldEmit := h.shouldEmitUnmatched(
			h.probingRowState.row, leftSide,
		); shouldEmit {
			return hjReadingLeftSide, renderedRow, nil
		}
		return hjReadingLeftSide, nil, nil
	}

	if err := h.cancelChecker.Check(); err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	}

	leftRow := h.probingRowState.row
	rightRow, err := i.Row()
	if err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	}
	defer i.Next()

	var renderedRow rowenc.EncDatumRow
	renderedRow, err = h.render(leftRow, rightRow)
	if err != nil {
		h.MoveToDraining(err)
		return hjStateUnknown, nil, h.DrainHelper()
	}
	// If the ON condition failed, renderedRow is nil.
	if renderedRow == nil {
		return hjProbingRow, nil, nil
	}

	h.probingRowState.matched = true
	shouldEmit := true
	if shouldMarkRightSide(h.joinType) {
		if i.IsMarked(h.Ctx) {
			switch h.joinType {
			case descpb.RightSemiJoin:
				// The row from the right already had a match and was emitted
				// previously, so we don't emit it for the second time.
				shouldEmit = false
			case descpb.IntersectAllJoin:
				// The row from the right has already been used for the
				// intersection, so we cannot use it again.
				shouldEmit = false
			case descpb.ExceptAllJoin:
				// The row from the right has already been used for the except
				// operation, so we cannot use it again. However, we need to
				// continue probing the same row from the left in order to see
				// whether we have a corresponding unmarked row from the right.
				h.probingRowState.matched = false
			}
		} else if err := i.Mark(h.Ctx); err != nil {
			h.MoveToDraining(err)
			return hjStateUnknown, nil, h.DrainHelper()
		}
	}

	nextState := hjProbingRow
	switch h.joinType {
	case descpb.LeftSemiJoin:
		// We can short-circuit iterating over the remaining matching rows from
		// the right, so we'll transition to the next row from the left.
		nextState = hjReadingLeftSide
	case descpb.LeftAntiJoin, descpb.RightAntiJoin:
		// We found a matching row, so we don't emit it in case of an anti join.
		shouldEmit = false
	case descpb.ExceptAllJoin:
		// We're definitely not emitting the combination of the current left
		// and right rows right now. If the current right row has already been
		// used, then h.probingRowState.matched is set to false, and we might
		// emit the current left row if we're at the end of the bucket on the
		// next probeRow() call.
		shouldEmit = false
		if h.probingRowState.matched {
			// We have found a match for the current row on the left, so we'll
			// transition to the next one.
			nextState = hjReadingLeftSide
		}
	case descpb.IntersectAllJoin:
		if shouldEmit {
			// We have found a match for the current row on the left, so we'll
			// transition to the next row from the left.
			nextState = hjReadingLeftSide
		}
	}

	if shouldEmit {
		return nextState, renderedRow, nil
	}
	return nextState, nil, nil
}

func (h *hashJoiner) emitRightUnmatched() (
	hashJoinerState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	i := h.emittingRightUnmatchedState.iter
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

	return hjEmittingRightUnmatched, h.renderUnmatchedRow(row, rightSide), nil
}

func (h *hashJoiner) close() {
	if h.InternalClose() {
		h.hashTable.Close(h.Ctx)
		if h.probingRowState.iter != nil {
			h.probingRowState.iter.Close()
		}
		if h.emittingRightUnmatchedState.iter != nil {
			h.emittingRightUnmatchedState.iter.Close()
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
) (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata, bool, error) {
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
	row rowenc.EncDatumRow, side joinSide,
) (rowenc.EncDatumRow, bool) {
	if !shouldEmitUnmatchedRow(side, h.joinType) {
		return nil, false
	}
	return h.renderUnmatchedRow(row, side), true
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (h *hashJoiner) execStatsForTrace() *execinfrapb.ComponentStats {
	lis, ok := getInputStats(h.leftSource)
	if !ok {
		return nil
	}
	ris, ok := getInputStats(h.rightSource)
	if !ok {
		return nil
	}
	return &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{lis, ris},
		Exec: execinfrapb.ExecStats{
			MaxAllocatedMem:  optional.MakeUint(uint64(h.MemMonitor.MaximumBytes())),
			MaxAllocatedDisk: optional.MakeUint(uint64(h.diskMonitor.MaximumBytes())),
		},
		Output: h.OutputHelper.Stats(),
	}
}

// Some types of joins need to mark rows that matched.
func shouldMarkRightSide(joinType descpb.JoinType) bool {
	switch joinType {
	case descpb.FullOuterJoin, descpb.RightOuterJoin, descpb.RightAntiJoin:
		// For right/full outer joins and right anti joins we need to mark the
		// rows from the right side in order to iterate through the unmatched
		// rows in hjEmittingRightUnmatched state.
		return true
	case descpb.RightSemiJoin:
		// For right semi joins we need to mark the rows in order to track
		// whether we have already emitted an output row corresponding to it.
		return true
	case descpb.IntersectAllJoin, descpb.ExceptAllJoin:
		// For set-operation joins we need to mark the rows in order to track
		// whether they have already been used for a set operation (our row
		// containers don't know how to delete the rows), so we reuse the
		// marking infrastructure to simulate "deleted" rows.
		return true
	default:
		return false
	}
}

// ChildCount is part of the execinfra.OpNode interface.
func (h *hashJoiner) ChildCount(verbose bool) int {
	if _, ok := h.leftSource.(execinfra.OpNode); ok {
		if _, ok := h.rightSource.(execinfra.OpNode); ok {
			return 2
		}
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (h *hashJoiner) Child(nth int, verbose bool) execinfra.OpNode {
	switch nth {
	case 0:
		if n, ok := h.leftSource.(execinfra.OpNode); ok {
			return n
		}
		panic("left input to hashJoiner is not an execinfra.OpNode")
	case 1:
		if n, ok := h.rightSource.(execinfra.OpNode); ok {
			return n
		}
		panic("right input to hashJoiner is not an execinfra.OpNode")
	default:
		panic(errors.AssertionFailedf("invalid index %d", nth))
	}
}
