// Copyright 2018 The Cockroach Authors.
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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/errors"
)

// windowerState represents the state of the processor.
type windowerState int

const (
	windowerStateUnknown windowerState = iota
	// windowerAccumulating means that rows are being read from the input
	// and accumulated in allRowsPartitioned.
	windowerAccumulating
	// windowerEmittingRows means that all rows have been read and
	// output rows are being emitted.
	windowerEmittingRows
)

// memRequiredByWindower indicates the minimum amount of RAM (in bytes) that
// the windower needs.
const memRequiredByWindower = 100 * 1024

// windower is the processor that performs computation of window functions
// that have the same PARTITION BY clause. It passes through all of its input
// columns and puts the output of a window function windowFn at
// windowFn.outputColIdx.
type windower struct {
	execinfra.ProcessorBase

	// runningState represents the state of the windower. This is in addition to
	// ProcessorBase.State - the runningState is only relevant when
	// ProcessorBase.State == StateRunning.
	runningState windowerState
	input        execinfra.RowSource
	inputDone    bool
	inputTypes   []*types.T
	outputTypes  []*types.T
	datumAlloc   rowenc.DatumAlloc
	acc          mon.BoundAccount
	diskMonitor  *mon.BytesMonitor

	scratch       []byte
	cancelChecker *cancelchecker.CancelChecker

	partitionBy                []uint32
	allRowsPartitioned         *rowcontainer.HashDiskBackedRowContainer
	partition                  *rowcontainer.DiskBackedIndexedRowContainer
	orderOfWindowFnsProcessing []int
	windowFns                  []*windowFunc
	builtins                   []tree.WindowFunc

	populated           bool
	partitionIdx        int
	rowsInBucketEmitted int
	partitionSizes      []int
	windowValues        [][][]tree.Datum
	allRowsIterator     rowcontainer.RowIterator
	outputRow           rowenc.EncDatumRow
}

var _ execinfra.Processor = &windower{}
var _ execinfra.RowSource = &windower{}
var _ execinfra.OpNode = &windower{}

const windowerProcName = "windower"

func newWindower(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.WindowerSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*windower, error) {
	w := &windower{
		input: input,
	}
	evalCtx := flowCtx.NewEvalCtx()
	w.inputTypes = input.OutputTypes()
	ctx := evalCtx.Ctx()

	w.partitionBy = spec.PartitionBy
	windowFns := spec.WindowFns
	w.windowFns = make([]*windowFunc, 0, len(windowFns))
	w.builtins = make([]tree.WindowFunc, 0, len(windowFns))
	// windower passes through all of its input columns and appends an output
	// column for each of window functions it is computing.
	w.outputTypes = make([]*types.T, len(w.inputTypes)+len(windowFns))
	copy(w.outputTypes, w.inputTypes)
	for _, windowFn := range windowFns {
		// Check for out of bounds arguments has been done during planning step.
		argTypes := make([]*types.T, len(windowFn.ArgsIdxs))
		for i, argIdx := range windowFn.ArgsIdxs {
			argTypes[i] = w.inputTypes[argIdx]
		}
		windowConstructor, outputType, err := execinfrapb.GetWindowFunctionInfo(windowFn.Func, argTypes...)
		if err != nil {
			return nil, err
		}
		w.outputTypes[windowFn.OutputColIdx] = outputType

		w.builtins = append(w.builtins, windowConstructor(evalCtx))
		wf := &windowFunc{
			ordering:     windowFn.Ordering,
			argsIdxs:     windowFn.ArgsIdxs,
			frame:        windowFn.Frame,
			filterColIdx: int(windowFn.FilterColIdx),
			outputColIdx: int(windowFn.OutputColIdx),
		}

		w.windowFns = append(w.windowFns, wf)
	}
	w.outputRow = make(rowenc.EncDatumRow, len(w.outputTypes))

	// Limit the memory use by creating a child monitor with a hard limit.
	// windower will overflow to disk if this limit is not enough.
	limit := execinfra.GetWorkMemLimit(flowCtx)
	if limit < memRequiredByWindower {
		if !flowCtx.Cfg.TestingKnobs.ForceDiskSpill && flowCtx.Cfg.TestingKnobs.MemoryLimitBytes == 0 {
			return nil, errors.Errorf(
				"window functions require %d bytes of RAM but only %d are in the budget. "+
					"Consider increasing sql.distsql.temp_storage.workmem cluster setting or distsql_workmem session variable",
				memRequiredByWindower, limit)
		}
		// The limit is set very low by the tests, but the windower requires
		// some amount of RAM, so we override the limit.
		limit = memRequiredByWindower
	}
	limitedMon := mon.NewMonitorInheritWithLimit("windower-limited", limit, evalCtx.Mon)
	limitedMon.Start(ctx, evalCtx.Mon, mon.BoundAccount{})

	if err := w.InitWithEvalCtx(
		w,
		post,
		w.outputTypes,
		flowCtx,
		evalCtx,
		processorID,
		output,
		limitedMon,
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{w.input},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				w.close()
				return nil
			}},
	); err != nil {
		return nil, err
	}

	w.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.DiskMonitor, "windower-disk")
	w.allRowsPartitioned = rowcontainer.NewHashDiskBackedRowContainer(
		evalCtx, w.MemMonitor, w.diskMonitor, flowCtx.Cfg.TempStorage,
	)
	if err := w.allRowsPartitioned.Init(
		ctx,
		false, /* shouldMark */
		w.inputTypes,
		w.partitionBy,
		true, /* encodeNull */
	); err != nil {
		return nil, err
	}

	w.acc = w.MemMonitor.MakeBoundAccount()
	// If we have aggregate builtins that aggregate a single datum, we want
	// them to reuse the same shared memory account with the windower.
	evalCtx.SingleDatumAggMemAccount = &w.acc

	if execinfra.ShouldCollectStats(ctx, flowCtx) {
		w.input = newInputStatCollector(w.input)
		w.ExecStatsForTrace = w.execStatsForTrace
	}

	return w, nil
}

// Start is part of the RowSource interface.
func (w *windower) Start(ctx context.Context) {
	ctx = w.StartInternal(ctx, windowerProcName)
	w.input.Start(ctx)
	w.cancelChecker = cancelchecker.NewCancelChecker(ctx)
	w.runningState = windowerAccumulating
}

// Next is part of the RowSource interface.
func (w *windower) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for w.State == execinfra.StateRunning {
		var row rowenc.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch w.runningState {
		case windowerAccumulating:
			w.runningState, row, meta = w.accumulateRows()
		case windowerEmittingRows:
			w.runningState, row, meta = w.emitRow()
		default:
			log.Fatalf(w.Ctx, "unsupported state: %d", w.runningState)
		}

		if row == nil && meta == nil {
			continue
		}
		return row, meta
	}
	return nil, w.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (w *windower) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	w.close()
}

func (w *windower) close() {
	if w.InternalClose() {
		if w.allRowsIterator != nil {
			w.allRowsIterator.Close()
		}
		w.allRowsPartitioned.Close(w.Ctx)
		if w.partition != nil {
			w.partition.Close(w.Ctx)
		}
		for _, builtin := range w.builtins {
			builtin.Close(w.Ctx, w.EvalCtx)
		}
		w.acc.Close(w.Ctx)
		w.MemMonitor.Stop(w.Ctx)
		w.diskMonitor.Stop(w.Ctx)
	}
}

// accumulateRows continually reads rows from the input and accumulates them
// in allRowsPartitioned. If it encounters metadata, the metadata is returned
// immediately. Subsequent calls of this function will resume row accumulation.
func (w *windower) accumulateRows() (
	windowerState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	for {
		row, meta := w.input.Next()
		if meta != nil {
			if meta.Err != nil {
				// We want to send the whole meta (below) rather than just the err,
				// so we pass nil as an argument.
				w.MoveToDraining(nil /* err */)
				return windowerStateUnknown, nil, meta
			}
			return windowerAccumulating, nil, meta
		}
		if row == nil {
			log.VEvent(w.Ctx, 1, "accumulation complete")
			w.inputDone = true
			// We need to sort all the rows based on partitionBy columns so that all
			// rows belonging to the same hash bucket are contiguous.
			w.allRowsPartitioned.Sort(w.Ctx)
			break
		}

		// The underlying row container will decode all datums as necessary, so we
		// don't need to worry about that.
		if err := w.allRowsPartitioned.AddRow(w.Ctx, row); err != nil {
			w.MoveToDraining(err)
			return windowerStateUnknown, nil, w.DrainHelper()
		}
	}

	return windowerEmittingRows, nil, nil
}

// emitRow emits the next row if output rows have already been populated;
// if they haven't, it first computes all window functions over all partitions
// (i.e. populates w.windowValues), and then emits the first row.
//
// emitRow() might move to stateDraining. It might also not return a row if the
// ProcOutputHelper filtered the current row out.
func (w *windower) emitRow() (windowerState, rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if w.inputDone {
		for !w.populated {
			if err := w.cancelChecker.Check(); err != nil {
				w.MoveToDraining(err)
				return windowerStateUnknown, nil, w.DrainHelper()
			}

			if err := w.computeWindowFunctions(w.Ctx, w.EvalCtx); err != nil {
				w.MoveToDraining(err)
				return windowerStateUnknown, nil, w.DrainHelper()
			}
			w.populated = true
		}

		if rowOutputted, err := w.populateNextOutputRow(); err != nil {
			w.MoveToDraining(err)
			return windowerStateUnknown, nil, nil
		} else if rowOutputted {
			return windowerEmittingRows, w.ProcessRowHelper(w.outputRow), nil
		}

		w.MoveToDraining(nil /* err */)
		return windowerStateUnknown, nil, nil
	}

	w.MoveToDraining(errors.Errorf("unexpected: emitRow() is called on a windower before all input rows are accumulated"))
	return windowerStateUnknown, nil, w.DrainHelper()
}

// spillAllRowsToDisk attempts to first spill w.allRowsPartitioned to disk if
// it's using memory. We choose to not to force w.partition to spill right away
// since it might be resorted multiple times with different orderings, so it's
// better to keep it in memory (if it hasn't spilled on its own). If
// w.allRowsPartitioned is already using disk, we attempt to spill w.partition.
func (w *windower) spillAllRowsToDisk() error {
	if w.allRowsPartitioned != nil {
		if !w.allRowsPartitioned.UsingDisk() {
			if err := w.allRowsPartitioned.SpillToDisk(w.Ctx); err != nil {
				return err
			}
		} else {
			// w.allRowsPartitioned has already been spilled, so we have to spill
			// w.partition if possible.
			if w.partition != nil {
				if !w.partition.UsingDisk() {
					if err := w.partition.SpillToDisk(w.Ctx); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// growMemAccount attempts to grow acc by usage, and if it encounters OOM
// error, it forces all rows to spill and attempts to grow acc by usage
// one more time.
func (w *windower) growMemAccount(acc *mon.BoundAccount, usage int64) error {
	if err := acc.Grow(w.Ctx, usage); err != nil {
		if sqlerrors.IsOutOfMemoryError(err) {
			if err := w.spillAllRowsToDisk(); err != nil {
				return err
			}
			if err := acc.Grow(w.Ctx, usage); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

// findOrderOfWindowFnsToProcessIn finds an ordering of window functions such
// that all window functions that have the same ORDER BY clause are computed
// one after another. The order is stored in w.orderOfWindowFnsProcessing.
// This allows for using the same row container without having to resort it
// multiple times.
func (w *windower) findOrderOfWindowFnsToProcessIn() {
	w.orderOfWindowFnsProcessing = make([]int, 0, len(w.windowFns))
	windowFnAdded := make([]bool, len(w.windowFns))
	for i, windowFn := range w.windowFns {
		if !windowFnAdded[i] {
			w.orderOfWindowFnsProcessing = append(w.orderOfWindowFnsProcessing, i)
			windowFnAdded[i] = true
		}
		for j := i + 1; j < len(w.windowFns); j++ {
			if windowFnAdded[j] {
				// j'th windowFn has been already added to orderOfWindowFnsProcessing.
				continue
			}
			if windowFn.ordering.Equal(w.windowFns[j].ordering) {
				w.orderOfWindowFnsProcessing = append(w.orderOfWindowFnsProcessing, j)
				windowFnAdded[j] = true
			}
		}
	}
}

// processPartition computes all window functions over the given partition and
// puts the result of computations in w.windowValues[partitionIdx]. It computes
// window functions in the order specified in w.orderOfWindowFnsProcessing.
// The same ReorderableRowContainer for partition is reused with changing the
// ordering and being resorted as necessary.
//
// Note: partition must have the ordering as needed by the first window
// function to be processed.
func (w *windower) processPartition(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	partition *rowcontainer.DiskBackedIndexedRowContainer,
	partitionIdx int,
) error {
	peerGrouper := &partitionPeerGrouper{
		ctx:     ctx,
		evalCtx: evalCtx,
		rowCopy: make(rowenc.EncDatumRow, len(w.inputTypes)),
	}
	usage := sizeOfSliceOfRows + rowSliceOverhead + sizeOfRow*int64(len(w.windowFns))
	if err := w.growMemAccount(&w.acc, usage); err != nil {
		return err
	}
	w.windowValues = append(w.windowValues, make([][]tree.Datum, len(w.windowFns)))

	// Partition has ordering as first window function to be processed needs, but
	// we need to sort the partition for the ordering to take effect.
	partition.Sort(ctx)

	var prevWindowFn *windowFunc
	for _, windowFnIdx := range w.orderOfWindowFnsProcessing {
		windowFn := w.windowFns[windowFnIdx]

		// TODO(yuzefovich): creating a new WindowFrameRun object for each
		// partition and populating it below for a custom window frame is
		// suboptimal. Consider extracting this logic into the constructor of
		// the windower and reusing the same objects between partitions.
		frameRun := &tree.WindowFrameRun{
			ArgsIdxs:     windowFn.argsIdxs,
			FilterColIdx: windowFn.filterColIdx,
		}

		if windowFn.frame != nil {
			var err error
			if frameRun.Frame, err = windowFn.frame.ConvertToAST(); err != nil {
				return err
			}
			startBound, endBound := windowFn.frame.Bounds.Start, windowFn.frame.Bounds.End
			if startBound.BoundType == execinfrapb.WindowerSpec_Frame_OFFSET_PRECEDING ||
				startBound.BoundType == execinfrapb.WindowerSpec_Frame_OFFSET_FOLLOWING {
				switch windowFn.frame.Mode {
				case execinfrapb.WindowerSpec_Frame_ROWS:
					frameRun.StartBoundOffset = tree.NewDInt(tree.DInt(int(startBound.IntOffset)))
				case execinfrapb.WindowerSpec_Frame_RANGE:
					datum, rem, err := rowenc.DecodeTableValue(&w.datumAlloc, startBound.OffsetType.Type, startBound.TypedOffset)
					if err != nil {
						return errors.NewAssertionErrorWithWrappedErrf(err,
							"error decoding %d bytes", errors.Safe(len(startBound.TypedOffset)))
					}
					if len(rem) != 0 {
						return errors.AssertionFailedf(
							"%d trailing bytes in encoded value", errors.Safe(len(rem)))
					}
					frameRun.StartBoundOffset = datum
				case execinfrapb.WindowerSpec_Frame_GROUPS:
					frameRun.StartBoundOffset = tree.NewDInt(tree.DInt(int(startBound.IntOffset)))
				default:
					return errors.AssertionFailedf(
						"unexpected WindowFrameMode: %d", errors.Safe(windowFn.frame.Mode))
				}
			}
			if endBound != nil {
				if endBound.BoundType == execinfrapb.WindowerSpec_Frame_OFFSET_PRECEDING ||
					endBound.BoundType == execinfrapb.WindowerSpec_Frame_OFFSET_FOLLOWING {
					switch windowFn.frame.Mode {
					case execinfrapb.WindowerSpec_Frame_ROWS:
						frameRun.EndBoundOffset = tree.NewDInt(tree.DInt(int(endBound.IntOffset)))
					case execinfrapb.WindowerSpec_Frame_RANGE:
						datum, rem, err := rowenc.DecodeTableValue(&w.datumAlloc, endBound.OffsetType.Type, endBound.TypedOffset)
						if err != nil {
							return errors.NewAssertionErrorWithWrappedErrf(err,
								"error decoding %d bytes", errors.Safe(len(endBound.TypedOffset)))
						}
						if len(rem) != 0 {
							return errors.AssertionFailedf(
								"%d trailing bytes in encoded value", errors.Safe(len(rem)))
						}
						frameRun.EndBoundOffset = datum
					case execinfrapb.WindowerSpec_Frame_GROUPS:
						frameRun.EndBoundOffset = tree.NewDInt(tree.DInt(int(endBound.IntOffset)))
					default:
						return errors.AssertionFailedf("unexpected WindowFrameMode: %d",
							errors.Safe(windowFn.frame.Mode))
					}
				}
			}
			if frameRun.RangeModeWithOffsets() {
				ordCol := windowFn.ordering.Columns[0]
				frameRun.OrdColIdx = int(ordCol.ColIdx)
				// We need this +1 because encoding.Direction has extra value "_"
				// as zeroth "entry" which its proto equivalent doesn't have.
				frameRun.OrdDirection = encoding.Direction(ordCol.Direction + 1)

				colTyp := w.inputTypes[ordCol.ColIdx]
				// Type of offset depends on the ordering column's type.
				offsetTyp := colTyp
				if types.IsDateTimeType(colTyp) {
					// For datetime related ordering columns, offset must be an Interval.
					offsetTyp = types.Interval
				}
				plusOp, minusOp, found := tree.WindowFrameRangeOps{}.LookupImpl(colTyp, offsetTyp)
				if !found {
					return pgerror.Newf(pgcode.Windowing,
						"given logical offset cannot be combined with ordering column")
				}
				frameRun.PlusOp, frameRun.MinusOp = plusOp, minusOp
			}
		}

		builtin := w.builtins[windowFnIdx]
		builtin.Reset(ctx)

		usage = datumSliceOverhead + sizeOfDatum*int64(partition.Len())
		if err := w.growMemAccount(&w.acc, usage); err != nil {
			return err
		}
		w.windowValues[partitionIdx][windowFnIdx] = make([]tree.Datum, partition.Len())

		if len(windowFn.ordering.Columns) > 0 {
			// If an ORDER BY clause is provided, we check whether the partition is
			// already sorted as we need (i.e. prevWindowFn has the same ordering),
			// and if it is not, we change the ordering to the needed and resort the
			// container.
			if prevWindowFn != nil && !windowFn.ordering.Equal(prevWindowFn.ordering) {
				if err := partition.Reorder(ctx, execinfrapb.ConvertToColumnOrdering(windowFn.ordering)); err != nil {
					return err
				}
				partition.Sort(ctx)
			}
		}
		peerGrouper.ordering = windowFn.ordering
		peerGrouper.partition = partition

		frameRun.Rows = partition
		frameRun.RowIdx = 0

		if !frameRun.Frame.IsDefaultFrame() {
			// We have a custom frame not equivalent to default one, so if we have
			// an aggregate function, we want to reset it for each row. Not resetting
			// is an optimization since we're not computing the result over the whole
			// frame but only as a result of the current row and previous results of
			// aggregation.
			builtins.ShouldReset(builtin)
		}

		if err := frameRun.PeerHelper.Init(frameRun, peerGrouper); err != nil {
			return err
		}
		frameRun.CurRowPeerGroupNum = 0

		var prevRes tree.Datum
		for frameRun.RowIdx < partition.Len() {
			// Perform calculations on each row in the current peer group.
			peerGroupEndIdx := frameRun.PeerHelper.GetFirstPeerIdx(frameRun.CurRowPeerGroupNum) + frameRun.PeerHelper.GetRowCount(frameRun.CurRowPeerGroupNum)
			for ; frameRun.RowIdx < peerGroupEndIdx; frameRun.RowIdx++ {
				if err := w.cancelChecker.Check(); err != nil {
					return err
				}
				res, err := builtin.Compute(ctx, evalCtx, frameRun)
				if err != nil {
					return err
				}
				row, err := frameRun.Rows.GetRow(ctx, frameRun.RowIdx)
				if err != nil {
					return err
				}
				if prevRes == nil || prevRes != res {
					// We don't want to double count the same memory, and since the same
					// memory can only be reused contiguously as res, comparing against
					// result of the previous row is sufficient.
					// We have already accounted for the size of a nil datum prior to
					// allocating the slice for window values, so we need to keep that in
					// mind.
					if err := w.growMemAccount(&w.acc, int64(res.Size())-sizeOfDatum); err != nil {
						return err
					}
				}
				w.windowValues[partitionIdx][windowFnIdx][row.GetIdx()] = res
				prevRes = res
			}
			if err := frameRun.PeerHelper.Update(frameRun); err != nil {
				return err
			}
			frameRun.CurRowPeerGroupNum++
		}

		prevWindowFn = windowFn
	}

	if err := w.growMemAccount(&w.acc, sizeOfInt); err != nil {
		return err
	}
	w.partitionSizes = append(w.partitionSizes, w.partition.Len())
	return nil
}

// computeWindowFunctions computes all window functions over all partitions.
// Partitions are processed one at a time with the underlying row container
// reused (and reordered if needed).
func (w *windower) computeWindowFunctions(ctx context.Context, evalCtx *tree.EvalContext) error {
	w.findOrderOfWindowFnsToProcessIn()

	// We don't know how many partitions there are, so we'll be accounting for
	// this memory right before every append to these slices.
	usage := sliceOfIntsOverhead + sliceOfRowsSliceOverhead
	if err := w.growMemAccount(&w.acc, usage); err != nil {
		return err
	}
	w.partitionSizes = make([]int, 0, 8)
	w.windowValues = make([][][]tree.Datum, 0, 8)
	bucket := ""

	// w.partition will have ordering as needed by the first window function to
	// be processed.
	ordering := execinfrapb.ConvertToColumnOrdering(w.windowFns[w.orderOfWindowFnsProcessing[0]].ordering)
	w.partition = rowcontainer.NewDiskBackedIndexedRowContainer(
		ordering,
		w.inputTypes,
		w.EvalCtx,
		w.FlowCtx.Cfg.TempStorage,
		w.MemMonitor,
		w.diskMonitor,
	)
	i, err := w.allRowsPartitioned.NewAllRowsIterator(ctx)
	if err != nil {
		return err
	}
	defer i.Close()

	// We iterate over all the rows and add them to w.partition one by one. When
	// a row from a different partition is encountered, we process the partition
	// and reset w.partition for reusing.
	for i.Rewind(); ; i.Next() {
		if ok, err := i.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		row, err := i.Row()
		if err != nil {
			return err
		}
		if err := w.cancelChecker.Check(); err != nil {
			return err
		}
		if len(w.partitionBy) > 0 {
			// We need to hash the row according to partitionBy
			// to figure out which partition the row belongs to.
			w.scratch = w.scratch[:0]
			for _, col := range w.partitionBy {
				if int(col) >= len(row) {
					return errors.AssertionFailedf(
						"hash column %d, row with only %d columns", errors.Safe(col), errors.Safe(len(row)))
				}
				var err error
				// We might allocate tree.Datums when hashing the row, so we'll
				// ask the fingerprint to account for them. Note that if the
				// datums are later used by the window functions (and accounted
				// for accordingly), this can lead to over-accounting which is
				// acceptable.
				w.scratch, err = row[col].Fingerprint(
					ctx, w.inputTypes[int(col)], &w.datumAlloc, w.scratch, &w.acc,
				)
				if err != nil {
					return err
				}
			}
			if string(w.scratch) != bucket {
				// Current row is from the new bucket, so we "finalize" the previous
				// bucket (if current row is not the first row among all rows in
				// allRowsPartitioned). We then process this partition, reset the
				// container for reuse by the next partition.
				if bucket != "" {
					if err := w.processPartition(ctx, evalCtx, w.partition, len(w.partitionSizes)); err != nil {
						return err
					}
				}
				bucket = string(w.scratch)
				if err := w.partition.UnsafeReset(ctx); err != nil {
					return err
				}
				if !w.windowFns[w.orderOfWindowFnsProcessing[0]].ordering.Equal(w.windowFns[w.orderOfWindowFnsProcessing[len(w.windowFns)-1]].ordering) {
					// The container no longer has the ordering as needed by the first
					// window function to be processed, so we need to change it.
					if err = w.partition.Reorder(ctx, ordering); err != nil {
						return err
					}
				}
			}
		}
		if err := w.partition.AddRow(w.Ctx, row); err != nil {
			return err
		}
	}
	return w.processPartition(ctx, evalCtx, w.partition, len(w.partitionSizes))
}

// populateNextOutputRow populates next output row to be returned. All input
// columns are passed through, and the results of window functions'
// computations are put in the desired columns (i.e. in outputColIdx of each
// window function).
func (w *windower) populateNextOutputRow() (bool, error) {
	if w.partitionIdx < len(w.partitionSizes) {
		if w.allRowsIterator == nil {
			w.allRowsIterator = w.allRowsPartitioned.NewUnmarkedIterator(w.Ctx)
			w.allRowsIterator.Rewind()
		}
		// rowIdx is the index of the next row to be emitted from the
		// partitionIdx'th partition.
		rowIdx := w.rowsInBucketEmitted
		if ok, err := w.allRowsIterator.Valid(); err != nil {
			return false, err
		} else if !ok {
			return false, nil
		}
		inputRow, err := w.allRowsIterator.Row()
		w.allRowsIterator.Next()
		if err != nil {
			return false, err
		}
		copy(w.outputRow, inputRow[:len(w.inputTypes)])
		for windowFnIdx, windowFn := range w.windowFns {
			windowFnRes := w.windowValues[w.partitionIdx][windowFnIdx][rowIdx]
			encWindowFnRes := rowenc.DatumToEncDatum(w.outputTypes[windowFn.outputColIdx], windowFnRes)
			w.outputRow[windowFn.outputColIdx] = encWindowFnRes
		}
		w.rowsInBucketEmitted++
		if w.rowsInBucketEmitted == w.partitionSizes[w.partitionIdx] {
			// We have emitted all rows from the current bucket, so we advance the
			// iterator.
			w.partitionIdx++
			w.rowsInBucketEmitted = 0
		}
		return true, nil

	}
	return false, nil
}

type windowFunc struct {
	ordering     execinfrapb.Ordering
	argsIdxs     []uint32
	frame        *execinfrapb.WindowerSpec_Frame
	filterColIdx int
	outputColIdx int
}

type partitionPeerGrouper struct {
	ctx       context.Context
	evalCtx   *tree.EvalContext
	partition *rowcontainer.DiskBackedIndexedRowContainer
	ordering  execinfrapb.Ordering
	rowCopy   rowenc.EncDatumRow
	err       error
}

func (n *partitionPeerGrouper) InSameGroup(i, j int) (bool, error) {
	if len(n.ordering.Columns) == 0 {
		// ORDER BY clause is omitted, so all rows are peers.
		return true, nil
	}
	if n.err != nil {
		return false, n.err
	}
	indexedRow, err := n.partition.GetRow(n.ctx, i)
	if err != nil {
		n.err = err
		return false, err
	}
	row := indexedRow.(rowcontainer.IndexedRow)
	// We need to copy the row explicitly since n.partition might be reusing
	// the underlying memory when GetRow() is called.
	copy(n.rowCopy, row.Row)
	rb, err := n.partition.GetRow(n.ctx, j)
	if err != nil {
		n.err = err
		return false, n.err
	}
	for _, o := range n.ordering.Columns {
		da := n.rowCopy[o.ColIdx].Datum
		db, err := rb.GetDatum(int(o.ColIdx))
		if err != nil {
			n.err = err
			return false, n.err
		}
		if c := da.Compare(n.evalCtx, db); c != 0 {
			if o.Direction != execinfrapb.Ordering_Column_ASC {
				return false, nil
			}
			return false, nil
		}
	}
	return true, nil
}

const sizeOfInt = int64(unsafe.Sizeof(int(0)))
const sliceOfIntsOverhead = int64(unsafe.Sizeof([]int{}))
const sizeOfSliceOfRows = int64(unsafe.Sizeof([][]tree.Datum{}))
const sliceOfRowsSliceOverhead = int64(unsafe.Sizeof([][][]tree.Datum{}))
const sizeOfRow = int64(unsafe.Sizeof([]tree.Datum{}))
const rowSliceOverhead = int64(unsafe.Sizeof([][]tree.Datum{}))
const sizeOfDatum = int64(unsafe.Sizeof(tree.Datum(nil)))
const datumSliceOverhead = int64(unsafe.Sizeof([]tree.Datum(nil)))

// CreateWindowerSpecFunc creates a WindowerSpec_Func based on the function
// name or returns an error if unknown function name is provided.
func CreateWindowerSpecFunc(funcStr string) (execinfrapb.WindowerSpec_Func, error) {
	if aggBuiltin, err := execinfrapb.GetAggregateFuncIdx(funcStr); err == nil {
		aggSpec := execinfrapb.AggregatorSpec_Func(aggBuiltin)
		return execinfrapb.WindowerSpec_Func{AggregateFunc: &aggSpec}, nil
	} else if winBuiltin, err := execinfrapb.GetWindowFuncIdx(funcStr); err == nil {
		winSpec := execinfrapb.WindowerSpec_WindowFunc(winBuiltin)
		return execinfrapb.WindowerSpec_Func{WindowFunc: &winSpec}, nil
	} else {
		return execinfrapb.WindowerSpec_Func{}, errors.Errorf("unknown aggregate/window function %s", funcStr)
	}
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (w *windower) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := getInputStats(w.input)
	if !ok {
		return nil
	}
	return &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		Exec: execinfrapb.ExecStats{
			MaxAllocatedMem:  optional.MakeUint(uint64(w.MemMonitor.MaximumBytes())),
			MaxAllocatedDisk: optional.MakeUint(uint64(w.diskMonitor.MaximumBytes())),
		},
		Output: w.OutputHelper.Stats(),
	}
}

// ChildCount is part of the execinfra.OpNode interface.
func (w *windower) ChildCount(verbose bool) int {
	if _, ok := w.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (w *windower) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := w.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to windower is not an execinfra.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
