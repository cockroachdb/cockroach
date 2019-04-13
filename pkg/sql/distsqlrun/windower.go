// Copyright 2018 The Cockroach Authors.
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
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// GetWindowFunctionInfo returns windowFunc constructor and the return type
// when given fn is applied to given inputTypes.
func GetWindowFunctionInfo(
	fn distsqlpb.WindowerSpec_Func, inputTypes ...types.T,
) (windowConstructor func(*tree.EvalContext) tree.WindowFunc, returnType types.T, err error) {
	if fn.AggregateFunc != nil && *fn.AggregateFunc == distsqlpb.AggregatorSpec_ANY_NOT_NULL {
		// The ANY_NOT_NULL builtin does not have a fixed return type;
		// handle it separately.
		if len(inputTypes) != 1 {
			return nil, types.T{}, errors.Errorf("any_not_null aggregate needs 1 input")
		}
		return builtins.NewAggregateWindowFunc(builtins.NewAnyNotNullAggregate), inputTypes[0], nil
	}
	datumTypes := make([]*types.T, len(inputTypes))
	for i := range inputTypes {
		datumTypes[i] = &inputTypes[i]
	}

	var funcStr string
	if fn.AggregateFunc != nil {
		funcStr = fn.AggregateFunc.String()
	} else if fn.WindowFunc != nil {
		funcStr = fn.WindowFunc.String()
	} else {
		return nil, types.T{}, errors.Errorf(
			"function is neither an aggregate nor a window function",
		)
	}
	props, builtins := builtins.GetBuiltinProperties(strings.ToLower(funcStr))
	for _, b := range builtins {
		typs := b.Types.Types()
		if len(typs) != len(inputTypes) {
			continue
		}
		match := true
		for i, t := range typs {
			if !datumTypes[i].Equivalent(t) {
				if props.NullableArgs && datumTypes[i].IsAmbiguous() {
					continue
				}
				match = false
				break
			}
		}
		if match {
			// Found!
			constructAgg := func(evalCtx *tree.EvalContext) tree.WindowFunc {
				return b.WindowFunc(datumTypes, evalCtx)
			}

			colTyp := *b.FixedReturnType()
			return constructAgg, colTyp, nil
		}
	}
	return nil, types.T{}, errors.Errorf(
		"no builtin aggregate/window function for %s on %v", funcStr, inputTypes,
	)
}

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

// windower is the processor that performs computation of window functions
// that have the same PARTITION BY clause. It puts the output of a window
// function windowFn at windowFn.argIdxStart and "consumes" columns
// windowFn.argIdxStart : windowFn.argIdxStart+windowFn.argCount,
// so it can both add (when argCount = 0) and remove (when argCount > 1) columns.
type windower struct {
	ProcessorBase

	// runningState represents the state of the windower. This is in addition to
	// ProcessorBase.State - the runningState is only relevant when
	// ProcessorBase.State == StateRunning.
	runningState windowerState
	input        RowSource
	inputDone    bool
	inputTypes   []types.T
	outputTypes  []types.T
	datumAlloc   sqlbase.DatumAlloc
	acc          mon.BoundAccount
	diskMonitor  *mon.BytesMonitor

	scratch       []byte
	cancelChecker *sqlbase.CancelChecker

	partitionBy                []uint32
	allRowsPartitioned         *rowcontainer.HashDiskBackedRowContainer
	partition                  *rowcontainer.DiskBackedIndexedRowContainer
	orderOfWindowFnsProcessing []int
	windowFns                  []*windowFunc

	populated           bool
	partitionIdx        int
	rowsInBucketEmitted int
	partitionSizes      []int
	windowValues        [][][]tree.Datum
	allRowsIterator     rowcontainer.RowIterator
	outputRow           sqlbase.EncDatumRow
}

var _ Processor = &windower{}
var _ RowSource = &windower{}

const windowerProcName = "windower"

func newWindower(
	flowCtx *FlowCtx,
	processorID int32,
	spec *distsqlpb.WindowerSpec,
	input RowSource,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
) (*windower, error) {
	w := &windower{
		input: input,
	}
	evalCtx := flowCtx.NewEvalCtx()
	w.inputTypes = input.OutputTypes()
	ctx := evalCtx.Ctx()
	memMonitor := NewMonitor(ctx, evalCtx.Mon, "windower-mem")
	w.acc = memMonitor.MakeBoundAccount()
	w.diskMonitor = NewMonitor(ctx, flowCtx.diskMonitor, "windower-disk")
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		w.input = NewInputStatCollector(w.input)
		w.finishTrace = w.outputStatsToTrace
	}

	windowFns := spec.WindowFns
	w.partitionBy = spec.PartitionBy
	allRowsPartitioned := rowcontainer.MakeHashDiskBackedRowContainer(
		nil, /* memRowContainer */
		evalCtx,
		memMonitor,
		w.diskMonitor,
		flowCtx.TempStorage,
	)
	w.allRowsPartitioned = &allRowsPartitioned
	if err := w.allRowsPartitioned.Init(
		ctx,
		false, /* shouldMark */
		w.inputTypes,
		w.partitionBy,
		true, /* encodeNull */
	); err != nil {
		return nil, err
	}
	w.windowFns = make([]*windowFunc, 0, len(windowFns))
	w.outputTypes = make([]types.T, 0, len(w.inputTypes))

	// inputColIdx is the index of the column that should be processed next.
	inputColIdx := 0
	for _, windowFn := range windowFns {
		// All window functions are sorted by their argIdxStart,
		// so we simply "copy" all columns up to windowFn.argIdxStart
		// (all window functions in samePartitionFuncs after windowFn
		// have their arguments in later columns).
		w.outputTypes = append(w.outputTypes, w.inputTypes[inputColIdx:int(windowFn.ArgIdxStart)]...)

		// Check for out of bounds arguments has been done during planning step.
		argTypes := w.inputTypes[windowFn.ArgIdxStart : windowFn.ArgIdxStart+windowFn.ArgCount]
		windowConstructor, outputType, err := GetWindowFunctionInfo(windowFn.Func, argTypes...)
		if err != nil {
			return nil, err
		}
		// Windower processor consumes all arguments of windowFn
		// and puts the result of computation of this window function
		// at windowFn.argIdxStart.
		w.outputTypes = append(w.outputTypes, outputType)
		inputColIdx = int(windowFn.ArgIdxStart + windowFn.ArgCount)

		wf := &windowFunc{
			create:       windowConstructor,
			ordering:     windowFn.Ordering,
			argIdxStart:  int(windowFn.ArgIdxStart),
			argCount:     int(windowFn.ArgCount),
			frame:        windowFn.Frame,
			filterColIdx: int(windowFn.FilterColIdx),
		}

		w.windowFns = append(w.windowFns, wf)
	}
	// We will simply copy all columns that come after arguments
	// to all window function.
	w.outputTypes = append(w.outputTypes, w.inputTypes[inputColIdx:]...)
	w.outputRow = make(sqlbase.EncDatumRow, len(w.outputTypes))

	if err := w.InitWithEvalCtx(
		w,
		post,
		w.outputTypes,
		flowCtx,
		evalCtx,
		processorID,
		output,
		memMonitor,
		ProcStateOpts{InputsToDrain: []RowSource{w.input},
			TrailingMetaCallback: func(context.Context) []ProducerMetadata {
				w.close()
				return nil
			}},
	); err != nil {
		return nil, err
	}

	return w, nil
}

// Start is part of the RowSource interface.
func (w *windower) Start(ctx context.Context) context.Context {
	w.input.Start(ctx)
	ctx = w.StartInternal(ctx, windowerProcName)
	w.cancelChecker = sqlbase.NewCancelChecker(ctx)
	w.runningState = windowerAccumulating
	return ctx
}

// Next is part of the RowSource interface.
func (w *windower) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for w.State == StateRunning {
		var row sqlbase.EncDatumRow
		var meta *ProducerMetadata
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
		w.acc.Close(w.Ctx)
		w.MemMonitor.Stop(w.Ctx)
		w.diskMonitor.Stop(w.Ctx)
	}
}

// accumulateRows continually reads rows from the input and accumulates them
// in allRowsPartitioned. If it encounters metadata, the metadata is returned
// immediately. Subsequent calls of this function will resume row accumulation.
func (w *windower) accumulateRows() (windowerState, sqlbase.EncDatumRow, *ProducerMetadata) {
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
func (w *windower) emitRow() (windowerState, sqlbase.EncDatumRow, *ProducerMetadata) {
	if w.inputDone {
		for !w.populated {
			if err := w.cancelChecker.Check(); err != nil {
				w.MoveToDraining(err)
				return windowerStateUnknown, nil, w.DrainHelper()
			}

			if err := w.computeWindowFunctions(w.Ctx, w.evalCtx); err != nil {
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
		if sqlbase.IsOutOfMemoryError(err) {
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
	var peerGrouper tree.PeerGroupChecker
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

		frameRun := &tree.WindowFrameRun{
			ArgCount:     windowFn.argCount,
			ArgIdxStart:  windowFn.argIdxStart,
			FilterColIdx: windowFn.filterColIdx,
		}

		if windowFn.frame != nil {
			frameRun.Frame = windowFn.frame.ConvertToAST()
			startBound, endBound := windowFn.frame.Bounds.Start, windowFn.frame.Bounds.End
			if startBound.BoundType == distsqlpb.WindowerSpec_Frame_OFFSET_PRECEDING ||
				startBound.BoundType == distsqlpb.WindowerSpec_Frame_OFFSET_FOLLOWING {
				switch windowFn.frame.Mode {
				case distsqlpb.WindowerSpec_Frame_ROWS:
					frameRun.StartBoundOffset = tree.NewDInt(tree.DInt(int(startBound.IntOffset)))
				case distsqlpb.WindowerSpec_Frame_RANGE:
					datum, rem, err := sqlbase.DecodeTableValue(&w.datumAlloc, &startBound.OffsetType.Type, startBound.TypedOffset)
					if err != nil {
						return pgerror.NewAssertionErrorWithWrappedErrf(err,
							"error decoding %d bytes", log.Safe(len(startBound.TypedOffset)))
					}
					if len(rem) != 0 {
						return pgerror.NewAssertionErrorf(
							"%d trailing bytes in encoded value", log.Safe(len(rem)))
					}
					frameRun.StartBoundOffset = datum
				case distsqlpb.WindowerSpec_Frame_GROUPS:
					frameRun.StartBoundOffset = tree.NewDInt(tree.DInt(int(startBound.IntOffset)))
				default:
					return pgerror.NewAssertionErrorf(
						"unexpected WindowFrameMode: %d", log.Safe(windowFn.frame.Mode))
				}
			}
			if endBound != nil {
				if endBound.BoundType == distsqlpb.WindowerSpec_Frame_OFFSET_PRECEDING ||
					endBound.BoundType == distsqlpb.WindowerSpec_Frame_OFFSET_FOLLOWING {
					switch windowFn.frame.Mode {
					case distsqlpb.WindowerSpec_Frame_ROWS:
						frameRun.EndBoundOffset = tree.NewDInt(tree.DInt(int(endBound.IntOffset)))
					case distsqlpb.WindowerSpec_Frame_RANGE:
						datum, rem, err := sqlbase.DecodeTableValue(&w.datumAlloc, &endBound.OffsetType.Type, endBound.TypedOffset)
						if err != nil {
							return pgerror.NewAssertionErrorWithWrappedErrf(err,
								"error decoding %d bytes", log.Safe(len(endBound.TypedOffset)))
						}
						if len(rem) != 0 {
							return pgerror.NewAssertionErrorf(
								"%d trailing bytes in encoded value", log.Safe(len(rem)))
						}
						frameRun.EndBoundOffset = datum
					case distsqlpb.WindowerSpec_Frame_GROUPS:
						frameRun.EndBoundOffset = tree.NewDInt(tree.DInt(int(endBound.IntOffset)))
					default:
						return pgerror.NewAssertionErrorf("unexpected WindowFrameMode: %d",
							log.Safe(windowFn.frame.Mode))
					}
				}
			}
			if frameRun.RangeModeWithOffsets() {
				ordCol := windowFn.ordering.Columns[0]
				frameRun.OrdColIdx = int(ordCol.ColIdx)
				// We need this +1 because encoding.Direction has extra value "_"
				// as zeroth "entry" which its proto equivalent doesn't have.
				frameRun.OrdDirection = encoding.Direction(ordCol.Direction + 1)

				colTyp := &w.inputTypes[ordCol.ColIdx]
				// Type of offset depends on the ordering column's type.
				offsetTyp := colTyp
				if types.IsDateTimeType(colTyp) {
					// For datetime related ordering columns, offset must be an Interval.
					offsetTyp = types.Interval
				}
				plusOp, minusOp, found := tree.WindowFrameRangeOps{}.LookupImpl(colTyp, offsetTyp)
				if !found {
					return pgerror.NewErrorf(pgerror.CodeWindowingError,
						"given logical offset cannot be combined with ordering column")
				}
				frameRun.PlusOp, frameRun.MinusOp = plusOp, minusOp
			}
		}

		builtin := windowFn.create(evalCtx)
		defer builtin.Close(ctx, evalCtx)

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
				if err := partition.Reorder(ctx, distsqlpb.ConvertToColumnOrdering(windowFn.ordering)); err != nil {
					return err
				}
				partition.Sort(ctx)
			}
			peerGrouper = &partitionPeerGrouper{
				ctx:       ctx,
				evalCtx:   evalCtx,
				partition: partition,
				ordering:  windowFn.ordering,
				rowCopy:   make(sqlbase.EncDatumRow, len(w.inputTypes)),
			}
		} else {
			// If ORDER BY clause is not provided, all rows are peers.
			peerGrouper = allPeers{}
		}

		frameRun.Rows = partition
		frameRun.RowIdx = 0

		if !frameRun.IsDefaultFrame() {
			// We have a custom frame not equivalent to default one, so if we have
			// an aggregate function, we want to reset it for each row.
			// Not resetting is an optimization since we're not computing
			// the result over the whole frame but only as a result of the current
			// row and previous results of aggregation.
			builtins.ShouldReset(builtin)
		}

		if err := frameRun.PeerHelper.Init(frameRun, peerGrouper); err != nil {
			return err
		}
		frameRun.CurRowPeerGroupNum = 0

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
				w.windowValues[partitionIdx][windowFnIdx][row.GetIdx()] = res
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
	ordering := distsqlpb.ConvertToColumnOrdering(w.windowFns[w.orderOfWindowFnsProcessing[0]].ordering)
	w.partition = rowcontainer.MakeDiskBackedIndexedRowContainer(
		ordering,
		w.inputTypes,
		w.evalCtx,
		w.flowCtx.TempStorage,
		w.MemMonitor,
		w.diskMonitor,
		0, /* rowCapacity */
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
					return pgerror.NewAssertionErrorf(
						"hash column %d, row with only %d columns", log.Safe(col), log.Safe(len(row)))
				}
				var err error
				w.scratch, err = row[int(col)].Encode(&w.inputTypes[int(col)], &w.datumAlloc, preferredEncoding, w.scratch)
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

// populateNextOutputRow combines results of computing window functions with
// non-argument columns of the input row to produce an output row.
// The output of windowFn is put in column windowFn.argIdxStart of w.outputRow.
// All columns that were arguments to window functions are omitted, and columns
// that were not such are passed through.
func (w *windower) populateNextOutputRow() (bool, error) {
	if w.partitionIdx < len(w.partitionSizes) {
		if w.allRowsIterator == nil {
			w.allRowsIterator = w.allRowsPartitioned.NewUnmarkedIterator(w.Ctx)
			w.allRowsIterator.Rewind()
		}
		// We reuse the same EncDatumRow since caller of Next() should've copied it.
		w.outputRow = w.outputRow[:0]
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
		inputColIdx := 0
		for windowFnIdx, windowFn := range w.windowFns {
			// We simply pass through columns in [inputColIdx, windowFn.argIdxStart).
			w.outputRow = append(w.outputRow, inputRow[inputColIdx:windowFn.argIdxStart]...)
			windowFnRes := w.windowValues[w.partitionIdx][windowFnIdx][rowIdx]
			encWindowFnRes := sqlbase.DatumToEncDatum(&w.outputTypes[len(w.outputRow)], windowFnRes)
			w.outputRow = append(w.outputRow, encWindowFnRes)
			// We skip all columns that were arguments to windowFn.
			inputColIdx = windowFn.argIdxStart + windowFn.argCount
		}
		// We simply pass through all columns after all arguments to window
		// functions.
		w.outputRow = append(w.outputRow, inputRow[inputColIdx:]...)
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
	create       func(*tree.EvalContext) tree.WindowFunc
	ordering     distsqlpb.Ordering
	argIdxStart  int
	argCount     int
	frame        *distsqlpb.WindowerSpec_Frame
	filterColIdx int
}

type partitionPeerGrouper struct {
	ctx       context.Context
	evalCtx   *tree.EvalContext
	partition *rowcontainer.DiskBackedIndexedRowContainer
	ordering  distsqlpb.Ordering
	rowCopy   sqlbase.EncDatumRow
	err       error
}

func (n *partitionPeerGrouper) InSameGroup(i, j int) (bool, error) {
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
			if o.Direction != distsqlpb.Ordering_Column_ASC {
				return false, nil
			}
			return false, nil
		}
	}
	return true, nil
}

type allPeers struct{}

// allPeers implements the PeerGroupChecker interface.
func (allPeers) InSameGroup(i, j int) (bool, error) { return true, nil }

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
func CreateWindowerSpecFunc(funcStr string) (distsqlpb.WindowerSpec_Func, error) {
	if aggBuiltin, ok := distsqlpb.AggregatorSpec_Func_value[funcStr]; ok {
		aggSpec := distsqlpb.AggregatorSpec_Func(aggBuiltin)
		return distsqlpb.WindowerSpec_Func{AggregateFunc: &aggSpec}, nil
	} else if winBuiltin, ok := distsqlpb.WindowerSpec_WindowFunc_value[funcStr]; ok {
		winSpec := distsqlpb.WindowerSpec_WindowFunc(winBuiltin)
		return distsqlpb.WindowerSpec_Func{WindowFunc: &winSpec}, nil
	} else {
		return distsqlpb.WindowerSpec_Func{}, errors.Errorf("unknown aggregate/window function %s", funcStr)
	}
}

var _ distsqlpb.DistSQLSpanStats = &WindowerStats{}

const windowerTagPrefix = "windower."

// Stats implements the SpanStats interface.
func (ws *WindowerStats) Stats() map[string]string {
	inputStatsMap := ws.InputStats.Stats(windowerTagPrefix)
	inputStatsMap[windowerTagPrefix+maxMemoryTagSuffix] = humanizeutil.IBytes(ws.MaxAllocatedMem)
	inputStatsMap[windowerTagPrefix+maxDiskTagSuffix] = humanizeutil.IBytes(ws.MaxAllocatedDisk)
	return inputStatsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (ws *WindowerStats) StatsForQueryPlan() []string {
	return append(
		ws.InputStats.StatsForQueryPlan("" /* prefix */),
		fmt.Sprintf("%s: %s", maxMemoryQueryPlanSuffix, humanizeutil.IBytes(ws.MaxAllocatedMem)),
		fmt.Sprintf("%s: %s", maxDiskQueryPlanSuffix, humanizeutil.IBytes(ws.MaxAllocatedDisk)),
	)
}
func (w *windower) outputStatsToTrace() {
	is, ok := getInputStats(w.flowCtx, w.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(w.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp,
			&WindowerStats{
				InputStats:       is,
				MaxAllocatedMem:  w.MemMonitor.MaximumBytes(),
				MaxAllocatedDisk: w.diskMonitor.MaximumBytes(),
			},
		)
	}
}
