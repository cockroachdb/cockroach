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
	"sort"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// GetWindowFunctionInfo returns windowFunc constructor and the return type
// when given fn is applied to given inputTypes.
func GetWindowFunctionInfo(
	fn WindowerSpec_Func, inputTypes ...sqlbase.ColumnType,
) (
	windowConstructor func(*tree.EvalContext) tree.WindowFunc,
	returnType sqlbase.ColumnType,
	err error,
) {
	if fn.AggregateFunc != nil && *fn.AggregateFunc == AggregatorSpec_ANY_NOT_NULL {
		// The ANY_NOT_NULL builtin does not have a fixed return type;
		// handle it separately.
		if len(inputTypes) != 1 {
			return nil, sqlbase.ColumnType{}, errors.Errorf("any_not_null aggregate needs 1 input")
		}
		return builtins.NewAggregateWindowFunc(builtins.NewAnyNotNullAggregate), inputTypes[0], nil
	}
	datumTypes := make([]types.T, len(inputTypes))
	for i := range inputTypes {
		datumTypes[i] = inputTypes[i].ToDatumType()
	}

	var funcStr string
	if fn.AggregateFunc != nil {
		funcStr = fn.AggregateFunc.String()
	} else if fn.WindowFunc != nil {
		funcStr = fn.WindowFunc.String()
	} else {
		return nil, sqlbase.ColumnType{}, errors.Errorf(
			"function is neither an aggregate nor a window function",
		)
	}
	_, builtins := builtins.GetBuiltinProperties(strings.ToLower(funcStr))
	for _, b := range builtins {
		types := b.Types.Types()
		if len(types) != len(inputTypes) {
			continue
		}
		match := true
		for i, t := range types {
			if !datumTypes[i].Equivalent(t) {
				match = false
				break
			}
		}
		if match {
			// Found!
			constructAgg := func(evalCtx *tree.EvalContext) tree.WindowFunc {
				return b.WindowFunc(datumTypes, evalCtx)
			}

			colTyp, err := sqlbase.DatumTypeToColumnType(b.FixedReturnType())
			if err != nil {
				return nil, sqlbase.ColumnType{}, err
			}
			return constructAgg, colTyp, nil
		}
	}
	return nil, sqlbase.ColumnType{}, errors.Errorf(
		"no builtin aggregate/window function for %s on %v", funcStr, inputTypes,
	)
}

// windowerState represents the state of the processor.
type windowerState int

const (
	windowerStateUnknown windowerState = iota
	// windowerAccumulating means that rows are being read from the input
	// and accumulated in encodedPartitions.
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
	inputTypes   []sqlbase.ColumnType
	outputTypes  []sqlbase.ColumnType
	datumAlloc   sqlbase.DatumAlloc
	rowAlloc     sqlbase.EncDatumRowAlloc

	// We choose to not track certain slices (like outputTypes, windowFns and
	// a couple of slices within computeWindowFunctions) since they are likely
	// to have very low (although varible) memory usage.
	accumulationAcc mon.BoundAccount
	decodingAcc     mon.BoundAccount
	resultsAcc      mon.BoundAccount
	partitionsAcc   mon.BoundAccount

	scratch       []byte
	cancelChecker *sqlbase.CancelChecker

	partitionBy       []uint32
	encodedPartitions map[string][]sqlbase.EncDatumRow
	windowFns         []*windowFunc

	populated            bool
	buckets              []string
	bucketToPartitionIdx []int
	bucketIter           int
	rowsInBucketEmitted  int
	windowValues         [][][]tree.Datum
	outputRow            sqlbase.EncDatumRow
}

var _ Processor = &windower{}
var _ RowSource = &windower{}

const windowerProcName = "windower"

func newWindower(
	flowCtx *FlowCtx,
	processorID int32,
	spec *WindowerSpec,
	input RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (*windower, error) {
	w := &windower{
		input: input,
	}
	w.inputTypes = input.OutputTypes()
	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := NewMonitor(ctx, flowCtx.EvalCtx.Mon, "windower-mem")
	w.accumulationAcc = memMonitor.MakeBoundAccount()
	w.decodingAcc = memMonitor.MakeBoundAccount()
	w.resultsAcc = memMonitor.MakeBoundAccount()
	w.partitionsAcc = memMonitor.MakeBoundAccount()
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		w.input = NewInputStatCollector(w.input)
		w.finishTrace = w.outputStatsToTrace
	}

	windowFns := spec.WindowFns
	w.encodedPartitions = make(map[string][]sqlbase.EncDatumRow)
	w.partitionBy = spec.PartitionBy
	w.windowFns = make([]*windowFunc, 0, len(windowFns))
	w.outputTypes = make([]sqlbase.ColumnType, 0, len(w.inputTypes))

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

	if err := w.Init(
		w,
		post,
		w.outputTypes,
		flowCtx,
		processorID,
		output,
		memMonitor,
		ProcStateOpts{InputsToDrain: []RowSource{w.input},
			TrailingMetaCallback: func() []ProducerMetadata {
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

// ConsumerDone is part of the RowSource interface.
func (w *windower) ConsumerDone() {
	w.MoveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (w *windower) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	w.close()
}

func (w *windower) close() {
	// Need to close the mem accounting while the context is still valid.
	w.accumulationAcc.Close(w.Ctx)
	w.decodingAcc.Close(w.Ctx)
	w.resultsAcc.Close(w.Ctx)
	w.partitionsAcc.Close(w.Ctx)
	w.InternalClose()
	w.MemMonitor.Stop(w.Ctx)
}

// accumulateRows continually reads rows from the input and accumulates them
// in encodedPartitions. If it encounters metadata, the metadata is immediately
// returned. Subsequent calls of this function will resume row accumulation.
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
			break
		}

		if err := w.accumulationAcc.Grow(w.Ctx, int64(row.Size())); err != nil {
			w.MoveToDraining(err)
			return windowerStateUnknown, nil, nil
		}
		if len(w.partitionBy) == 0 {
			w.encodedPartitions[""] = append(w.encodedPartitions[""], w.rowAlloc.CopyRow(row))
		} else {
			// We need to hash the row according to partitionBy
			// to figure out which partition the row belongs to.
			w.scratch = w.scratch[:0]
			for _, col := range w.partitionBy {
				if int(col) >= len(row) {
					panic(fmt.Sprintf("hash column %d, row with only %d columns", col, len(row)))
				}
				var err error
				w.scratch, err = row[int(col)].Encode(&w.inputTypes[int(col)], &w.datumAlloc, preferredEncoding, w.scratch)
				if err != nil {
					return windowerStateUnknown, nil, &ProducerMetadata{Err: err}
				}
			}
			w.encodedPartitions[string(w.scratch)] = append(w.encodedPartitions[string(w.scratch)], w.rowAlloc.CopyRow(row))
		}
	}

	return windowerEmittingRows, nil, nil
}

// decodePartitions ensures that all EncDatums of each row in each encoded
// partition are decoded. It should be called after accumulation of rows is
// complete.
func (w *windower) decodePartitions() error {
	for _, encodedPartition := range w.encodedPartitions {
		for _, encRow := range encodedPartition {
			for i := range encRow {
				if err := encRow[i].EnsureDecoded(&w.inputTypes[i], &w.datumAlloc); err != nil {
					return err
				}
				if err := w.decodingAcc.Grow(w.Ctx, int64(encRow[i].Datum.Size())); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// emitRow emits the next row if output rows have already been populated;
// if they haven't, it first decodes all rows, computes all window functions
// over all partitions (i.e. populates outputRows), and then emits the first row.
//
// emitRow() might move to stateDraining. It might also not return a row if the
// ProcOutputHelper filtered the current row out.
func (w *windower) emitRow() (windowerState, sqlbase.EncDatumRow, *ProducerMetadata) {
	if w.inputDone {
		for !w.populated {
			if err := w.cancelChecker.Check(); err != nil {
				w.MoveToDraining(err)
				return windowerStateUnknown, nil, nil
			}

			if err := w.decodePartitions(); err != nil {
				w.MoveToDraining(err)
				return windowerStateUnknown, nil, nil
			}

			if err := w.computeWindowFunctions(w.Ctx, w.evalCtx); err != nil {
				w.MoveToDraining(err)
				return windowerStateUnknown, nil, nil
			}
			w.populated = true
		}

		if w.populateNextOutputRow() {
			return windowerEmittingRows, w.processRowHelper(w.outputRow), nil
		}

		w.MoveToDraining(nil /* err */)
		return windowerStateUnknown, nil, nil
	}

	w.MoveToDraining(errors.Errorf("unexpected: emitRow() is called on a windower before all input rows are accumulated"))
	return windowerStateUnknown, nil, nil
}

// computeWindowFunctions computes all window functions over all partitions.
// The output of windowFn is put in column windowFn.argIdxStart of outputRows.
// Each window function is "responsible" for columns in the interval
// [prevWindowFn.argIdxStart + 1, curWindowFn.argIdxStart] meaning
// that non-argument columns are first copied and then the result is appended.
// After all window functions have been computed, the remaining columns
// are also simply appended to corresponding rows in outputRows.
func (w *windower) computeWindowFunctions(ctx context.Context, evalCtx *tree.EvalContext) error {
	var peerGrouper tree.PeerGroupChecker
	usage := sliceOfRowsSliceOverhead + sizeOfSliceOfRows*int64(len(w.windowFns))
	if err := w.resultsAcc.Grow(w.Ctx, usage); err != nil {
		return err
	}
	w.windowValues = make([][][]tree.Datum, len(w.windowFns))

	usage = indexedRowsStructSliceOverhead + sizeOfIndexedRowsStruct*int64(len(w.encodedPartitions))
	if err := w.partitionsAcc.Grow(w.Ctx, usage); err != nil {
		return err
	}
	partitions := make([]indexedRows, len(w.encodedPartitions))

	w.buckets = make([]string, 0, len(w.encodedPartitions))
	w.bucketToPartitionIdx = make([]int, 0, len(w.encodedPartitions))
	partitionIdx := 0
	for bucket, encodedPartition := range w.encodedPartitions {
		// We want to fix some order of iteration over encoded partitions
		// to be consistent.
		w.buckets = append(w.buckets, bucket)
		w.bucketToPartitionIdx = append(w.bucketToPartitionIdx, partitionIdx)
		usage = indexedRowStructSliceOverhead + sizeOfIndexedRowStruct*int64(len(encodedPartition))
		if err := w.partitionsAcc.Grow(w.Ctx, usage); err != nil {
			return err
		}
		rows := make([]indexedRow, 0, len(encodedPartition))
		for idx := 0; idx < len(encodedPartition); idx++ {
			rows = append(rows, indexedRow{idx: idx, row: encodedPartition[idx]})
		}
		partitions[partitionIdx] = indexedRows{rows: rows}
		partitionIdx++
	}

	// partitionPreviouslySortedFuncIdx maps index of a window function f1
	// to the index of another window function f2 with the same ORDER BY
	// clause such that f2 will have been processed before f1.
	usage = sliceOfIndexedRowsSliceOverhead + sizeOfSliceOfIndexedRows*int64(len(w.windowFns))
	if err := w.partitionsAcc.Grow(w.Ctx, usage); err != nil {
		return err
	}
	sortedPartitionsCache := make([][]indexedRows, len(w.windowFns))
	partitionPreviouslySortedFuncIdx := make([]int, len(w.windowFns))
	for i := 0; i < len(w.windowFns); i++ {
		partitionPreviouslySortedFuncIdx[i] = -1
	}
	shouldCacheSortedPartitions := make([]bool, len(w.windowFns))
	for windowFnIdx, windowFn := range w.windowFns {
		for laterFnIdx := windowFnIdx + 1; laterFnIdx < len(w.windowFns); laterFnIdx++ {
			if partitionPreviouslySortedFuncIdx[laterFnIdx] != -1 {
				// We've already found equal ordering for laterFn
				// among previous window functions.
				continue
			}
			if windowFn.ordering.Equal(w.windowFns[laterFnIdx].ordering) {
				partitionPreviouslySortedFuncIdx[laterFnIdx] = windowFnIdx
				shouldCacheSortedPartitions[windowFnIdx] = true
			}
		}
	}

	for windowFnIdx, windowFn := range w.windowFns {
		usage = rowSliceOverhead + sizeOfRow*int64(len(w.encodedPartitions))
		if err := w.resultsAcc.Grow(w.Ctx, usage); err != nil {
			return err
		}
		w.windowValues[windowFnIdx] = make([][]tree.Datum, len(w.encodedPartitions))

		frameRun := &tree.WindowFrameRun{
			ArgCount:     windowFn.argCount,
			ArgIdxStart:  windowFn.argIdxStart,
			FilterColIdx: windowFn.filterColIdx,
		}

		if windowFn.frame != nil {
			frameRun.Frame = windowFn.frame.convertToAST()
			startBound, endBound := windowFn.frame.Bounds.Start, windowFn.frame.Bounds.End
			if startBound.BoundType == WindowerSpec_Frame_OFFSET_PRECEDING ||
				startBound.BoundType == WindowerSpec_Frame_OFFSET_FOLLOWING {
				switch windowFn.frame.Mode {
				case WindowerSpec_Frame_ROWS:
					frameRun.StartBoundOffset = tree.NewDInt(tree.DInt(int(startBound.IntOffset)))
				case WindowerSpec_Frame_RANGE:
					datum, rem, err := sqlbase.DecodeTableValue(&w.datumAlloc, startBound.OffsetType.Type.ToDatumType(), startBound.TypedOffset)
					if err != nil {
						return errors.Wrapf(err, "error decoding %d bytes", len(startBound.TypedOffset))
					}
					if len(rem) != 0 {
						return errors.Errorf("%d trailing bytes in encoded value", len(rem))
					}
					frameRun.StartBoundOffset = datum
				case WindowerSpec_Frame_GROUPS:
					frameRun.StartBoundOffset = tree.NewDInt(tree.DInt(int(startBound.IntOffset)))
				default:
					panic("unexpected WindowFrameMode")
				}
			}
			if endBound != nil {
				if endBound.BoundType == WindowerSpec_Frame_OFFSET_PRECEDING ||
					endBound.BoundType == WindowerSpec_Frame_OFFSET_FOLLOWING {
					switch windowFn.frame.Mode {
					case WindowerSpec_Frame_ROWS:
						frameRun.EndBoundOffset = tree.NewDInt(tree.DInt(int(endBound.IntOffset)))
					case WindowerSpec_Frame_RANGE:
						datum, rem, err := sqlbase.DecodeTableValue(&w.datumAlloc, endBound.OffsetType.Type.ToDatumType(), endBound.TypedOffset)
						if err != nil {
							return errors.Wrapf(err, "error decoding %d bytes", len(endBound.TypedOffset))
						}
						if len(rem) != 0 {
							return errors.Errorf("%d trailing bytes in encoded value", len(rem))
						}
						frameRun.EndBoundOffset = datum
					case WindowerSpec_Frame_GROUPS:
						frameRun.EndBoundOffset = tree.NewDInt(tree.DInt(int(endBound.IntOffset)))
					default:
						panic("unexpected WindowFrameMode")
					}
				}
			}
			if frameRun.RangeModeWithOffsets() {
				ordCol := windowFn.ordering.Columns[0]
				frameRun.OrdColIdx = int(ordCol.ColIdx)
				// We need this +1 because encoding.Direction has extra value "_"
				// as zeroth "entry" which its proto equivalent doesn't have.
				frameRun.OrdDirection = encoding.Direction(ordCol.Direction + 1)

				colTyp := w.inputTypes[ordCol.ColIdx].ToDatumType()
				// Type of offset depends on the ordering column's type.
				offsetTyp := colTyp
				if types.IsDateTimeType(colTyp) {
					// For datetime related ordering columns, offset must be an Interval.
					offsetTyp = types.Interval
				}
				plusOp, minusOp, found := tree.WindowFrameRangeOps{}.LookupImpl(colTyp, offsetTyp)
				if !found {
					return pgerror.NewErrorf(pgerror.CodeWindowingError, "given logical offset cannot be combined with ordering column")
				}
				frameRun.PlusOp, frameRun.MinusOp = plusOp, minusOp
			}
		}

		for partitionIdx := 0; partitionIdx < len(partitions); partitionIdx++ {
			builtin := windowFn.create(evalCtx)
			defer builtin.Close(ctx, evalCtx)

			partition := partitions[partitionIdx]
			usage = datumSliceOverhead + sizeOfDatum*int64(partition.Len())
			if err := w.resultsAcc.Grow(w.Ctx, usage); err != nil {
				return err
			}
			w.windowValues[windowFnIdx][partitionIdx] = make([]tree.Datum, partition.Len())

			if len(windowFn.ordering.Columns) > 0 {
				// If an ORDER BY clause is provided, order the partition and use the
				// sorter as our peerGroupChecker.
				if funcIdx := partitionPreviouslySortedFuncIdx[windowFnIdx]; funcIdx != -1 {
					// We have cached sorted partitions - no need to resort them.
					partition = sortedPartitionsCache[funcIdx][partitionIdx]
					peerGrouper = &partitionSorter{
						evalCtx:  evalCtx,
						rows:     partition,
						ordering: windowFn.ordering,
					}
				} else {
					sorter := &partitionSorter{
						evalCtx:  evalCtx,
						rows:     partition,
						ordering: windowFn.ordering,
					}
					sort.Sort(sorter)
					peerGrouper = sorter
					if shouldCacheSortedPartitions[windowFnIdx] {
						// Later window functions will need rows in the same order,
						// so we cache copies of all sorted partitions.
						if sortedPartitionsCache[windowFnIdx] == nil {
							usage = indexedRowsStructSliceOverhead + sizeOfIndexedRowsStruct*int64(len(partitions))
							if err := w.partitionsAcc.Grow(w.Ctx, usage); err != nil {
								return err
							}
							sortedPartitionsCache[windowFnIdx] = make([]indexedRows, len(partitions))
						}
						// TODO(yuzefovich): figure out how to avoid making this deep copy.
						usage = indexedRowStructSliceOverhead + sizeOfIndexedRowStruct*int64(partition.Len())
						if err := w.partitionsAcc.Grow(w.Ctx, usage); err != nil {
							return err
						}
						sortedPartitionsCache[windowFnIdx][partitionIdx] = partition.makeCopy()
					}
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

			frameRun.PeerHelper.Init(frameRun, peerGrouper)
			frameRun.CurRowPeerGroupNum = 0

			for frameRun.RowIdx < partition.Len() {
				// Perform calculations on each row in the current peer group.
				peerGroupEndIdx := frameRun.PeerHelper.GetFirstPeerIdx(frameRun.CurRowPeerGroupNum) + frameRun.PeerHelper.GetRowCount(frameRun.CurRowPeerGroupNum)
				for ; frameRun.RowIdx < peerGroupEndIdx; frameRun.RowIdx++ {
					res, err := builtin.Compute(ctx, evalCtx, frameRun)
					if err != nil {
						return err
					}
					w.windowValues[windowFnIdx][partitionIdx][frameRun.Rows.GetRow(frameRun.RowIdx).GetIdx()] = res
				}
				frameRun.PeerHelper.Update(frameRun)
				frameRun.CurRowPeerGroupNum++
			}
		}
	}

	return nil
}

// populateNextOutputRow combines results of computing window functions with
// non-argument columns of the input row to produce an output row.
func (w *windower) populateNextOutputRow() bool {
	if w.bucketIter < len(w.encodedPartitions) {
		// We reuse the same EncDatumRow since caller of Next() should've copied it.
		w.outputRow = w.outputRow[:0]
		// rowIdx is the index of the next row to be emitted from partition with
		// hash w.buckets[w.bucketIter].
		rowIdx := w.rowsInBucketEmitted
		inputRow := w.encodedPartitions[w.buckets[w.bucketIter]][rowIdx]
		partitionIdx := w.bucketToPartitionIdx[w.bucketIter]
		inputColIdx := 0
		for windowFnIdx, windowFn := range w.windowFns {
			// We simply pass through columns in [inputColIdx, windowFn.argIdxStart).
			w.outputRow = append(w.outputRow, inputRow[inputColIdx:windowFn.argIdxStart]...)
			windowFnRes := w.windowValues[windowFnIdx][partitionIdx][rowIdx]
			encWindowFnRes := sqlbase.DatumToEncDatum(w.outputTypes[len(w.outputRow)], windowFnRes)
			w.outputRow = append(w.outputRow, encWindowFnRes)
			// We skip all columns that were arguments to windowFn.
			inputColIdx = windowFn.argIdxStart + windowFn.argCount
		}
		// We simply pass through all columns after all arguments to window functions.
		w.outputRow = append(w.outputRow, inputRow[inputColIdx:]...)
		w.rowsInBucketEmitted++
		if w.rowsInBucketEmitted == len(w.encodedPartitions[w.buckets[w.bucketIter]]) {
			// We have emitted all rows from the current bucket, so we advance the
			// iterator.
			w.bucketIter++
			w.rowsInBucketEmitted = 0
		}
		return true

	}
	return false
}

type windowFunc struct {
	create       func(*tree.EvalContext) tree.WindowFunc
	ordering     Ordering
	argIdxStart  int
	argCount     int
	frame        *WindowerSpec_Frame
	filterColIdx int
}

type partitionSorter struct {
	evalCtx  *tree.EvalContext
	rows     indexedRows
	ordering Ordering
}

// partitionSorter implements the sort.Interface interface.
func (n *partitionSorter) Len() int { return n.rows.Len() }
func (n *partitionSorter) Swap(i, j int) {
	n.rows.rows[i], n.rows.rows[j] = n.rows.rows[j], n.rows.rows[i]
}
func (n *partitionSorter) Less(i, j int) bool { return n.Compare(i, j) < 0 }

// partitionSorter implements the tree.PeerGroupChecker interface.
func (n *partitionSorter) InSameGroup(i, j int) bool { return n.Compare(i, j) == 0 }

func (n *partitionSorter) Compare(i, j int) int {
	ra, rb := n.rows.rows[i], n.rows.rows[j]
	for _, o := range n.ordering.Columns {
		da := ra.GetDatum(int(o.ColIdx))
		db := rb.GetDatum(int(o.ColIdx))
		if c := da.Compare(n.evalCtx, db); c != 0 {
			if o.Direction != Ordering_Column_ASC {
				return -c
			}
			return c
		}
	}
	return 0
}

type allPeers struct{}

// allPeers implements the peerGroupChecker interface.
func (allPeers) InSameGroup(i, j int) bool { return true }

const sizeOfIndexedRowsStruct = int64(unsafe.Sizeof(indexedRows{}))
const indexedRowsStructSliceOverhead = int64(unsafe.Sizeof([]indexedRows{}))
const sizeOfSliceOfIndexedRows = int64(unsafe.Sizeof([]indexedRows{}))
const sliceOfIndexedRowsSliceOverhead = int64(unsafe.Sizeof([][]indexedRows{}))
const sizeOfIndexedRowStruct = int64(unsafe.Sizeof(indexedRow{}))
const indexedRowStructSliceOverhead = int64(unsafe.Sizeof([]indexedRow{}))
const sizeOfSliceOfRows = int64(unsafe.Sizeof([][]tree.Datum{}))
const sliceOfRowsSliceOverhead = int64(unsafe.Sizeof([][][]tree.Datum{}))
const sizeOfRow = int64(unsafe.Sizeof([]tree.Datum{}))
const rowSliceOverhead = int64(unsafe.Sizeof([][]tree.Datum{}))
const sizeOfDatum = int64(unsafe.Sizeof(tree.Datum(nil)))
const datumSliceOverhead = int64(unsafe.Sizeof([]tree.Datum(nil)))

// indexedRows are rows with the corresponding indices.
type indexedRows struct {
	rows []indexedRow
}

// Len implements tree.IndexedRows interface.
func (ir indexedRows) Len() int {
	return len(ir.rows)
}

// GetRow implements tree.IndexedRows interface.
func (ir indexedRows) GetRow(idx int) tree.IndexedRow {
	return ir.rows[idx]
}

func (ir indexedRows) makeCopy() indexedRows {
	ret := indexedRows{rows: make([]indexedRow, ir.Len())}
	copy(ret.rows, ir.rows)
	return ret
}

// indexedRow is a row with a corresponding index.
type indexedRow struct {
	idx int
	row sqlbase.EncDatumRow
}

// GetIdx implements tree.IndexedRow interface.
func (ir indexedRow) GetIdx() int {
	return ir.idx
}

// GetDatum implements tree.IndexedRow interface.
func (ir indexedRow) GetDatum(colIdx int) tree.Datum {
	return ir.row[colIdx].Datum
}

// GetDatums implements tree.IndexedRow interface.
func (ir indexedRow) GetDatums(startColIdx, endColIdx int) tree.Datums {
	datums := make(tree.Datums, 0, endColIdx-startColIdx)
	for idx := startColIdx; idx < endColIdx; idx++ {
		datums = append(datums, ir.row[idx].Datum)
	}
	return datums
}

var _ DistSQLSpanStats = &WindowerStats{}

const windowerTagPrefix = "windower."

// Stats implements the SpanStats interface.
func (ws *WindowerStats) Stats() map[string]string {
	inputStatsMap := ws.InputStats.Stats(windowerTagPrefix)
	inputStatsMap[windowerTagPrefix+maxMemoryTagSuffix] = humanizeutil.IBytes(ws.MaxAllocatedMem)
	return inputStatsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (ws *WindowerStats) StatsForQueryPlan() []string {
	return append(
		ws.InputStats.StatsForQueryPlan("" /* prefix */),
		fmt.Sprintf("%s: %s", maxMemoryQueryPlanSuffix, humanizeutil.IBytes(ws.MaxAllocatedMem)),
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
				InputStats:      is,
				MaxAllocatedMem: w.MemMonitor.MaximumBytes(),
			},
		)
	}
}
