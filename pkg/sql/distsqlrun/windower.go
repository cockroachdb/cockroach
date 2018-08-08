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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	processorBase

	// runningState represents the state of the windower. This is in addition to
	// processorBase.state - the runningState is only relevant when
	// processorBase.state == stateRunning.
	runningState windowerState
	input        RowSource
	inputDone    bool
	inputTypes   []sqlbase.ColumnType
	outputTypes  []sqlbase.ColumnType
	datumAlloc   sqlbase.DatumAlloc
	rowAlloc     sqlbase.EncDatumRowAlloc

	scratch []byte

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

	if err := w.init(
		w,
		post,
		w.outputTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		procStateOpts{inputsToDrain: []RowSource{w.input}},
	); err != nil {
		return nil, err
	}

	return w, nil
}

// Start is part of the RowSource interface.
func (w *windower) Start(ctx context.Context) context.Context {
	w.input.Start(ctx)
	ctx = w.startInternal(ctx, windowerProcName)
	w.cancelChecker = sqlbase.NewCancelChecker(ctx)
	w.runningState = windowerAccumulating
	return ctx
}

// Next is part of the RowSource interface.
func (w *windower) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for w.state == stateRunning {
		var row sqlbase.EncDatumRow
		var meta *ProducerMetadata
		switch w.runningState {
		case windowerAccumulating:
			w.runningState, row, meta = w.accumulateRows()
		case windowerEmittingRows:
			w.runningState, row, meta = w.emitRow()
		default:
			log.Fatalf(w.ctx, "unsupported state: %d", w.runningState)
		}

		if row == nil && meta == nil {
			continue
		}
		return row, meta
	}
	return nil, w.drainHelper()
}

// ConsumerDone is part of the RowSource interface.
func (w *windower) ConsumerDone() {
	w.moveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (w *windower) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	w.internalClose()
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
				w.moveToDraining(nil /* err */)
				return windowerStateUnknown, nil, meta
			}
			return windowerAccumulating, nil, meta
		}
		if row == nil {
			log.VEvent(w.ctx, 1, "accumulation complete")
			w.inputDone = true
			break
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
				w.moveToDraining(err)
				return windowerStateUnknown, nil, nil
			}

			if err := w.decodePartitions(); err != nil {
				w.moveToDraining(err)
				return windowerStateUnknown, nil, nil
			}

			if err := w.computeWindowFunctions(w.ctx, w.evalCtx); err != nil {
				w.moveToDraining(err)
				return windowerStateUnknown, nil, nil
			}
			w.populated = true
		}

		if w.populateNextOutputRow() {
			return windowerEmittingRows, w.processRowHelper(w.outputRow), nil
		}

		w.moveToDraining(nil /* err */)
		return windowerStateUnknown, nil, nil
	}

	w.moveToDraining(errors.Errorf("unexpected: emitRow() is called on a windower before all input rows are accumulated"))
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
	var peerGrouper peerGroupChecker
	w.windowValues = make([][][]tree.Datum, len(w.windowFns))
	partitions := make([]indexedRows, len(w.encodedPartitions))

	w.buckets = make([]string, 0, len(w.encodedPartitions))
	w.bucketToPartitionIdx = make([]int, 0, len(w.encodedPartitions))
	partitionIdx := 0
	for bucket, encodedPartition := range w.encodedPartitions {
		// We want to fix some order of iteration over encoded partitions
		// to be consistent.
		w.buckets = append(w.buckets, bucket)
		w.bucketToPartitionIdx = append(w.bucketToPartitionIdx, partitionIdx)
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
	partitionPreviouslySortedFuncIdx := make([]int, len(w.windowFns))
	for i := 0; i < len(w.windowFns); i++ {
		partitionPreviouslySortedFuncIdx[i] = -1
	}
	sortedPartitionsCache := make([][]indexedRows, len(w.windowFns))
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
				frameRun.StartBoundOffset = tree.NewDInt(tree.DInt(int(startBound.IntOffset)))
			}
			if endBound != nil {
				if endBound.BoundType == WindowerSpec_Frame_OFFSET_PRECEDING ||
					endBound.BoundType == WindowerSpec_Frame_OFFSET_FOLLOWING {
					frameRun.EndBoundOffset = tree.NewDInt(tree.DInt(int(endBound.IntOffset)))
				}
			}
		}

		for partitionIdx := 0; partitionIdx < len(partitions); partitionIdx++ {
			builtin := windowFn.create(evalCtx)
			defer builtin.Close(ctx, evalCtx)

			partition := partitions[partitionIdx]
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
							sortedPartitionsCache[windowFnIdx] = make([]indexedRows, len(partitions))
						}
						// TODO(yuzefovich): we should figure out how to avoid making this
						// deep copy.
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

			for frameRun.RowIdx < partition.Len() {
				// Compute the size of the current peer group.
				frameRun.FirstPeerIdx = frameRun.RowIdx
				frameRun.PeerRowCount = 1
				for ; frameRun.FirstPeerIdx+frameRun.PeerRowCount < frameRun.PartitionSize(); frameRun.PeerRowCount++ {
					cur := frameRun.FirstPeerIdx + frameRun.PeerRowCount
					if !peerGrouper.InSameGroup(cur, cur-1) {
						break
					}
				}

				// Perform calculations on each row in the current peer group.
				for ; frameRun.RowIdx < frameRun.FirstPeerIdx+frameRun.PeerRowCount; frameRun.RowIdx++ {
					res, err := builtin.Compute(ctx, evalCtx, frameRun)
					if err != nil {
						return err
					}
					w.windowValues[windowFnIdx][partitionIdx][frameRun.Rows.GetRow(frameRun.RowIdx).GetIdx()] = res
				}
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

// partitionSorter implements the peerGroupChecker interface.
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

// peerGroupChecker can check if a pair of row indexes within a partition are
// in the same peer group.
type peerGroupChecker interface {
	InSameGroup(i, j int) bool
}

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
