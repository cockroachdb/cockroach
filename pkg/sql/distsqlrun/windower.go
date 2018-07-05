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
	"hash/crc32"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// GetWindowFunctionInfo return windowFunc constructor and the return type
// when given fn is applied to given inputTypes.
func GetWindowFunctionInfo(
	fn WindowerSpec_Func, inputTypes ...sqlbase.ColumnType,
) (
	windowConstructor func(*tree.EvalContext) tree.WindowFunc,
	returnType sqlbase.ColumnType,
	err error,
) {
	if fn == WindowerSpec_ANY_NOT_NULL {
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

	_, builtins := builtins.GetBuiltinProperties(strings.ToLower(fn.String()))
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
		"no builtin aggregate/window function for %s on %v", fn, inputTypes,
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
	encodedPartitions map[int][]sqlbase.EncDatumRow
	partitions        [][]tree.IndexedRow
	windowFns         []*windowFunc

	populated  bool
	outputRows []tree.Datums
	rowIter    int
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
	w.encodedPartitions = make(map[int][]sqlbase.EncDatumRow)
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
			create:      windowConstructor,
			ordering:    windowFn.Ordering,
			argIdxStart: int(windowFn.ArgIdxStart),
			argCount:    int(windowFn.ArgCount),
			frame:       windowFn.Frame,
		}

		w.windowFns = append(w.windowFns, wf)
	}
	// We will simply copy all columns that come after arguments
	// to all window function.
	w.outputTypes = append(w.outputTypes, w.inputTypes[inputColIdx:]...)

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
			w.encodedPartitions[0] = append(w.encodedPartitions[0], w.rowAlloc.CopyRow(row))
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
					panic(fmt.Sprintf("unexpected error while encoding: %v", err))
				}
			}

			bucket := int(crc32.Update(0, crc32Table, w.scratch))
			w.encodedPartitions[bucket] = append(w.encodedPartitions[bucket], w.rowAlloc.CopyRow(row))
		}
	}

	return windowerEmittingRows, nil, nil
}

// decodePartitions loops over all encodedPartitions and decodes each row in
// each encoded partition. It should be called after accumulation of rows is
// complete.
func (w *windower) decodePartitions() {
	w.partitions = make([][]tree.IndexedRow, 0, len(w.encodedPartitions))
	rowIdx := 0
	for _, encodedPartition := range w.encodedPartitions {
		partition := make([]tree.IndexedRow, 0, len(encodedPartition))
		for _, encRow := range encodedPartition {
			// Set up Idx of IndexedRow as row's index among all rows of all partitions.
			partition = append(partition, tree.IndexedRow{Idx: rowIdx, Row: w.decodeRow(encRow)})
			rowIdx++
		}
		w.partitions = append(w.partitions, partition)
	}

	// We no longer need encoded partitions, so we free up the memory.
	w.encodedPartitions = nil
}

// decodeRow decodes given encoded row.
func (w *windower) decodeRow(encRow sqlbase.EncDatumRow) tree.Datums {
	row := make(tree.Datums, 0, len(encRow))
	for col, encDatum := range encRow {
		if err := encDatum.EnsureDecoded(&w.inputTypes[col], &w.datumAlloc); err != nil {
			panic(fmt.Sprintf("unexpected error while ensuring decoded: %v", err))
		}
		row = append(row, encDatum.Datum)
	}
	return row
}

// encodeRow encodes given row.
func (w *windower) encodeRow(row tree.Datums) sqlbase.EncDatumRow {
	encRow := make(sqlbase.EncDatumRow, len(row))
	for idx, datum := range row {
		encRow[idx] = sqlbase.DatumToEncDatum(w.outputTypes[idx], datum)
	}
	return encRow
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

			w.decodePartitions()

			if err := w.computeWindowFunctions(w.ctx, w.evalCtx); err != nil {
				w.moveToDraining(err)
				return windowerStateUnknown, nil, nil
			}
			w.populated = true
		}

		if w.rowIter < len(w.outputRows) {
			res := w.encodeRow(w.outputRows[w.rowIter])
			w.rowIter++
			return windowerEmittingRows, w.processRowHelper(res), nil
		}

		w.moveToDraining(nil /* err */)
		return windowerStateUnknown, nil, nil
	}

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
	numberOfRows := 0
	for _, partition := range w.partitions {
		numberOfRows += len(partition)
	}
	w.outputRows = make([]tree.Datums, 0, numberOfRows)
	for idx := 0; idx < numberOfRows; idx++ {
		w.outputRows = append(w.outputRows, make(tree.Datums, 0, len(w.outputTypes)))
	}

	var peerGrouper peerGroupChecker

	// partitionPreviouslySortedFuncIdx maps index of a window function f1
	// to the index of another window function f2 with the same ORDER BY
	// clause such that f2 will have been processed before f1.
	partitionPreviouslySortedFuncIdx := make([]int, len(w.windowFns))
	for i := 0; i < len(w.windowFns); i++ {
		partitionPreviouslySortedFuncIdx[i] = -1
	}
	sortedPartitionsCache := make([][][]tree.IndexedRow, len(w.windowFns))
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

	// inputColIdx is the index of the column that should be processed next.
	inputColIdx := 0

	for windowFnIdx, windowFn := range w.windowFns {
		frameRun := &tree.WindowFrameRun{
			ArgCount:    windowFn.argCount,
			ArgIdxStart: windowFn.argIdxStart,
		}

		if windowFn.frame != nil {
			frameRun.Frame = convertToWindowFrame(*windowFn.frame)
			frameRun.StartBoundOffset = int(windowFn.frame.Bounds.Start.Offset)
			if windowFn.frame.Bounds.End != nil {
				frameRun.EndBoundOffset = int(windowFn.frame.Bounds.End.Offset)
			}
		}

		for partitionIdx, partition := range w.partitions {
			builtin := windowFn.create(evalCtx)
			defer builtin.Close(ctx, evalCtx)

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
						if partitionIdx == 0 {
							sortedPartitionsCache[windowFnIdx] = make([][]tree.IndexedRow, 0, len(w.partitions))
						}
						sortedPartitionsCache[windowFnIdx] = append(sortedPartitionsCache[windowFnIdx], w.copyPartition(partition))
					}
				}
			} else if frameRun.Frame != nil && frameRun.Frame.Mode == tree.ROWS {
				// If ORDER BY clause is not provided and Frame is specified with ROWS mode,
				// any row has no peers.
				peerGrouper = noPeers{}
			} else {
				// If ORDER BY clause is not provided and either no Frame is provided or Frame is
				// specified with RANGE mode, all rows are peers.
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

			for frameRun.RowIdx < len(partition) {
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
					// All window functions are sorted by their argIdxStart,
					// so we simply "copy" all datums up to windowFn.ArgIdxStart
					// of the current row.
					currentRowIdx := frameRun.Rows[frameRun.RowIdx].Idx
					w.outputRows[currentRowIdx] = append(w.outputRows[currentRowIdx], frameRun.Rows[frameRun.RowIdx].Row[inputColIdx:windowFn.argIdxStart]...)

					// We put the result of window function computation at windowFn.argIdxStart.
					w.outputRows[currentRowIdx] = append(w.outputRows[currentRowIdx], res)
				}
			}
		}
		// We consume all arguments of windowFn.
		inputColIdx = windowFn.argIdxStart + windowFn.argCount
	}

	// We simply copy all columns that come after arguments
	// to all window function.
	for _, partition := range w.partitions {
		for _, row := range partition {
			currentRowIdx := row.Idx
			w.outputRows[currentRowIdx] = append(w.outputRows[currentRowIdx], row.Row[inputColIdx:]...)
		}
	}

	// We no longer need partitions, so we release its underlying memory.
	w.partitions = nil

	return nil
}

// copyPartition returns a copy of the given partition.
func (w *windower) copyPartition(partition []tree.IndexedRow) []tree.IndexedRow {
	ret := make([]tree.IndexedRow, 0, len(partition))
	for _, row := range partition {
		ir := tree.IndexedRow{Idx: row.Idx, Row: w.datumAlloc.NewDatums(len(row.Row))}
		for i, datum := range row.Row {
			ir.Row[i] = datum
		}
		ret = append(ret, ir)
	}
	return ret
}

type windowFunc struct {
	create      func(*tree.EvalContext) tree.WindowFunc
	ordering    Ordering
	argIdxStart int
	argCount    int
	frame       *WindowerSpec_Frame
}

type partitionSorter struct {
	evalCtx  *tree.EvalContext
	rows     []tree.IndexedRow
	ordering Ordering
}

// partitionSorter implements the sort.Interface interface.
func (n *partitionSorter) Len() int { return len(n.rows) }
func (n *partitionSorter) Swap(i, j int) {
	n.rows[i], n.rows[j] = n.rows[j], n.rows[i]
}
func (n *partitionSorter) Less(i, j int) bool { return n.Compare(i, j) < 0 }

// partitionSorter implements the peerGroupChecker interface.
func (n *partitionSorter) InSameGroup(i, j int) bool { return n.Compare(i, j) == 0 }

func (n *partitionSorter) Compare(i, j int) int {
	ra, rb := n.rows[i], n.rows[j]
	for _, o := range n.ordering.Columns {
		da := ra.Row[o.ColIdx]
		db := rb.Row[o.ColIdx]
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

type noPeers struct{}

// noPeers implements the peerGroupChecker interface.
func (noPeers) InSameGroup(i, j int) bool { return false }

// peerGroupChecker can check if a pair of row indexes within a partition are
// in the same peer group.
type peerGroupChecker interface {
	InSameGroup(i, j int) bool
}

// mapToSpecMode maps tree.WindowFrameMode to WindowerSpec_Frame_Mode.
func mapToSpecMode(m tree.WindowFrameMode) WindowerSpec_Frame_Mode {
	switch m {
	case tree.RANGE:
		return WindowerSpec_Frame_RANGE
	case tree.ROWS:
		return WindowerSpec_Frame_ROWS
	default:
		panic("unexpected WindowFrameMode")
	}
}

// mapToSpecBoundType maps tree.WindowFrameBoundType to WindowerSpec_Frame_BoundType.
func mapToSpecBoundType(bt tree.WindowFrameBoundType) WindowerSpec_Frame_BoundType {
	switch bt {
	case tree.UnboundedPreceding:
		return WindowerSpec_Frame_UNBOUNDED_PRECEDING
	case tree.ValuePreceding:
		return WindowerSpec_Frame_VALUE_PRECEDING
	case tree.CurrentRow:
		return WindowerSpec_Frame_CURRENT_ROW
	case tree.ValueFollowing:
		return WindowerSpec_Frame_VALUE_FOLLOWING
	case tree.UnboundedFollowing:
		return WindowerSpec_Frame_UNBOUNDED_FOLLOWING
	default:
		panic("unexpected WindowFrameBoundType")
	}
}

// convertToSpecBounds produces WindowerSpec_Frame_Bounds based on tree.WindowFrameBounds.
// If offset expr's are present, it evaluates them and saves the results in the spec.
func convertToSpecBounds(
	b tree.WindowFrameBounds, evalCtx *tree.EvalContext,
) (WindowerSpec_Frame_Bounds, error) {
	bounds := WindowerSpec_Frame_Bounds{}

	if b.StartBound == nil {
		panic("unexpected: StartBound is nil")
	}
	bounds.Start = WindowerSpec_Frame_Bound{BoundType: mapToSpecBoundType(b.StartBound.BoundType)}
	if b.StartBound.OffsetExpr != nil {
		typedStartOffset := b.StartBound.OffsetExpr.(tree.TypedExpr)
		dStartOffset, err := typedStartOffset.Eval(evalCtx)
		if err != nil {
			return bounds, err
		}
		startOffset := int(tree.MustBeDInt(dStartOffset))
		if startOffset < 0 {
			return bounds, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "frame starting offset must not be negative")
		}
		bounds.Start.Offset = uint32(startOffset)
	}

	if b.EndBound != nil {
		bounds.End = &WindowerSpec_Frame_Bound{BoundType: mapToSpecBoundType(b.EndBound.BoundType)}
		if b.EndBound.OffsetExpr != nil {
			typedEndOffset := b.EndBound.OffsetExpr.(tree.TypedExpr)
			dEndOffset, err := typedEndOffset.Eval(evalCtx)
			if err != nil {
				return bounds, err
			}
			endOffset := int(tree.MustBeDInt(dEndOffset))
			if endOffset < 0 {
				return bounds, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "frame ending offset must not be negative")
			}
			bounds.End.Offset = uint32(endOffset)
		}
	}

	return bounds, nil
}

// ConvertToSpec produces WindowerSpec_Frame based on tree.WindowFrame.
func ConvertToSpec(f tree.WindowFrame, evalCtx *tree.EvalContext) (WindowerSpec_Frame, error) {
	frame := WindowerSpec_Frame{Mode: mapToSpecMode(f.Mode)}
	bounds, err := convertToSpecBounds(f.Bounds, evalCtx)
	frame.Bounds = bounds
	return frame, err
}

// mapToWindowFrameMode maps WindowerSpec_Frame_Mode to tree.WindowFrameMode.
func mapToWindowFrameMode(m WindowerSpec_Frame_Mode) tree.WindowFrameMode {
	switch m {
	case WindowerSpec_Frame_RANGE:
		return tree.RANGE
	case WindowerSpec_Frame_ROWS:
		return tree.ROWS
	default:
		panic("unexpected WindowerSpec_Frame_Mode")
	}
}

// convertToWindowFrameBoundType maps WindowerSpec_Frame_BoundType to tree.WindowFrameBoundType.
func convertToWindowFrameBoundType(bt WindowerSpec_Frame_BoundType) tree.WindowFrameBoundType {
	switch bt {
	case WindowerSpec_Frame_UNBOUNDED_PRECEDING:
		return tree.UnboundedPreceding
	case WindowerSpec_Frame_VALUE_PRECEDING:
		return tree.ValuePreceding
	case WindowerSpec_Frame_CURRENT_ROW:
		return tree.CurrentRow
	case WindowerSpec_Frame_VALUE_FOLLOWING:
		return tree.ValueFollowing
	case WindowerSpec_Frame_UNBOUNDED_FOLLOWING:
		return tree.UnboundedFollowing
	default:
		panic("unexpected WindowerSpec_Frame_BoundType")
	}
}

// convertToWindowFrameBounds produces tree.WindowFrameBounds based on
// WindowerSpec_Frame_Bounds. Note that it might not be fully equivalent
// to original - offsetExpr's are ignored since frameRun will have
// evaluated offsets.
func convertToWindowFrameBounds(b WindowerSpec_Frame_Bounds) tree.WindowFrameBounds {
	bounds := tree.WindowFrameBounds{StartBound: &tree.WindowFrameBound{
		BoundType: convertToWindowFrameBoundType(b.Start.BoundType),
	}}
	if b.End != nil {
		bounds.EndBound = &tree.WindowFrameBound{BoundType: convertToWindowFrameBoundType(b.End.BoundType)}
	}
	return bounds
}

// convertToWindowFrame produces tree.WindowFrame based on WindowerSpec_Frame.
func convertToWindowFrame(f WindowerSpec_Frame) *tree.WindowFrame {
	return &tree.WindowFrame{Mode: mapToWindowFrameMode(f.Mode), Bounds: convertToWindowFrameBounds(f.Bounds)}
}
