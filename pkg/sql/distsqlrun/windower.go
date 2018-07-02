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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// windowerState represents the state of the processor.
type windowerState int

const (
	windowerStateUnknown windowerState = iota

	windowerAccumulating

	windowerEmittingRows
)

type windower struct {
	processorBase

	runningState windowerState
	input        RowSource
	inputDone    bool
	inputTypes   []sqlbase.ColumnType
	outputTypes  []sqlbase.ColumnType
	datumAlloc   sqlbase.DatumAlloc
	rowAlloc     sqlbase.EncDatumRowAlloc

	bucketsAcc mon.BoundAccount

	row     sqlbase.EncDatumRow
	scratch []byte

	cancelChecker *sqlbase.CancelChecker

	//=====
	partitionBy []uint32
	partitions  map[string][]sqlbase.EncDatumRow

	wrappedRenderVals *sqlbase.RowContainer

	// The populated values for this windowNode.
	populated bool

	windowValues [][]tree.Datum
	curRowIdx    int
	windowFrames []*tree.WindowFrame

	windowsAcc mon.BoundAccount
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
	outputTypes := input.OutputTypes()
	w := &windower{
		input: input,
	}
	if err := w.init(
		w,
		post,
		outputTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		procStateOpts{inputsToDrain: []RowSource{w.input}},
	); err != nil {
		return nil, err
	}
	w.inputTypes = input.OutputTypes()
	w.partitionBy = spec.PartitionBy
	w.partitions = make(map[string][]sqlbase.EncDatumRow)
	return w, nil
}

// Start is part of the RowSource interface.
func (w *windower) Start(ctx context.Context) context.Context {
	w.input.Start(ctx)
	ctx = w.startInternal(ctx, windowerProcName)
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

func (w *windower) accumulateRows() (windowerState, sqlbase.EncDatumRow, *ProducerMetadata) {
	for {
		row, meta := w.input.Next()
		//fmt.Printf("Accumulating\nrow: %+v\nmeta: %+v\n\n", row, meta)
		if meta != nil {
			if meta.Err != nil {
				w.moveToDraining(nil /* err */)
				return windowerStateUnknown, nil, meta
			}
			return windowerAccumulating, nil, meta
		}
		if row == nil {
			//log.VEvent(w.ctx, 1, "accumulation complete")
			w.inputDone = true
			break
		}

		if len(w.partitionBy) == 0 {
			w.partitions[""] = append(w.partitions[""], row)
		} else {
			w.scratch = w.scratch[:0]
			for _, col := range w.partitionBy {
				if int(col) >= len(row) {
					fmt.Printf("hash column %d, row with only %d columns", col, len(row))
				}
				var err error
				w.scratch, err = row[int(col)].Encode(&w.inputTypes[int(col)], &w.datumAlloc, preferredEncoding, w.scratch)
				if err != nil {
					fmt.Printf("error received: %+v\n\n", err)
				}
			}

			bucket := int(crc32.Update(0, crc32Table, w.scratch))
			w.partitions[string(bucket)] = append(w.partitions[string(bucket)], w.rowAlloc.CopyRow(row))
		}
	}

	return windowerEmittingRows, nil, nil
}

func (w *windower) emitRow() (windowerState, sqlbase.EncDatumRow, *ProducerMetadata) {
	if w.inputDone {
		for !w.populated {
			if err := w.cancelChecker.Check(); err != nil {
				w.moveToDraining(err)
				return windowerStateUnknown, nil, nil
			}

			w.populated = true
			if err := w.computeWindows(w.ctx, w.evalCtx); err != nil {
				w.moveToDraining(err)
				return windowerStateUnknown, nil, nil
			}
			//w.values.rows = sqlbase.NewRowContainer(
			//	params.EvalContext().Mon.MakeBoundAccount(),
			//	sqlbase.ColTypeInfoFromResCols(n.run.values.columns),
			//	n.run.wrappedRenderVals.Len(),
			//)
			//if err := n.populateValues(params.ctx, params.EvalContext()); err != nil {
			//	return false, err
			//}
			//break
		}

		//values := n.plan.Values()
		//if _, err := n.run.wrappedRenderVals.AddRow(params.ctx, values); err != nil {
		//	return false, err
		//}
		//
		//return n.run.values.Next(params)

		for idx, partition := range w.partitions {
			if partition != nil {
				fmt.Printf("partition %v:\n", idx)
				for i, r := range partition {
					fmt.Printf("%v: %+v\n", i, r)
				}
			}
		}
		w.moveToDraining(nil /* err */)
		return windowerStateUnknown, nil, nil
	}

	return windowerStateUnknown, nil, nil
}

func (w *windower) computeWindows(ctx context.Context, evalCtx *tree.EvalContext) error {
	//for _, partition := range w.partitions {
	//	builtin := windowFn.expr.GetWindowConstructor()(evalCtx)
	//	defer builtin.Close(ctx, evalCtx)
	//
	//	// In order to calculate aggregates over a particular window frame,
	//	// we need a way to 'reset' the aggregate, so this constructor will be used for that.
	//	aggConstructor := windowFn.expr.GetAggregateConstructor()
	//
	//	var peerGrouper peerGroupChecker
	//	if windowFn.columnOrdering != nil {
	//		// If an ORDER BY clause is provided, order the partition and use the
	//		// sorter as our peerGroupChecker.
	//		sorter := &partitionSorter{
	//			evalCtx:       evalCtx,
	//			rows:          partition,
	//			windowDefVals: n.run.wrappedRenderVals,
	//			ordering:      windowFn.columnOrdering,
	//		}
	//		// The sort needs to be deterministic because multiple window functions with
	//		// syntactically equivalent ORDER BY clauses in their window definitions
	//		// need to be guaranteed to be evaluated in the same order, even if the
	//		// ORDER BY *does not* uniquely determine an ordering. In the future, this
	//		// could be guaranteed by only performing a single pass over a sorted partition
	//		// for functions with syntactically equivalent PARTITION BY and ORDER BY clauses.
	//		sort.Sort(sorter)
	//		peerGrouper = sorter
	//	} else if frameRun.Frame != nil && frameRun.Frame.Mode == tree.ROWS {
	//		// If ORDER BY clause is not provided and Frame is specified with ROWS mode,
	//		// any row has no peers.
	//		peerGrouper = noPeers{}
	//	} else {
	//		// If ORDER BY clause is not provided and either no Frame is provided or Frame is
	//		// specified with RANGE mode, all rows are peers.
	//		peerGrouper = allPeers{}
	//	}
	//
	//	frameRun.Rows = partition
	//	frameRun.ArgIdxStart = windowFn.argIdxStart
	//	frameRun.ArgCount = windowFn.argCount
	//	frameRun.RowIdx = 0
	//
	//	if frameRun.Frame != nil {
	//		builtins.AddAggregateConstructorToFramableAggregate(builtin, aggConstructor)
	//	}
	//
	//	for frameRun.RowIdx < len(partition) {
	//		// Compute the size of the current peer group.
	//		frameRun.FirstPeerIdx = frameRun.RowIdx
	//		frameRun.PeerRowCount = 1
	//		for ; frameRun.FirstPeerIdx+frameRun.PeerRowCount < frameRun.PartitionSize(); frameRun.PeerRowCount++ {
	//			cur := frameRun.FirstPeerIdx + frameRun.PeerRowCount
	//			if !peerGrouper.InSameGroup(cur, cur-1) {
	//				break
	//			}
	//		}
	//
	//		// Perform calculations on each row in the current peer group.
	//		for ; frameRun.RowIdx < frameRun.FirstPeerIdx+frameRun.PeerRowCount; frameRun.RowIdx++ {
	//			res, err := builtin.Compute(ctx, evalCtx, frameRun)
	//			if err != nil {
	//				return err
	//			}
	//
	//			// This may overestimate, because WindowFuncs may perform internal caching.
	//			sz := res.Size()
	//			if err := n.run.windowsAcc.Grow(ctx, int64(sz)); err != nil {
	//				return err
	//			}
	//
	//			// Save result into n.run.windowValues, indexed by original row index.
	//			valRowIdx := partition[frameRun.RowIdx].Idx
	//			n.run.windowValues[valRowIdx][windowIdx] = res
	//		}
	//	}
	//}
	return nil
}
