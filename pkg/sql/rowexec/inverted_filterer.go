// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedidx"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/errors"
)

// invertedFilterState represents the state of the processor.
type invertedFiltererState int

const (
	ifrStateUnknown invertedFiltererState = iota
	// ifrReadingInput means that the inverted index rows are being read from
	// the input.
	ifrReadingInput
	// ifrEmittingRows means we are emitting the results of the evaluation.
	ifrEmittingRows
)

type invertedFilterer struct {
	execinfra.ProcessorBase
	runningState   invertedFiltererState
	input          execinfra.RowSource
	invertedColIdx uint32

	diskMonitor *mon.BytesMonitor
	rc          *rowcontainer.DiskBackedNumberedRowContainer

	invertedEval batchedInvertedExprEvaluator
	// The invertedEval result.
	evalResult []KeyIndex
	// The next result row, i.e., evalResult[resultIdx].
	resultIdx int

	// Scratch space for constructing the PK row to feed to rc.
	keyRow rowenc.EncDatumRow
	// Scratch space for constructing the output row.
	outputRow rowenc.EncDatumRow
}

var _ execinfra.Processor = &invertedFilterer{}
var _ execinfra.RowSource = &invertedFilterer{}
var _ execinfra.OpNode = &invertedFilterer{}

const invertedFiltererProcName = "inverted filterer"

func newInvertedFilterer(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.InvertedFiltererSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.RowSourcedProcessor, error) {
	ifr := &invertedFilterer{
		input:          input,
		invertedColIdx: spec.InvertedColIdx,
		invertedEval: batchedInvertedExprEvaluator{
			exprs: []*inverted.SpanExpressionProto{&spec.InvertedExpr},
		},
	}

	// The RowContainer columns are all columns other than the inverted column.
	// The output has the same types as the input.
	outputColTypes := input.OutputTypes()
	rcColTypes := make([]*types.T, len(outputColTypes)-1)
	copy(rcColTypes, outputColTypes[:ifr.invertedColIdx])
	copy(rcColTypes[ifr.invertedColIdx:], outputColTypes[ifr.invertedColIdx+1:])
	ifr.keyRow = make(rowenc.EncDatumRow, len(rcColTypes))
	ifr.outputRow = make(rowenc.EncDatumRow, len(outputColTypes))
	ifr.outputRow[ifr.invertedColIdx].Datum = tree.DNull

	// Initialize ProcessorBase.
	if err := ifr.ProcessorBase.Init(
		ifr, post, outputColTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{ifr.input},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				ifr.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	ctx := flowCtx.EvalCtx.Ctx()
	// Initialize memory monitor and row container for input rows.
	ifr.MemMonitor = execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx, "inverted-filterer-limited")
	ifr.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.DiskMonitor, "inverted-filterer-disk")
	ifr.rc = rowcontainer.NewDiskBackedNumberedRowContainer(
		true, /* deDup */
		rcColTypes,
		ifr.EvalCtx,
		ifr.FlowCtx.Cfg.TempStorage,
		ifr.MemMonitor,
		ifr.diskMonitor,
	)

	if execinfra.ShouldCollectStats(flowCtx.EvalCtx.Ctx(), flowCtx) {
		ifr.input = newInputStatCollector(ifr.input)
		ifr.ExecStatsForTrace = ifr.execStatsForTrace
	}

	if spec.PreFiltererSpec != nil {
		semaCtx := flowCtx.TypeResolverFactory.NewSemaContext(flowCtx.EvalCtx.Txn)
		var exprHelper execinfrapb.ExprHelper
		colTypes := []*types.T{spec.PreFiltererSpec.Type}
		if err := exprHelper.Init(spec.PreFiltererSpec.Expression, colTypes, semaCtx, ifr.EvalCtx); err != nil {
			return nil, err
		}
		preFilterer, preFiltererState, err := invertedidx.NewBoundPreFilterer(
			spec.PreFiltererSpec.Type, exprHelper.Expr)
		if err != nil {
			return nil, err
		}
		ifr.invertedEval.filterer = preFilterer
		ifr.invertedEval.preFilterState = append(ifr.invertedEval.preFilterState, preFiltererState)
	}
	// TODO(sumeer): for expressions that only involve unions, and the output
	// does not need to be in key-order, we should incrementally output after
	// de-duping. It will reduce the container memory/disk by 2x.

	// Prepare inverted evaluator for later evaluation.
	_, err := ifr.invertedEval.init()
	if err != nil {
		return nil, err
	}

	return ifr, nil
}

// Next is part of the RowSource interface.
func (ifr *invertedFilterer) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// - Read all the input and add to the row container (with de-duping), and feed it
	//   to the invertedEval.
	// - Evaluate the inverted expression
	// - Retrieve the results and for each row evaluate the ON expression and output.
	for ifr.State == execinfra.StateRunning {
		var row rowenc.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch ifr.runningState {
		case ifrReadingInput:
			ifr.runningState, meta = ifr.readInput()
		case ifrEmittingRows:
			ifr.runningState, row, meta = ifr.emitRow()
		default:
			log.Fatalf(ifr.Ctx, "unsupported state: %d", ifr.runningState)
		}
		if row == nil && meta == nil {
			continue
		}
		if meta != nil {
			return nil, meta
		}
		if outRow := ifr.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, ifr.DrainHelper()
}

func (ifr *invertedFilterer) readInput() (invertedFiltererState, *execinfrapb.ProducerMetadata) {
	row, meta := ifr.input.Next()
	if meta != nil {
		if meta.Err != nil {
			ifr.MoveToDraining(nil /* err */)
			return ifrStateUnknown, meta
		}
		return ifrReadingInput, meta
	}
	if row == nil {
		log.VEventf(ifr.Ctx, 1, "no more input rows")
		evalResult := ifr.invertedEval.evaluate()
		ifr.rc.SetupForRead(ifr.Ctx, evalResult)
		// invertedEval had a single expression in the batch, and the results
		// for that expression are in evalResult[0].
		ifr.evalResult = evalResult[0]
		return ifrEmittingRows, nil
	}
	// Replace missing values with nulls to appease the row container.
	for i := range row {
		if row[i].IsUnset() {
			row[i].Datum = tree.DNull
		}
	}

	// Add to the evaluator.
	//
	// NB: Inverted columns are custom encoded in a manner that does not
	// correspond to Datum encoding, and in the code here we only want the encoded
	// bytes. We have two possibilities with what the provider of this row has
	// done:
	//  1. Not decoded the row: This is the len(enc) > 0 case.
	//  2. Decoded the row, but special-cased the inverted column by stuffing the
	//     encoded bytes into a "decoded" DBytes: This is the len(enc) == 0 case.
	enc := row[ifr.invertedColIdx].EncodedBytes()
	if len(enc) == 0 {
		// If the input is from the vectorized engine, the encoded bytes may be
		// empty (case 2 above). In this case, the Datum should contain the encoded
		// key as a DBytes. The Datum should never be DNull since nulls aren't
		// stored in inverted indexes.
		if row[ifr.invertedColIdx].Datum == nil {
			ifr.MoveToDraining(errors.New("no datum found"))
			return ifrStateUnknown, ifr.DrainHelper()
		}
		if row[ifr.invertedColIdx].Datum.ResolvedType().Family() != types.BytesFamily {
			ifr.MoveToDraining(errors.New("virtual inverted column should have type bytes"))
			return ifrStateUnknown, ifr.DrainHelper()
		}
		enc = []byte(*row[ifr.invertedColIdx].Datum.(*tree.DBytes))
	}
	shouldAdd, err := ifr.invertedEval.prepareAddIndexRow(enc, nil /* encFull */)
	if err != nil {
		ifr.MoveToDraining(err)
		return ifrStateUnknown, ifr.DrainHelper()
	}
	if shouldAdd {
		// Transform to keyRow which is everything other than the inverted
		// column and then add it to the row container and the inverted expr
		// evaluator.
		copy(ifr.keyRow, row[:ifr.invertedColIdx])
		copy(ifr.keyRow[ifr.invertedColIdx:], row[ifr.invertedColIdx+1:])
		keyIndex, err := ifr.rc.AddRow(ifr.Ctx, ifr.keyRow)
		if err != nil {
			ifr.MoveToDraining(err)
			return ifrStateUnknown, ifr.DrainHelper()
		}
		if err = ifr.invertedEval.addIndexRow(keyIndex); err != nil {
			ifr.MoveToDraining(err)
			return ifrStateUnknown, ifr.DrainHelper()
		}
	}
	return ifrReadingInput, nil
}

func (ifr *invertedFilterer) emitRow() (
	invertedFiltererState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	drainFunc := func(err error) (
		invertedFiltererState,
		rowenc.EncDatumRow,
		*execinfrapb.ProducerMetadata,
	) {
		ifr.MoveToDraining(err)
		return ifrStateUnknown, nil, ifr.DrainHelper()
	}
	if ifr.resultIdx >= len(ifr.evalResult) {
		// We are done emitting all rows.
		return drainFunc(ifr.rc.UnsafeReset(ifr.Ctx))
	}
	curRowIdx := ifr.resultIdx
	ifr.resultIdx++
	keyRow, err := ifr.rc.GetRow(ifr.Ctx, ifr.evalResult[curRowIdx], false /* skip */)
	if err != nil {
		return drainFunc(err)
	}
	copy(ifr.outputRow[:ifr.invertedColIdx], keyRow[:ifr.invertedColIdx])
	copy(ifr.outputRow[ifr.invertedColIdx+1:], keyRow[ifr.invertedColIdx:])
	return ifrEmittingRows, ifr.outputRow, nil
}

// Start is part of the RowSource interface.
func (ifr *invertedFilterer) Start(ctx context.Context) {
	ctx = ifr.StartInternal(ctx, invertedFiltererProcName)
	ifr.input.Start(ctx)
	ifr.runningState = ifrReadingInput
}

// ConsumerClosed is part of the RowSource interface.
func (ifr *invertedFilterer) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ifr.close()
}

func (ifr *invertedFilterer) close() {
	if ifr.InternalClose() {
		ifr.rc.Close(ifr.Ctx)
		if ifr.MemMonitor != nil {
			ifr.MemMonitor.Stop(ifr.Ctx)
		}
		if ifr.diskMonitor != nil {
			ifr.diskMonitor.Stop(ifr.Ctx)
		}
	}
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (ifr *invertedFilterer) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := getInputStats(ifr.input)
	if !ok {
		return nil
	}
	return &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		Exec: execinfrapb.ExecStats{
			MaxAllocatedMem:  optional.MakeUint(uint64(ifr.MemMonitor.MaximumBytes())),
			MaxAllocatedDisk: optional.MakeUint(uint64(ifr.diskMonitor.MaximumBytes())),
		},
		Output: ifr.OutputHelper.Stats(),
	}
}

// ChildCount is part of the execinfra.OpNode interface.
func (ifr *invertedFilterer) ChildCount(verbose bool) int {
	if _, ok := ifr.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (ifr *invertedFilterer) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := ifr.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to invertedFilterer is not an execinfra.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
