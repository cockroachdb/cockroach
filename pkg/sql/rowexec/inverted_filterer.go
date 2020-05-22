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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
)

// invertedFilterState represents the state of the processor.
type invertedFiltererState int

const (
	ifStateUnknown invertedFiltererState = iota
	// ifReadingInput means that the inverted index rows are being read from
	// the input.
	ifReadingInput
	// ifEmittingRows means we are emitting the results of the evaluation.
	ifEmittingRows
)

type invertedFilterer struct {
	execinfra.ProcessorBase
	runningState invertedFiltererState
	input        execinfra.RowSource
	inputTypes   []*types.T

	diskMonitor *mon.BytesMonitor
	rc          *rowcontainer.DiskBackedNumberedRowContainer

	invertedEval batchedInvertedExprEvaluator
	// The invertedEval result.
	evalResult []KeyIndex
	// The next result row, i.e., evalResult[resultIdx].
	resultIdx int

	onExprHelper execinfra.ExprHelper
}

var _ execinfra.Processor = (*invertedFilterer)(nil)
var _ execinfra.RowSource = (*invertedFilterer)(nil)
var _ execinfrapb.MetadataSource = (*invertedFilterer)(nil)
var _ execinfra.OpNode = (*invertedFilterer)(nil)

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
		input:      input,
		inputTypes: input.OutputTypes(),
		invertedEval: batchedInvertedExprEvaluator{
			exprs: []*invertedexpr.SpanExpressionProto{&spec.InvertedExpr},
		},
	}

	// TODO(sumeer): for expressions that only involve unions, and the output
	// does not need to be in key-order, we should incrementally output after
	// de-duping. It will reduce the container memory/disk by 2x.

	// Prepare inverted evaluator for later evaluation. The returned spans are
	// ignored since the input is already setup to provide rows from the spans.
	ifr.invertedEval.getSpans()

	outputColTypes := make([]*types.T, len(ifr.inputTypes)-1)
	copy(outputColTypes, ifr.inputTypes[1:])

	// Initialize ProcessorBase.
	if err := ifr.ProcessorBase.Init(
		ifr, post, outputColTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{ifr.input},
			TrailingMetaCallback: func(ctx context.Context) []execinfrapb.ProducerMetadata {
				ifr.close()
				return ifr.generateMeta(ctx)
			},
		},
	); err != nil {
		return nil, err
	}

	// Initialize evaluation of OnExpr.
	if err := ifr.onExprHelper.Init(spec.OnExpr, outputColTypes, ifr.EvalCtx); err != nil {
		return nil, err
	}

	ctx := flowCtx.EvalCtx.Ctx()
	// Initialize memory monitor and row container for input rows.
	ifr.MemMonitor = execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg, "inverter-filterer-limited")
	ifr.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, "inverted-filterer-disk")
	ifr.rc = rowcontainer.NewDiskBackedNumberedRowContainer(
		true, /* deDup */
		outputColTypes,
		ifr.EvalCtx,
		ifr.FlowCtx.Cfg.TempStorage,
		ifr.MemMonitor,
		ifr.diskMonitor,
		0, /* rowCapacity */
	)

	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		ifr.input = newInputStatCollector(ifr.input)
		ifr.FinishTrace = ifr.outputStatsToTrace
	}

	return ifr, nil
}

// Spilled returns whether the invertedFilterer spilled to disk.
func (ifr *invertedFilterer) Spilled() bool {
	return ifr.rc.UsingDisk()
}

// Next is part of the RowSource interface.
func (ifr *invertedFilterer) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// - Read all the input and add to the row container (with de-duping), and feed it
	//   to the invertedEval.
	// - Evaluate the inverted expression
	// - Retrieve the results and for each row evaluate the on expression and output.
	for ifr.State == execinfra.StateRunning {
		var row sqlbase.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch ifr.runningState {
		case ifReadingInput:
			ifr.runningState, meta = ifr.readInput()
		case ifEmittingRows:
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
			return ifStateUnknown, meta
		}
		return ifReadingInput, meta
	}
	if row == nil {
		log.VEventf(ifr.Ctx, 1, "no more input rows")
		evalResult := ifr.invertedEval.evaluate()
		ifr.rc.SetupForRead(ifr.Ctx, evalResult)
		ifr.evalResult = evalResult[0]
		return ifEmittingRows, nil
	}
	// Replace missing values with nulls to appease the row container.
	for i := range row {
		if row[i].IsUnset() {
			row[i].Datum = tree.DNull
		}
	}
	// Add the primary key in the row to the row container.
	keyIndex, err := ifr.rc.AddRow(ifr.Ctx, row[1:])
	if err != nil {
		ifr.MoveToDraining(err)
		return ifStateUnknown, ifr.DrainHelper()
	}
	// Add to the evaluator.
	ifr.invertedEval.addIndexRow(row[0].EncodedBytes(), KeyIndex(keyIndex))
	return ifReadingInput, nil
}

func (ifr *invertedFilterer) emitRow() (
	invertedFiltererState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	drainFunc := func(err error) (
		invertedFiltererState,
		sqlbase.EncDatumRow,
		*execinfrapb.ProducerMetadata,
	) {
		ifr.MoveToDraining(err)
		return ifStateUnknown, nil, ifr.DrainHelper()
	}
	if ifr.resultIdx > len(ifr.evalResult) {
		if err := ifr.rc.UnsafeReset(ifr.Ctx); err != nil {
			return drainFunc(err)
		}
	}
	currRowIdx := ifr.resultIdx
	ifr.resultIdx++
	row, err := ifr.rc.GetRow(ifr.Ctx, currRowIdx, false /* skip */)
	if err != nil {
		return drainFunc(err)
	}
	if ifr.onExprHelper.Expr != nil {
		res, err := ifr.onExprHelper.EvalFilter(row)
		if err != nil {
			return drainFunc(err)
		}
		if !res {
			return ifEmittingRows, nil, nil
		}
	}
	return ifEmittingRows, row, nil
}

// Start is part of the RowSource interface.
func (ifr *invertedFilterer) Start(ctx context.Context) context.Context {
	ifr.input.Start(ctx)
	ctx = ifr.StartInternal(ctx, invertedFiltererProcName)
	ifr.runningState = ifReadingInput
	return ctx
}

// ConsumerClosed is part of the RowSource interface.
func (ifr *invertedFilterer) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ifr.close()
}

func (ifr *invertedFilterer) close() {
	if ifr.InternalClose() {
		if ifr.MemMonitor != nil {
			ifr.MemMonitor.Stop(ifr.Ctx)
		}
		if ifr.diskMonitor != nil {
			ifr.diskMonitor.Stop(ifr.Ctx)
		}
	}
}

var _ execinfrapb.DistSQLSpanStats = (*InvertedFiltererStats)(nil)

const invertedFiltererTagPrefix = "invertedfilterer."

// Stats implements the SpanStats interface.
func (ifs *InvertedFiltererStats) Stats() map[string]string {
	statsMap := ifs.InputStats.Stats(invertedFiltererTagPrefix)
	statsMap[invertedFiltererTagPrefix+MaxMemoryTagSuffix] = humanizeutil.IBytes(ifs.MaxAllocatedMem)
	statsMap[invertedFiltererTagPrefix+MaxDiskTagSuffix] = humanizeutil.IBytes(ifs.MaxAllocatedDisk)
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (ifs *InvertedFiltererStats) StatsForQueryPlan() []string {
	stats := ifs.InputStats.StatsForQueryPlan("" /* prefix */)
	if ifs.MaxAllocatedMem != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(ifs.MaxAllocatedMem)))
	}
	if ifs.MaxAllocatedDisk != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", MaxDiskQueryPlanSuffix, humanizeutil.IBytes(ifs.MaxAllocatedDisk)))
	}
	return stats
}

// outputStatsToTrace outputs the collected invertedFilterer stats to the
// trace. Will fail silently if the invertedFilterer is not collecting stats.
func (ifr *invertedFilterer) outputStatsToTrace() {
	is, ok := getInputStats(ifr.FlowCtx, ifr.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(ifr.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp,
			&InvertedFiltererStats{
				InputStats:       is,
				MaxAllocatedMem:  ifr.MemMonitor.MaximumBytes(),
				MaxAllocatedDisk: ifr.diskMonitor.MaximumBytes(),
			},
		)
	}
}

func (ifr *invertedFilterer) generateMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	if tfs := execinfra.GetLeafTxnFinalState(ctx, ifr.FlowCtx.Txn); tfs != nil {
		return []execinfrapb.ProducerMetadata{{LeafTxnFinalState: tfs}}
	}
	return nil
}

// DrainMeta is part of the MetadataSource interface.
func (ifr *invertedFilterer) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	return ifr.generateMeta(ctx)
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
	panic(fmt.Sprintf("invalid index %d", nth))
}
