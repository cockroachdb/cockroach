// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colflow

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel/attribute"
)

// FlowCoordinator is the execinfra.Processor that is responsible for shutting
// down the vectorized flow on the gateway node.
type FlowCoordinator struct {
	execinfra.ProcessorBaseNoHelper
	colexecop.NonExplainable

	input execinfra.RowSource

	// row and meta are the results produced by calling input.Next stored here
	// in order for that call to be wrapped in the panic-catcher.
	row  rowenc.EncDatumRow
	meta *execinfrapb.ProducerMetadata

	// cancelFlow cancels the context of the flow.
	cancelFlow context.CancelFunc
}

var flowCoordinatorPool = sync.Pool{
	New: func() interface{} {
		return &FlowCoordinator{}
	},
}

// NewFlowCoordinator creates a new FlowCoordinator processor that is the root
// of the vectorized flow.
// - cancelFlow is the cancellation function of the flow's context (i.e. it is
// Flow.ctxCancel).
func NewFlowCoordinator(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
	cancelFlow context.CancelFunc,
) *FlowCoordinator {
	f := flowCoordinatorPool.Get().(*FlowCoordinator)
	f.input = input
	f.cancelFlow = cancelFlow
	f.Init(
		f,
		flowCtx,
		// FlowCoordinator doesn't modify the eval context, so it is safe to
		// reuse the one from the flow context.
		flowCtx.EvalCtx,
		processorID,
		output,
		execinfra.ProcStateOpts{
			// We append input to inputs to drain below in order to reuse
			// the same underlying slice from the pooled FlowCoordinator.
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				// Note that the input must have been drained by the
				// ProcessorBaseNoHelper by this point, so we can just close the
				// FlowCoordinator.
				f.close()
				return nil
			},
		},
	)
	f.AddInputToDrain(input)
	return f
}

var _ execinfra.OpNode = &FlowCoordinator{}
var _ execinfra.Processor = &FlowCoordinator{}
var _ execinfra.Releasable = &FlowCoordinator{}

// ChildCount is part of the execinfra.OpNode interface.
func (f *FlowCoordinator) ChildCount(verbose bool) int {
	return 1
}

// Child is part of the execinfra.OpNode interface.
func (f *FlowCoordinator) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		// The input must be the execinfra.OpNode (it's either a materializer or
		// a wrapped row-execution processor).
		return f.input.(execinfra.OpNode)
	}
	colexecerror.InternalError(errors.AssertionFailedf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// OutputTypes is part of the execinfra.Processor interface.
func (f *FlowCoordinator) OutputTypes() []*types.T {
	return f.input.OutputTypes()
}

// Start is part of the execinfra.RowSource interface.
func (f *FlowCoordinator) Start(ctx context.Context) {
	ctx = f.StartInternalNoSpan(ctx)
	if err := colexecerror.CatchVectorizedRuntimeError(func() {
		f.input.Start(ctx)
	}); err != nil {
		f.MoveToDraining(err)
	}
}

func (f *FlowCoordinator) next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if f.State == execinfra.StateRunning {
		row, meta := f.input.Next()
		if meta != nil {
			if meta.Err != nil {
				f.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row != nil {
			return row, nil
		}
		// Both row and meta are nil, so we transition to draining.
		f.MoveToDraining(nil /* err */)
	}
	return nil, f.DrainHelper()
}

func (f *FlowCoordinator) nextAdapter() {
	f.row, f.meta = f.next()
}

// Next is part of the execinfra.RowSource interface.
func (f *FlowCoordinator) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if err := colexecerror.CatchVectorizedRuntimeError(f.nextAdapter); err != nil {
		f.MoveToDraining(err)
		return nil, f.DrainHelper()
	}
	return f.row, f.meta
}

func (f *FlowCoordinator) close() {
	if f.InternalClose() {
		f.cancelFlow()
	}
}

// ConsumerClosed is part of the execinfra.RowSource interface.
func (f *FlowCoordinator) ConsumerClosed() {
	f.close()
}

// Release implements the execinfra.Releasable interface.
func (f *FlowCoordinator) Release() {
	f.ProcessorBaseNoHelper.Reset()
	*f = FlowCoordinator{
		// We're keeping the reference to the same ProcessorBaseNoHelper since
		// it allows us to reuse some of the slices.
		ProcessorBaseNoHelper: f.ProcessorBaseNoHelper,
	}
	flowCoordinatorPool.Put(f)
}

// BatchFlowCoordinator is a component that is responsible for running the
// vectorized flow (by receiving the batches from the root operator and pushing
// them to the batch receiver) and shutting down the whole flow when done. It
// can only be planned on the gateway node when colexecop.Operator is the root
// of the tree and the consumer is an execinfra.BatchReceiver.
type BatchFlowCoordinator struct {
	colexecop.OneInputNode
	colexecop.NonExplainable
	flowCtx     *execinfra.FlowCtx
	processorID int32

	input  colexecargs.OpWithMetaInfo
	output execinfra.BatchReceiver

	// batch is the result produced by calling input.Next stored here in order
	// for that call to be wrapped in the panic-catcher.
	batch coldata.Batch

	// cancelFlow cancels the context of the flow.
	cancelFlow context.CancelFunc
}

var batchFlowCoordinatorPool = sync.Pool{
	New: func() interface{} {
		return &BatchFlowCoordinator{}
	},
}

// NewBatchFlowCoordinator creates a new BatchFlowCoordinator operator that is
// the root of the vectorized flow.
// - cancelFlow is the cancellation function of the flow's context (i.e. it is
// Flow.ctxCancel).
func NewBatchFlowCoordinator(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input colexecargs.OpWithMetaInfo,
	output execinfra.BatchReceiver,
	cancelFlow context.CancelFunc,
) *BatchFlowCoordinator {
	f := batchFlowCoordinatorPool.Get().(*BatchFlowCoordinator)
	*f = BatchFlowCoordinator{
		OneInputNode: colexecop.NewOneInputNode(input.Root),
		flowCtx:      flowCtx,
		processorID:  processorID,
		input:        input,
		output:       output,
		cancelFlow:   cancelFlow,
	}
	return f
}

var _ execinfra.OpNode = &BatchFlowCoordinator{}
var _ execinfra.Releasable = &BatchFlowCoordinator{}

func (f *BatchFlowCoordinator) init(ctx context.Context) error {
	return colexecerror.CatchVectorizedRuntimeError(func() {
		f.input.Root.Init(ctx)
	})
}

func (f *BatchFlowCoordinator) nextAdapter() {
	f.batch = f.input.Root.Next()
}

func (f *BatchFlowCoordinator) next() error {
	return colexecerror.CatchVectorizedRuntimeError(f.nextAdapter)
}

func (f *BatchFlowCoordinator) pushError(err error) execinfra.ConsumerStatus {
	meta := execinfrapb.GetProducerMeta()
	meta.Err = err
	return f.output.PushBatch(nil /* batch */, meta)
}

// Run is the main loop of the coordinator. It runs the flow to completion and
// then shuts it down.
func (f *BatchFlowCoordinator) Run(ctx context.Context) {
	status := execinfra.NeedMoreRows

	ctx, span := execinfra.ProcessorSpan(ctx, "batch flow coordinator")
	if span != nil {
		if span.IsVerbose() {
			span.SetTag(execinfrapb.FlowIDTagKey, attribute.StringValue(f.flowCtx.ID.String()))
			span.SetTag(execinfrapb.ProcessorIDTagKey, attribute.IntValue(int(f.processorID)))
		}
	}

	// Make sure that we close the coordinator and notify the batch receiver in
	// all cases.
	defer func() {
		if err := f.close(ctx); err != nil && status != execinfra.ConsumerClosed {
			f.pushError(err)
		}
		f.output.ProducerDone()
		// Note that f.close is only safe to call before finishing the tracing
		// span because some components might still use the span when they are
		// being closed.
		span.Finish()
	}()

	if err := f.init(ctx); err != nil {
		f.pushError(err)
		// If initialization is not successful, we just exit since the operator
		// tree might not be setup properly.
		return
	}

	for status == execinfra.NeedMoreRows {
		err := f.next()
		if err != nil {
			switch status = f.pushError(err); status {
			case execinfra.ConsumerClosed:
				return
			}
			continue
		}
		if f.batch.Length() == 0 {
			// All rows have been exhausted, so we transition to draining.
			break
		}
		switch status = f.output.PushBatch(f.batch, nil /* meta */); status {
		case execinfra.ConsumerClosed:
			return
		}
	}

	// Collect the stats and get the trace if necessary.
	if span != nil {
		for _, s := range f.input.StatsCollectors {
			span.RecordStructured(s.GetStats())
		}
		if meta := execinfra.GetTraceDataAsMetadata(span); meta != nil {
			status = f.output.PushBatch(nil /* batch */, meta)
			if status == execinfra.ConsumerClosed {
				return
			}
		}
	}

	// Drain all metadata sources.
	drainedMeta := f.input.MetadataSources.DrainMeta()
	for i := range drainedMeta {
		if execinfra.ShouldSwallowReadWithinUncertaintyIntervalError(&drainedMeta[i]) {
			// This metadata object contained an error that was swallowed per
			// the contract of execinfra.StateDraining.
			continue
		}
		status = f.output.PushBatch(nil /* batch */, &drainedMeta[i])
		if status == execinfra.ConsumerClosed {
			return
		}
	}
}

// close cancels the flow and closes all colexecop.Closers the coordinator is
// responsible for.
func (f *BatchFlowCoordinator) close(ctx context.Context) error {
	f.cancelFlow()
	var lastErr error
	for _, toClose := range f.input.ToClose {
		if err := toClose.Close(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Release implements the execinfra.Releasable interface.
func (f *BatchFlowCoordinator) Release() {
	*f = BatchFlowCoordinator{}
	batchFlowCoordinatorPool.Put(f)
}
