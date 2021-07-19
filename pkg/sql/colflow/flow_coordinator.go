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
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow/colmeta"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

type flowCoordinator interface {
	spanName() string

	// Methods to be used by the producer goroutine.

	// handleTracing takes in an optional tracing span and propagates the trace
	// to the consumer goroutine if the output is still accepting data. It is
	// guaranteed to be called before drainInput.
	//
	// It is guaranteed to be called exactly once if span is non-nil, regardless
	// of when other methods tell the producer goroutine to exit.
	handleTracing(context.Context, *tracing.Span) (exit bool)
	// startInput initializes the input to the flow coordinator. If an error is
	// returned, the producer will try to communicate it to the consumer
	// goroutine and then will exit.
	startInput(context.Context) error
	// produceNext asks the input to produce the next piece of data and
	// communicates it to the consumer goroutine. If drain is true, then the
	// consumer should stop calling this method and transition to draining; if
	// exit is true, then the consumer should skip draining and finish its
	// execution.
	produceNext(context.Context) (drain, exit bool)
	// drainInput notifies the input that it should transition into draining
	// state. This method exhausts the input (unless the context is canceled or
	// output is closed). Only the metadata is communicated to the producer at
	// this stage.
	drainInput(context.Context)

	// Methods to be used by the consumer (main) goroutine.

	// consumeNext retrieves the next piece of data from the producer goroutine
	// and pushes it to the output. It returns possibly updates status of the
	// output and whether the producer goroutine should stop its execution.
	consumeNext() (outputStatus execinfra.ConsumerStatus, exit bool)
	// close cancels the flow, notifies both the input and the output about
	// shutting down, and optionally performs some other cleanup.
	close()
}

type flowCoordinatorBase struct {
	coordinator flowCoordinator

	flowCtx     *execinfra.FlowCtx
	processorID int32

	// handler should not be accessed after the initialization.
	handler colmeta.StreamingMetadataHandler
	// These three fields point to the same handler object but provide different
	// interfaces in order to have a clear distinction of which methods are
	// allowed to be used by each of the goroutines.
	producer      colmeta.DataProducer
	consumer      colmeta.DataConsumer
	streamingMeta colmeta.StreamingMetadataProducer

	// outputStatus is the execinfra.ConsumerStatus of the output. It must be
	// accessed atomically.
	outputStatus uint32

	// cancelFlow cancels the context of the flow.
	cancelFlow context.CancelFunc
}

var _ colexecop.StreamingMetadataReceiver = &flowCoordinatorBase{}

func (f *flowCoordinatorBase) init(
	coordinator flowCoordinator,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	cancelFlow context.CancelFunc,
	streamingMetadataSources []colexecop.StreamingMetadataSource,
) {
	*f = flowCoordinatorBase{
		coordinator: coordinator,
		flowCtx:     flowCtx,
		processorID: processorID,
		cancelFlow:  cancelFlow,
	}
	f.handler.Init()
	f.producer = &f.handler
	f.consumer = &f.handler
	f.streamingMeta = &f.handler
	for _, s := range streamingMetadataSources {
		s.SetReceiver(f)
	}
}

// PushStreamingMeta is part of the colexecop.StreamingMetadataReceiver
// interface.
func (f *flowCoordinatorBase) PushStreamingMeta(
	ctx context.Context, metadata *execinfrapb.ProducerMetadata,
) error {
	return f.streamingMeta.SendStreamingMeta(ctx, metadata)
}

// getOutputStatus returns the status of the output.
func (f *flowCoordinatorBase) getOutputStatus() execinfra.ConsumerStatus {
	return execinfra.ConsumerStatus(atomic.LoadUint32(&f.outputStatus))
}

// setOutputStatus updates the status of the output.
func (f *flowCoordinatorBase) setOutputStatus(status execinfra.ConsumerStatus) {
	atomic.StoreUint32(&f.outputStatus, uint32(status))
}

// Run is the main loop of the coordinator. It runs the flow to completion and
// then shuts it down.
func (f *flowCoordinatorBase) Run(flowCtx context.Context) {
	waitCh := make(chan struct{})
	defer func() {
		f.coordinator.close()
		// Block the main goroutine until the producer exits. This is needed in
		// order to not release the flow coordinator before it still might be
		// used by the producer goroutine.
		<-waitCh
	}()

	// Spin up the producer goroutine that will be reading from the input and
	// communicating all the data to the consumer goroutine (the current one).
	// TODO(yuzefovich): consider using stopper to run this goroutine.
	go func(flowCtx context.Context) {
		defer close(waitCh)
		defer f.producer.ProducerDone()
		<-f.producer.WaitForConsumer()
		// Check whether we have been canceled while we were waiting for the
		// consumer to arrive.
		if err := flowCtx.Err(); err != nil {
			log.VEventf(flowCtx, 1, "%s", err.Error())
			return
		}

		tracingHandled := false
		var ctx context.Context
		var span *tracing.Span
		if !f.flowCtx.Cfg.TestingKnobs.DistSQLNoSpans {
			ctx, span = execinfra.ProcessorSpan(flowCtx, f.coordinator.spanName())
			if span != nil {
				if span.IsVerbose() {
					span.SetTag(execinfrapb.FlowIDTagKey, f.flowCtx.ID.String())
					span.SetTag(execinfrapb.ProcessorIDTagKey, f.processorID)
				}
				defer func() {
					if !tracingHandled {
						f.coordinator.handleTracing(ctx, span)
					}
					span.Finish()
				}()
			}
		} else {
			ctx = flowCtx
		}

		if err := f.coordinator.startInput(ctx); err != nil {
			meta := execinfrapb.GetProducerMeta()
			meta.Err = err
			// We don't care about the returned error since we're exiting
			// anyway.
			_ = f.producer.SendMeta(ctx, meta)
			return
		}

		// This is the main loop of the producer goroutine.
		for f.getOutputStatus() == execinfra.NeedMoreRows {
			drain, exit := f.coordinator.produceNext(ctx)
			if exit {
				return
			}
			if drain {
				break
			}
		}

		tracingHandled = true
		if exit := f.coordinator.handleTracing(ctx, span); exit {
			return
		}
		f.coordinator.drainInput(ctx)
	}(flowCtx)

	// This is the body of the consumer goroutine of the coordinator.
	f.consumer.ConsumerArrived()
	for {
		status, exit := f.coordinator.consumeNext()
		f.setOutputStatus(status)
		if status == execinfra.ConsumerClosed || exit {
			return
		}
	}
}

// FlowCoordinator is the execinfra.Processor that is responsible for shutting
// down the vectorized flow on the gateway node.
type FlowCoordinator struct {
	flowCoordinatorBase
	colexecop.NonExplainable

	input  execinfra.RowSource
	output execinfra.RowReceiver

	row  rowenc.EncDatumRow
	meta *execinfrapb.ProducerMetadata
}

var _ execinfra.OpNode = &FlowCoordinator{}
var _ execinfra.Processor = &FlowCoordinator{}
var _ execinfra.Releasable = &FlowCoordinator{}
var _ flowCoordinator = &FlowCoordinator{}

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
	streamingMetadataSources []colexecop.StreamingMetadataSource,
	cancelFlow context.CancelFunc,
) *FlowCoordinator {
	f := flowCoordinatorPool.Get().(*FlowCoordinator)
	f.input = input
	f.output = output
	f.init(f, flowCtx, processorID, cancelFlow, streamingMetadataSources)
	return f
}

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

// MustBeStreaming is part of the execinfra.Processor interface.
func (f *FlowCoordinator) MustBeStreaming() bool {
	return false
}

func (f *FlowCoordinator) spanName() string {
	return "row flow coordinator"
}

func (f *FlowCoordinator) handleTracing(ctx context.Context, span *tracing.Span) (exit bool) {
	// Propagate the trace if necessary and if the output is still accepting
	// data.
	if span != nil && f.getOutputStatus() != execinfra.ConsumerClosed {
		if meta := execinfra.GetTraceDataAsMetadata(span); meta != nil {
			return f.producer.SendMeta(ctx, meta) != nil
		}
	}
	return false
}

func (f *FlowCoordinator) startInput(ctx context.Context) error {
	return colexecerror.CatchVectorizedRuntimeError(func() {
		f.input.Start(ctx)
	})
}

func (f *FlowCoordinator) nextAdapter() {
	f.row, f.meta = f.input.Next()
}

func (f *FlowCoordinator) produceNext(ctx context.Context) (drain, exit bool) {
	err := colexecerror.CatchVectorizedRuntimeError(f.nextAdapter)
	if err != nil {
		meta := execinfrapb.GetProducerMeta()
		meta.Err = err
		return false, f.producer.SendMeta(ctx, meta) != nil
	}
	if f.row == nil && f.meta == nil {
		// The input has been fully exhausted.
		return false, true
	}
	if f.row != nil {
		return false, f.producer.SendRow(ctx, f.row) != nil
	}
	return false, f.producer.SendMeta(ctx, f.meta) != nil
}

func (f *FlowCoordinator) consumeNext() (outputStatus execinfra.ConsumerStatus, exit bool) {
	row, meta := f.consumer.NextRowAndMeta()
	if row == nil && meta == nil {
		return outputStatus, true
	}
	return f.output.Push(row, meta), false
}

func (f *FlowCoordinator) drainInput(ctx context.Context) {
	f.input.ConsumerDone()
	for f.getOutputStatus() != execinfra.ConsumerClosed {
		row, meta := f.input.Next()
		if row != nil {
			continue
		}
		if meta == nil {
			return
		}
		// Note that we don't need to check whether meta should be swallowed
		// because the input must be doing that already (by using DrainHelper
		// in the draining state).
		if err := f.producer.SendMeta(ctx, meta); err != nil {
			return
		}
	}
}

func (f *FlowCoordinator) close() {
	f.cancelFlow()
	f.input.ConsumerClosed()
	f.output.ProducerDone()
}

// Release implements the execinfra.Releasable interface.
func (f *FlowCoordinator) Release() {
	*f = FlowCoordinator{}
	flowCoordinatorPool.Put(f)
}

// BatchFlowCoordinator is a component that is responsible for running the
// vectorized flow (by receiving the batches from the root operator and pushing
// them to the batch receiver) and shutting down the whole flow when done. It
// can only be planned on the gateway node when colexecop.Operator is the root
// of the tree and the consumer is an execinfra.BatchReceiver.
type BatchFlowCoordinator struct {
	flowCoordinatorBase
	colexecop.OneInputNode
	colexecop.NonExplainable

	allocator *colmem.Allocator
	input     colexecargs.OpWithMetaInfo
	output    execinfra.BatchReceiver

	batch coldata.Batch
}

var _ execinfra.OpNode = &BatchFlowCoordinator{}
var _ execinfra.Releasable = &BatchFlowCoordinator{}
var _ flowCoordinator = &BatchFlowCoordinator{}

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
	allocator *colmem.Allocator,
	processorID int32,
	input colexecargs.OpWithMetaInfo,
	output execinfra.BatchReceiver,
	cancelFlow context.CancelFunc,
) *BatchFlowCoordinator {
	f := batchFlowCoordinatorPool.Get().(*BatchFlowCoordinator)
	*f = BatchFlowCoordinator{
		OneInputNode: colexecop.NewOneInputNode(input.Root),
		allocator:    allocator,
		input:        input,
		output:       output,
	}
	f.init(f, flowCtx, processorID, cancelFlow, input.StreamingMetadataSources)
	return f
}

func (f *BatchFlowCoordinator) spanName() string {
	return "batch flow coordinator"
}

func (f *BatchFlowCoordinator) handleTracing(ctx context.Context, span *tracing.Span) (exit bool) {
	// Collect the stats and propagate the trace if necessary and if the output
	// is still accepting data.
	if span != nil && f.getOutputStatus() != execinfra.ConsumerClosed {
		for _, s := range f.input.StatsCollectors {
			span.RecordStructured(s.GetStats())
		}
		if meta := execinfra.GetTraceDataAsMetadata(span); meta != nil {
			return f.producer.SendMeta(ctx, meta) != nil
		}
	}
	return false
}

func (f *BatchFlowCoordinator) startInput(ctx context.Context) error {
	return colexecerror.CatchVectorizedRuntimeError(func() {
		f.input.Root.Init(ctx)
	})
}

func (f *BatchFlowCoordinator) nextAdapter() {
	f.batch = f.input.Root.Next()
}

func (f *BatchFlowCoordinator) produceNext(ctx context.Context) (drain, exit bool) {
	err := colexecerror.CatchVectorizedRuntimeError(f.nextAdapter)
	if err != nil {
		meta := execinfrapb.GetProducerMeta()
		meta.Err = err
		return false, f.producer.SendMeta(ctx, meta) != nil
	}
	if f.batch.Length() == 0 {
		// All rows have been exhausted, so we transition to draining.
		return true, false
	}
	return false, f.producer.SendBatch(ctx, f.batch, f.allocator) != nil
}

func (f *BatchFlowCoordinator) consumeNext() (outputStatus execinfra.ConsumerStatus, exit bool) {
	batch, meta := f.consumer.NextBatchAndMeta()
	if batch == nil && meta == nil {
		return outputStatus, true
	}
	return f.output.PushBatch(batch, meta), false
}

func (f *BatchFlowCoordinator) drainInput(ctx context.Context) {
	// Drain all metadata sources.
	drainedMeta := f.input.MetadataSources.DrainMeta()
	for i := range drainedMeta {
		if execinfra.ShouldSwallowReadWithinUncertaintyIntervalError(&drainedMeta[i]) {
			// This metadata object contained an error that was swallowed
			// per the contract of execinfra.StateDraining.
			continue
		}
		if f.getOutputStatus() == execinfra.ConsumerClosed {
			return
		}
		if err := f.producer.SendMeta(ctx, &drainedMeta[i]); err != nil {
			return
		}
	}
}

func (f *BatchFlowCoordinator) close() {
	f.cancelFlow()
	var lastErr error
	for _, toClose := range f.input.ToClose {
		if err := toClose.Close(); err != nil {
			lastErr = err
		}
	}
	if lastErr != nil && f.getOutputStatus() != execinfra.ConsumerClosed {
		meta := execinfrapb.GetProducerMeta()
		meta.Err = lastErr
		_ = f.output.PushBatch(nil /* batch */, meta)
	}
	f.output.ProducerDone()
}

// Release implements the execinfra.Releasable interface.
func (f *BatchFlowCoordinator) Release() {
	*f = BatchFlowCoordinator{}
	batchFlowCoordinatorPool.Put(f)
}
