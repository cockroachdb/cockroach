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

	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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

	// cancelFlow will return a function to cancel the context of the flow. It
	// is a function in order to be lazily evaluated, since the context
	// cancellation function is only available after the flow is Start()'ed.
	cancelFlow func() context.CancelFunc
}

var flowCoordinatorPool = sync.Pool{
	New: func() interface{} {
		return &FlowCoordinator{}
	},
}

// NewFlowCoordinator creates a new FlowCoordinator processor that is the root
// of the vectorized flow.
// - cancelFlow should return the context cancellation function that cancels
// the context of the flow (i.e. it is Flow.ctxCancel).
func NewFlowCoordinator(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
	cancelFlow func() context.CancelFunc,
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
		f.cancelFlow()()
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
}
