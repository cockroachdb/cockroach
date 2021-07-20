// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// columnarizerMode indicates the mode of operation of the Columnarizer.
type columnarizerMode int

const (
	// columnarizerBufferingMode is the mode of operation in which the
	// Columnarizer will be buffering up rows (dynamically, up to
	// coldata.BatchSize()) before emitting the output batch.
	// TODO(jordan): evaluate whether it's more efficient to skip the buffer
	// phase.
	columnarizerBufferingMode columnarizerMode = iota
	// columnarizerStreamingMode is the mode of operation in which the
	// Columnarizer will always emit batches with a single tuple (until it is
	// done).
	columnarizerStreamingMode
)

// Columnarizer turns an execinfra.RowSource input into an Operator output, by
// reading the input in chunks of size coldata.BatchSize() and converting each
// chunk into a coldata.Batch column by column.
type Columnarizer struct {
	// Note that we consciously don't embed a colexecop.InitHelper here because
	// we currently rely on the ProcessorBase to provide the same (and more)
	// functionality.
	// TODO(yuzefovich): consider whether embedding ProcessorBaseNoHelper into
	// the columnarizers makes sense.
	execinfra.ProcessorBaseNoHelper
	colexecop.NonExplainable

	mode      columnarizerMode
	allocator *colmem.Allocator
	input     execinfra.RowSource
	da        rowenc.DatumAlloc

	buffered        rowenc.EncDatumRows
	batch           coldata.Batch
	maxBatchMemSize int64
	accumulatedMeta []execinfrapb.ProducerMetadata
	typs            []*types.T

	// removedFromFlow marks this Columnarizer as having been removed from the
	// flow. This renders all future calls to Init, Next, Close, and DrainMeta
	// noops.
	removedFromFlow bool
}

var _ colexecop.Operator = &Columnarizer{}
var _ colexecop.VectorizedStatsCollector = &Columnarizer{}

// NewBufferingColumnarizer returns a new Columnarizer that will be buffering up
// rows before emitting them as output batches.
func NewBufferingColumnarizer(
	allocator *colmem.Allocator,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
) *Columnarizer {
	return newColumnarizer(allocator, flowCtx, processorID, input, columnarizerBufferingMode)
}

// NewStreamingColumnarizer returns a new Columnarizer that emits every input
// row as a separate batch.
func NewStreamingColumnarizer(
	allocator *colmem.Allocator,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
) *Columnarizer {
	return newColumnarizer(allocator, flowCtx, processorID, input, columnarizerStreamingMode)
}

// newColumnarizer returns a new Columnarizer.
func newColumnarizer(
	allocator *colmem.Allocator,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
	mode columnarizerMode,
) *Columnarizer {
	switch mode {
	case columnarizerBufferingMode, columnarizerStreamingMode:
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unexpected columnarizerMode %d", mode))
	}
	c := &Columnarizer{
		allocator:       allocator,
		input:           input,
		maxBatchMemSize: execinfra.GetWorkMemLimit(flowCtx),
		mode:            mode,
	}
	c.ProcessorBaseNoHelper.Init(
		nil, /* self */
		flowCtx,
		flowCtx.EvalCtx,
		processorID,
		nil, /* output */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{input},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				// Close will call InternalClose(). Note that we don't return
				// any trailing metadata here because the columnarizers
				// propagate it in DrainMeta.
				if err := c.Close(); util.CrdbTestBuild && err != nil {
					// Close never returns an error.
					colexecerror.InternalError(errors.AssertionFailedf("unexpected error %v from Columnarizer.Close", err))
				}
				return nil
			}},
	)
	c.typs = c.input.OutputTypes()
	return c
}

// Init is part of the colexecop.Operator interface.
func (c *Columnarizer) Init(ctx context.Context) {
	if c.removedFromFlow {
		return
	}
	if c.Ctx != nil {
		// Init has already been called.
		return
	}
	c.accumulatedMeta = make([]execinfrapb.ProducerMetadata, 0, 1)
	ctx = c.StartInternalNoSpan(ctx)
	c.input.Start(ctx)
	if execStatsHijacker, ok := c.input.(execinfra.ExecStatsForTraceHijacker); ok {
		// The columnarizer is now responsible for propagating the execution
		// stats of the wrapped processor.
		//
		// Note that this columnarizer cannot be removed from the flow because
		// it will have a vectorized stats collector planned on top, so the
		// optimization of wrapRowSources() in execplan.go will never trigger.
		// We check this assumption with an assertion below in the test setting.
		//
		// Still, just to be safe, we delay the hijacking until Init so that in
		// case the assumption is wrong, we still get the stats from the wrapped
		// processor.
		c.ExecStatsForTrace = execStatsHijacker.HijackExecStatsForTrace()
	}
}

// GetStats is part of the colexecop.VectorizedStatsCollector interface.
func (c *Columnarizer) GetStats() *execinfrapb.ComponentStats {
	if c.removedFromFlow && util.CrdbTestBuild {
		colexecerror.InternalError(errors.AssertionFailedf(
			"unexpectedly the columnarizer was removed from the flow when stats are being collected",
		))
	}
	componentID := c.FlowCtx.ProcessorComponentID(c.ProcessorID)
	if c.removedFromFlow || c.ExecStatsForTrace == nil {
		return &execinfrapb.ComponentStats{Component: componentID}
	}
	s := c.ExecStatsForTrace()
	s.Component = componentID
	return s
}

// Next is part of the colexecop.Operator interface.
func (c *Columnarizer) Next() coldata.Batch {
	if c.removedFromFlow {
		return coldata.ZeroBatch
	}
	var reallocated bool
	switch c.mode {
	case columnarizerBufferingMode:
		c.batch, reallocated = c.allocator.ResetMaybeReallocate(
			c.typs, c.batch, 1 /* minCapacity */, c.maxBatchMemSize,
		)
	case columnarizerStreamingMode:
		// Note that we're not using ResetMaybeReallocate because we will
		// always have at most one tuple in the batch.
		if c.batch == nil {
			c.batch = c.allocator.NewMemBatchWithFixedCapacity(c.typs, 1 /* minCapacity */)
			reallocated = true
		} else {
			c.batch.ResetInternalBatch()
		}
	}
	if reallocated {
		oldRows := c.buffered
		newRows := make(rowenc.EncDatumRows, c.batch.Capacity())
		_ = newRows[len(oldRows)]
		for i := 0; i < len(oldRows); i++ {
			//gcassert:bce
			newRows[i] = oldRows[i]
		}
		for i := len(oldRows); i < len(newRows); i++ {
			//gcassert:bce
			newRows[i] = make(rowenc.EncDatumRow, len(c.typs))
		}
		c.buffered = newRows
	}
	// Buffer up rows up to the capacity of the batch.
	nRows := 0
	for ; nRows < c.batch.Capacity(); nRows++ {
		row, meta := c.input.Next()
		if meta != nil {
			nRows--
			if meta.Err != nil {
				// If an error occurs, return it immediately.
				colexecerror.ExpectedError(meta.Err)
			}
			c.accumulatedMeta = append(c.accumulatedMeta, *meta)
			continue
		}
		if row == nil {
			break
		}
		copy(c.buffered[nRows], row)
	}

	// Check if we have buffered more rows than the current allocation size
	// and increase it if so.
	if nRows > c.da.AllocSize {
		c.da.AllocSize = nRows
	}

	// Write each column into the output batch.
	outputRows := c.buffered[:nRows]
	for idx, ct := range c.typs {
		err := EncDatumRowsToColVec(c.allocator, outputRows, c.batch.ColVec(idx), idx, ct, &c.da)
		if err != nil {
			colexecerror.InternalError(err)
		}
	}
	c.batch.SetLength(nRows)
	return c.batch
}

var (
	_ colexecop.DrainableOperator = &Columnarizer{}
	_ colexecop.Closer            = &Columnarizer{}
)

// DrainMeta is part of the colexecop.MetadataSource interface.
func (c *Columnarizer) DrainMeta() []execinfrapb.ProducerMetadata {
	if c.removedFromFlow {
		return nil
	}
	if c.Ctx == nil {
		// The columnarizer wasn't initialized, so the wrapped processors might
		// not have been started leaving them in an unsafe to drain state, so
		// we skip the draining. Mostly likely this happened because a panic was
		// encountered in Init.
		return c.accumulatedMeta
	}
	c.MoveToDraining(nil /* err */)
	for {
		meta := c.DrainHelper()
		if meta == nil {
			break
		}
		c.accumulatedMeta = append(c.accumulatedMeta, *meta)
	}
	return c.accumulatedMeta
}

// Close is part of the colexecop.ClosableOperator interface.
func (c *Columnarizer) Close() error {
	if c.removedFromFlow {
		return nil
	}
	c.InternalClose()
	return nil
}

// ChildCount is part of the execinfra.OpNode interface.
func (c *Columnarizer) ChildCount(verbose bool) int {
	if _, ok := c.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (c *Columnarizer) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := c.input.(execinfra.OpNode); ok {
			return n
		}
		colexecerror.InternalError(errors.AssertionFailedf("input to Columnarizer is not an execinfra.OpNode"))
	}
	colexecerror.InternalError(errors.AssertionFailedf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// Input returns the input of this columnarizer.
func (c *Columnarizer) Input() execinfra.RowSource {
	return c.input
}

// MarkAsRemovedFromFlow is called by planning code to make all future calls on
// this columnarizer noops. It exists to support an execution optimization where
// a Columnarizer is removed from a flow in cases where it would be the input to
// a Materializer (which is redundant). Simply bypassing the Columnarizer is not
// enough because it is added to a slice of Closers and MetadataSources that are
// difficult to change once physical planning moves on from the Columnarizer.
func (c *Columnarizer) MarkAsRemovedFromFlow() {
	c.removedFromFlow = true
}
