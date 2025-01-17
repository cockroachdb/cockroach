// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execreleasable"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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

	mode            columnarizerMode
	initialized     bool
	helper          colmem.SetAccountingHelper
	streamingMemAcc *mon.BoundAccount
	// metadataAccountedFor tracks how much memory has been reserved in the
	// streamingMemAcc for the metadata.
	metadataAccountedFor int64
	input                execinfra.RowSource
	da                   tree.DatumAlloc
	// getWrappedExecStats, if non-nil, is the function to get the execution
	// statistics of the wrapped row-by-row processor. We store it separately
	// from execinfra.ProcessorBaseNoHelper.ExecStatsForTrace so that the
	// function is not called when the columnarizer is being drained (which is
	// after the vectorized stats are processed).
	getWrappedExecStats func() *execinfrapb.ComponentStats

	batch           coldata.Batch
	vecs            coldata.TypedVecs
	accumulatedMeta []execinfrapb.ProducerMetadata
	typs            []*types.T

	// removedFromFlow marks this Columnarizer as having been removed from the
	// flow. This renders all future calls to Init, Next, Close, and DrainMeta
	// noops.
	removedFromFlow bool
}

var _ colexecop.DrainableClosableOperator = &Columnarizer{}
var _ colexecop.VectorizedStatsCollector = &Columnarizer{}
var _ execreleasable.Releasable = &Columnarizer{}

// NewBufferingColumnarizer returns a new Columnarizer that will be buffering up
// rows before emitting them as output batches.
// - batchAllocator must use the memory account that is not shared with any
// other user.
func NewBufferingColumnarizer(
	batchAllocator *colmem.Allocator,
	streamingMemAcc *mon.BoundAccount,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
) *Columnarizer {
	return newColumnarizer(batchAllocator, streamingMemAcc, flowCtx, processorID, input, columnarizerBufferingMode)
}

// NewBufferingColumnarizerForTests is a convenience wrapper around
// NewBufferingColumnarizer to be used in tests (when we don't care about the
// memory accounting).
func NewBufferingColumnarizerForTests(
	allocator *colmem.Allocator,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
) *Columnarizer {
	return NewBufferingColumnarizer(allocator, allocator.Acc(), flowCtx, processorID, input)
}

// NewStreamingColumnarizer returns a new Columnarizer that emits every input
// row as a separate batch.
// - batchAllocator must use the memory account that is not shared with any
// other user.
func NewStreamingColumnarizer(
	batchAllocator *colmem.Allocator,
	streamingMemAcc *mon.BoundAccount,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
) *Columnarizer {
	return newColumnarizer(batchAllocator, streamingMemAcc, flowCtx, processorID, input, columnarizerStreamingMode)
}

var columnarizerPool = sync.Pool{
	New: func() interface{} {
		return &Columnarizer{}
	},
}

// newColumnarizer returns a new Columnarizer.
func newColumnarizer(
	batchAllocator *colmem.Allocator,
	streamingMemAcc *mon.BoundAccount,
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
	c := columnarizerPool.Get().(*Columnarizer)
	*c = Columnarizer{
		ProcessorBaseNoHelper: c.ProcessorBaseNoHelper,
		streamingMemAcc:       streamingMemAcc,
		input:                 input,
		mode:                  mode,
	}
	c.ProcessorBaseNoHelper.Init(
		nil, /* self */
		flowCtx,
		processorID,
		execinfra.ProcStateOpts{
			// We append input to inputs to drain below in order to reuse the same
			// underlying slice from the pooled columnarizer.
			TrailingMetaCallback: c.trailingMetaCallback,
		},
	)
	c.AddInputToDrain(input)
	c.typs = c.input.OutputTypes()
	c.helper.Init(batchAllocator, execinfra.GetWorkMemLimit(flowCtx), c.typs, false /* alwaysReallocate */)
	return c
}

// Init is part of the colexecop.Operator interface.
func (c *Columnarizer) Init(ctx context.Context) {
	if c.removedFromFlow || c.initialized {
		return
	}
	c.initialized = true
	c.accumulatedMeta = make([]execinfrapb.ProducerMetadata, 0, 1)
	ctx = c.StartInternal(ctx, "columnarizer" /* name */)
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
		c.getWrappedExecStats = execStatsHijacker.HijackExecStatsForTrace()
	}
}

// GetStats is part of the colexecop.VectorizedStatsCollector interface.
func (c *Columnarizer) GetStats() *execinfrapb.ComponentStats {
	if c.removedFromFlow && buildutil.CrdbTestBuild {
		colexecerror.InternalError(errors.AssertionFailedf(
			"unexpectedly the columnarizer was removed from the flow when stats are being collected",
		))
	}
	componentID := c.FlowCtx.ProcessorComponentID(c.ProcessorID)
	if c.removedFromFlow || c.getWrappedExecStats == nil {
		return &execinfrapb.ComponentStats{Component: componentID}
	}
	s := c.getWrappedExecStats()
	if s == nil {
		return &execinfrapb.ComponentStats{Component: componentID}
	}
	s.Component = componentID
	return s
}

// IsFastPathNode returns true if the provided RowSource is the
// planNodeToRowSource wrapper with "fast-path" enabled. The logic is injected
// in the sql package to avoid an import cycle.
var IsFastPathNode func(source execinfra.RowSource) bool

// IsColumnarizerAroundFastPathNode returns true if the provided Operator is a
// Columnarizer that has "fast-path" enabled planNodeToRowSource wrapper as the
// input.
func IsColumnarizerAroundFastPathNode(o colexecop.Operator) bool {
	o = MaybeUnwrapInvariantsChecker(o)
	c, ok := o.(*Columnarizer)
	return ok && IsFastPathNode(c.input)
}

// Next is part of the colexecop.Operator interface.
func (c *Columnarizer) Next() coldata.Batch {
	if c.removedFromFlow {
		return coldata.ZeroBatch
	}
	var reallocated bool
	var tuplesToBeSet int
	if c.mode == columnarizerStreamingMode {
		// In the streaming mode, we always emit a batch with at most one tuple,
		// so we say that there is just one tuple left to be set (even though we
		// might end up emitting more tuples - it's ok from the point of view of
		// the helper).
		tuplesToBeSet = 1
	}
	c.batch, reallocated = c.helper.ResetMaybeReallocate(c.typs, c.batch, tuplesToBeSet)
	if reallocated {
		c.vecs.SetBatch(c.batch)
	}
	nRows := 0
	for batchDone := false; !batchDone; {
		row, meta := c.input.Next()
		if meta != nil {
			if meta.Err != nil {
				// If an error occurs, return it immediately.
				colexecerror.ExpectedError(meta.Err)
			}
			c.accumulatedMeta = append(c.accumulatedMeta, *meta)
			c.metadataAccountedFor += colexecutils.AccountForMetadata(c.Ctx(), c.streamingMemAcc, c.accumulatedMeta[len(c.accumulatedMeta)-1:])
			continue
		}
		if row == nil {
			break
		}
		EncDatumRowToColVecs(row, nRows, c.vecs, c.typs, &c.da)
		batchDone = c.helper.AccountForSet(nRows)
		nRows++
	}

	c.batch.SetLength(nRows)
	return c.batch
}

// DrainMeta is part of the colexecop.MetadataSource interface.
func (c *Columnarizer) DrainMeta() []execinfrapb.ProducerMetadata {
	if c.removedFromFlow {
		return nil
	}
	// We no longer need the batch.
	c.batch = nil
	c.helper.ReleaseMemory()
	bufferedMeta := c.accumulatedMeta
	// Eagerly lose the reference to the metadata since it might be of
	// non-trivial footprint.
	c.accumulatedMeta = nil
	defer func() {
		c.streamingMemAcc.Shrink(c.Ctx(), c.metadataAccountedFor)
		c.metadataAccountedFor = 0
	}()
	if !c.initialized {
		// The columnarizer wasn't initialized, so the wrapped processors might
		// not have been started leaving them in an unsafe to drain state, so
		// we skip the draining. Mostly likely this happened because a panic was
		// encountered in Init.
		return bufferedMeta
	}
	c.MoveToDraining(nil /* err */)
	for {
		meta := c.DrainHelper()
		if meta == nil {
			break
		}
		bufferedMeta = append(bufferedMeta, *meta)
	}
	return bufferedMeta
}

// Close is part of the colexecop.ClosableOperator interface.
func (c *Columnarizer) Close(context.Context) error {
	if c.removedFromFlow {
		return nil
	}
	c.helper.Release()
	c.InternalClose()
	return nil
}

func (c *Columnarizer) trailingMetaCallback() []execinfrapb.ProducerMetadata {
	// Close will call InternalClose(). Note that we don't return any trailing
	// metadata here because the columnarizers propagate it in DrainMeta.
	if err := c.Close(c.Ctx()); buildutil.CrdbTestBuild && err != nil {
		// Close never returns an error.
		colexecerror.InternalError(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error from Columnarizer.Close"))
	}
	return nil
}

// Release releases this Columnarizer back to the pool.
func (c *Columnarizer) Release() {
	c.ProcessorBaseNoHelper.Reset()
	*c = Columnarizer{ProcessorBaseNoHelper: c.ProcessorBaseNoHelper}
	columnarizerPool.Put(c)
}

// ChildCount is part of the execopnode.OpNode interface.
func (c *Columnarizer) ChildCount(verbose bool) int {
	if _, ok := c.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (c *Columnarizer) Child(nth int, verbose bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := c.input.(execopnode.OpNode); ok {
			return n
		}
		colexecerror.InternalError(errors.AssertionFailedf("input to Columnarizer is not an execopnode.OpNode"))
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
