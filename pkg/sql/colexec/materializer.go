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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// Materializer converts an Operator input into a execinfra.RowSource.
type Materializer struct {
	execinfra.ProcessorBaseNoHelper
	colexecop.NonExplainable

	input colexecop.Operator
	typs  []*types.T

	drainHelper *drainHelper

	// runtime fields --

	// curIdx represents the current index into the column batch: the next row
	// the Materializer will emit.
	curIdx int
	// batch is the current Batch the Materializer is processing.
	batch coldata.Batch
	// converter contains the converted vectors of the current batch. Note that
	// if the batch had a selection vector on top of it, the converted vectors
	// will be "dense" and contain only tuples that were selected.
	converter *colconv.VecToDatumConverter

	// row is the memory used for the output row.
	row rowenc.EncDatumRow

	// outputRow stores the returned results of next() to be passed through an
	// adapter.
	outputRow rowenc.EncDatumRow

	// closers is a slice of Closers that should be Closed on termination.
	closers colexecop.Closers
}

// drainHelper is a utility struct that wraps MetadataSources in a RowSource
// interface. This is done so that the Materializer can drain MetadataSources
// in the vectorized input tree as inputs, rather than draining them in the
// trailing metadata state, which is meant only for internal metadata
// generation.
type drainHelper struct {
	// If unset, the drainHelper wasn't Start()'ed, so all operations on it
	// are noops.
	ctx context.Context

	statsCollectors []colexecop.VectorizedStatsCollector
	sources         colexecop.MetadataSources

	bufferedMeta []execinfrapb.ProducerMetadata
}

var _ execinfra.RowSource = &drainHelper{}
var _ execinfra.Releasable = &drainHelper{}

var drainHelperPool = sync.Pool{
	New: func() interface{} {
		return &drainHelper{}
	},
}

func newDrainHelper(
	statsCollectors []colexecop.VectorizedStatsCollector, sources colexecop.MetadataSources,
) *drainHelper {
	d := drainHelperPool.Get().(*drainHelper)
	d.statsCollectors = statsCollectors
	d.sources = sources
	return d
}

// OutputTypes implements the execinfra.RowSource interface.
func (d *drainHelper) OutputTypes() []*types.T {
	colexecerror.InternalError(errors.AssertionFailedf("unimplemented"))
	// Unreachable code.
	return nil
}

// Start implements the execinfra.RowSource interface.
func (d *drainHelper) Start(ctx context.Context) {
	d.ctx = ctx
}

// Next implements the execinfra.RowSource interface.
func (d *drainHelper) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if d.ctx == nil {
		// The drainHelper wasn't Start()'ed, so this operation is a noop.
		return nil, nil
	}
	if len(d.statsCollectors) > 0 {
		// If statsCollectors is non-nil, then the drainHelper is responsible
		// for attaching the execution statistics to the span. Note that we
		// neither retrieve the trace from the span (via sp.GetRecording()) nor
		// propagate the trace as a metadata here - that is left to the
		// materializer (more precisely, to the embedded ProcessorBase) which is
		// necessary in order to not collect same trace data twice.
		if sp := tracing.SpanFromContext(d.ctx); sp != nil {
			for _, s := range d.statsCollectors {
				sp.RecordStructured(s.GetStats())
			}
		}
		d.statsCollectors = nil
	}
	if d.bufferedMeta == nil {
		d.bufferedMeta = d.sources.DrainMeta()
		if d.bufferedMeta == nil {
			// Still nil, avoid more calls to DrainMeta.
			d.bufferedMeta = []execinfrapb.ProducerMetadata{}
		}
	}
	if len(d.bufferedMeta) == 0 {
		return nil, nil
	}
	meta := d.bufferedMeta[0]
	d.bufferedMeta = d.bufferedMeta[1:]
	return nil, &meta
}

// ConsumerDone implements the execinfra.RowSource interface.
func (d *drainHelper) ConsumerDone() {}

// ConsumerClosed implements the execinfra.RowSource interface.
func (d *drainHelper) ConsumerClosed() {}

// Release implements the execinfra.Releasable interface.
func (d *drainHelper) Release() {
	*d = drainHelper{}
	drainHelperPool.Put(d)
}

var materializerPool = sync.Pool{
	New: func() interface{} {
		return &Materializer{}
	},
}

// NewMaterializer creates a new Materializer processor which processes the
// columnar data coming from input to return it as rows.
// Arguments:
// - typs is the output types schema. Typs are assumed to have been hydrated.
// - getStats (when tracing is enabled) returns all of the execution statistics
// of operators which the materializer is responsible for.
// NOTE: the constructor does *not* take in an execinfrapb.PostProcessSpec
// because we expect input to handle that for us.
func NewMaterializer(
	flowCtx *execinfra.FlowCtx, processorID int32, input colexecargs.OpWithMetaInfo, typs []*types.T,
) *Materializer {
	m := materializerPool.Get().(*Materializer)
	*m = Materializer{
		ProcessorBaseNoHelper: m.ProcessorBaseNoHelper,
		input:                 input.Root,
		typs:                  typs,
		drainHelper:           newDrainHelper(input.StatsCollectors, input.MetadataSources),
		converter:             colconv.NewAllVecToDatumConverter(len(typs)),
		row:                   make(rowenc.EncDatumRow, len(typs)),
		// We have to perform a deep copy of closers because the input object
		// might be released before the materializer is closed.
		// TODO(yuzefovich): improve this. It will require untangling of
		// planTop.close and the row sources pointed to by the plan via
		// rowSourceToPlanNode wrappers.
		closers: append(m.closers[:0], input.ToClose...),
	}

	m.Init(
		m,
		flowCtx,
		// Materializer doesn't modify the eval context, so it is safe to reuse
		// the one from the flow context.
		flowCtx.EvalCtx,
		processorID,
		nil, /* output */
		execinfra.ProcStateOpts{
			// We append drainHelper to inputs to drain below in order to reuse
			// the same underlying slice from the pooled materializer.
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				// Note that we delegate draining all of the metadata sources
				// to drainHelper which is added as an input to drain below.
				m.close()
				return nil
			},
		},
	)
	m.AddInputToDrain(m.drainHelper)
	return m
}

var _ execinfra.OpNode = &Materializer{}
var _ execinfra.Processor = &Materializer{}
var _ execinfra.Releasable = &Materializer{}

// ChildCount is part of the execinfra.OpNode interface.
func (m *Materializer) ChildCount(verbose bool) int {
	return 1
}

// Child is part of the execinfra.OpNode interface.
func (m *Materializer) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		return m.input
	}
	colexecerror.InternalError(errors.AssertionFailedf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// OutputTypes is part of the execinfra.Processor interface.
func (m *Materializer) OutputTypes() []*types.T {
	return m.typs
}

// Start is part of the execinfra.RowSource interface.
func (m *Materializer) Start(ctx context.Context) {
	ctx = m.StartInternalNoSpan(ctx)
	// We can encounter an expected error during Init (e.g. an operator
	// attempts to allocate a batch, but the memory budget limit has been
	// reached), so we need to wrap it with a catcher.
	if err := colexecerror.CatchVectorizedRuntimeError(func() {
		m.input.Init(ctx)
	}); err != nil {
		m.MoveToDraining(err)
	} else {
		// Note that we intentionally only start the drain helper if
		// initialization was successful - not starting the helper will tell it
		// to not drain the metadata sources (which have not been properly
		// initialized).
		m.drainHelper.Start(ctx)
	}
}

// next is the logic of Next() extracted in a separate method to be used by an
// adapter to be able to wrap the latter with a catcher. nil is returned when a
// zero-length batch is encountered.
func (m *Materializer) next() rowenc.EncDatumRow {
	if m.batch == nil || m.curIdx >= m.batch.Length() {
		// Get a fresh batch.
		m.batch = m.input.Next()
		if m.batch.Length() == 0 {
			return nil
		}
		m.curIdx = 0
		m.converter.ConvertBatchAndDeselect(m.batch)
	}

	for colIdx := range m.typs {
		// Note that we don't need to apply the selection vector of the
		// batch to index m.curIdx because vecToDatumConverter returns a
		// "dense" datum column.
		m.row[colIdx].Datum = m.converter.GetDatumColumn(colIdx)[m.curIdx]
	}
	m.curIdx++
	// Note that there is no post-processing to be done in the
	// materializer, so we do not use ProcessRowHelper and emit the row
	// directly.
	return m.row
}

// nextAdapter calls next() and saves the returned results in m. For internal
// use only. The purpose of having this function is to not create an anonymous
// function on every call to Next().
func (m *Materializer) nextAdapter() {
	m.outputRow = m.next()
}

// Next is part of the execinfra.RowSource interface.
func (m *Materializer) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for m.State == execinfra.StateRunning {
		if err := colexecerror.CatchVectorizedRuntimeError(m.nextAdapter); err != nil {
			m.MoveToDraining(err)
			continue
		}
		if m.outputRow == nil {
			// Zero-length batch was encountered, move to draining.
			m.MoveToDraining(nil /* err */)
			continue
		}
		return m.outputRow, nil
	}
	// Forward any metadata.
	return nil, m.DrainHelper()
}

func (m *Materializer) close() {
	if m.InternalClose() {
		if m.Ctx == nil {
			// In some edge cases (like when Init of an operator above this
			// materializer encounters a panic), the materializer might never be
			// started, yet it still will attempt to close its Closers. This
			// context is only used for logging purposes, so it is ok to grab
			// the background context in order to prevent a NPE below.
			m.Ctx = context.Background()
		}
		m.closers.CloseAndLogOnErr(m.Ctx, "materializer")
	}
}

// ConsumerClosed is part of the execinfra.RowSource interface.
func (m *Materializer) ConsumerClosed() {
	m.close()
}

// Release implements the execinfra.Releasable interface.
func (m *Materializer) Release() {
	m.drainHelper.Release()
	m.ProcessorBaseNoHelper.Reset()
	m.converter.Release()
	for i := range m.closers {
		m.closers[i] = nil
	}
	*m = Materializer{
		// We're keeping the reference to the same ProcessorBaseNoHelper since
		// it allows us to reuse some of the slices.
		ProcessorBaseNoHelper: m.ProcessorBaseNoHelper,
		closers:               m.closers[:0],
	}
	materializerPool.Put(m)
}
