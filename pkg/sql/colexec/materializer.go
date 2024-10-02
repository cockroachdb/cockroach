// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execreleasable"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// Materializer converts an Operator input into a execinfra.RowSource.
type Materializer struct {
	execinfra.ProcessorBaseNoHelper
	colexecop.NonExplainable

	input colexecop.Operator
	typs  []*types.T

	drainHelper drainHelper

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

	// streamingMemAcc can be nil in tests.
	streamingMemAcc      *mon.BoundAccount
	metadataAccountedFor int64

	statsCollectors []colexecop.VectorizedStatsCollector
	sources         colexecop.MetadataSources

	drained bool
	meta    []execinfrapb.ProducerMetadata
}

var _ execinfra.RowSource = &drainHelper{}

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
	if !d.drained {
		d.meta = d.sources.DrainMeta()
		d.drained = true
		if d.streamingMemAcc != nil {
			d.metadataAccountedFor = colexecutils.AccountForMetadata(d.ctx, d.streamingMemAcc, d.meta)
		}
	}
	if len(d.meta) == 0 {
		// Eagerly lose the reference to the slice.
		d.meta = nil
		if d.streamingMemAcc != nil {
			// At this point, the caller took over the metadata, so we can
			// release the allocations.
			d.streamingMemAcc.Shrink(d.ctx, d.metadataAccountedFor)
			d.metadataAccountedFor = 0
		}
		return nil, nil
	}
	meta := d.meta[0]
	d.meta = d.meta[1:]
	return nil, &meta
}

// ConsumerDone implements the execinfra.RowSource interface.
func (d *drainHelper) ConsumerDone() {}

// ConsumerClosed implements the execinfra.RowSource interface.
func (d *drainHelper) ConsumerClosed() {}

var materializerPool = sync.Pool{
	New: func() interface{} {
		return &Materializer{}
	},
}

// NewMaterializer creates a new Materializer processor which processes the
// columnar data coming from input to return it as rows.
// Arguments:
// - streamingMemAcc can be nil in tests.
// - typs is the output types schema. Typs are assumed to have been hydrated.
// NOTE: the constructor does *not* take in an execinfrapb.PostProcessSpec
// because we expect input to handle that for us.
func NewMaterializer(
	streamingMemAcc *mon.BoundAccount,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input colexecargs.OpWithMetaInfo,
	typs []*types.T,
) *Materializer {
	m := materializerPool.Get().(*Materializer)
	*m = Materializer{
		ProcessorBaseNoHelper: m.ProcessorBaseNoHelper,
		input:                 input.Root,
		typs:                  typs,
		converter:             colconv.NewAllVecToDatumConverter(len(typs)),
		row:                   m.row,
	}
	if cap(m.row) >= len(typs) && cap(m.row) > 0 {
		// The latter part of the condition is needed in order to distinguish
		// between a row with no columns and no rows.
		m.row = m.row[:len(typs)]
	} else {
		m.row = make(rowenc.EncDatumRow, len(typs))
	}
	m.drainHelper.streamingMemAcc = streamingMemAcc
	m.drainHelper.statsCollectors = input.StatsCollectors
	m.drainHelper.sources = input.MetadataSources

	m.Init(
		m,
		flowCtx,
		processorID,
		execinfra.ProcStateOpts{
			// We append drainHelper to inputs to drain below in order to reuse
			// the same underlying slice from the pooled materializer. The
			// drainHelper is responsible for draining the metadata from the
			// input tree.
		},
	)
	m.AddInputToDrain(&m.drainHelper)
	return m
}

var _ execopnode.OpNode = &Materializer{}
var _ execinfra.Processor = &Materializer{}
var _ execreleasable.Releasable = &Materializer{}

// ChildCount is part of the execopnode.OpNode interface.
func (m *Materializer) ChildCount(verbose bool) int {
	return 1
}

// Child is part of the execopnode.OpNode interface.
func (m *Materializer) Child(nth int, verbose bool) execopnode.OpNode {
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
	ctx = m.StartInternal(ctx, "materializer" /* name */)
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

// Release implements the execinfra.Releasable interface.
func (m *Materializer) Release() {
	m.ProcessorBaseNoHelper.Reset()
	m.converter.Release()
	for i := range m.row {
		m.row[i] = rowenc.EncDatum{}
	}
	*m = Materializer{
		// We're keeping the reference to the same ProcessorBaseNoHelper since
		// it allows us to reuse some of the slices.
		ProcessorBaseNoHelper: m.ProcessorBaseNoHelper,
		row:                   m.row[:0],
	}
	materializerPool.Put(m)
}
