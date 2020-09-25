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
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Materializer converts an Operator input into a execinfra.RowSource.
type Materializer struct {
	execinfra.ProcessorBase
	NonExplainable

	input colexecbase.Operator
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

	// ouputRow stores the returned results of next() to be passed through an
	// adapter.
	outputRow rowenc.EncDatumRow

	// cancelFlow will return a function to cancel the context of the flow. It is
	// a function in order to be lazily evaluated, since the context cancellation
	// function is only available when Starting. This function differs from
	// ctxCancel in that it will cancel all components of the Materializer's flow,
	// including those started asynchronously.
	cancelFlow func() context.CancelFunc

	// closers is a slice of Closers that should be Closed on termination.
	closers Closers
}

// drainHelper is a utility struct that wraps MetadataSources in a RowSource
// interface. This is done so that the Materializer can drain MetadataSources
// in the vectorized input tree as inputs, rather than draining them in the
// trailing metadata state, which is meant only for internal metadata
// generation.
type drainHelper struct {
	execinfrapb.MetadataSources
	ctx          context.Context
	bufferedMeta []execinfrapb.ProducerMetadata
}

var _ execinfra.RowSource = &drainHelper{}

func newDrainHelper(sources execinfrapb.MetadataSources) *drainHelper {
	return &drainHelper{
		MetadataSources: sources,
	}
}

// OutputTypes implements the RowSource interface.
func (d *drainHelper) OutputTypes() []*types.T {
	colexecerror.InternalError(errors.AssertionFailedf("unimplemented"))
	// Unreachable code.
	return nil
}

// Start implements the RowSource interface.
func (d *drainHelper) Start(ctx context.Context) context.Context {
	d.ctx = ctx
	return ctx
}

// Next implements the RowSource interface.
func (d *drainHelper) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if d.bufferedMeta == nil {
		d.bufferedMeta = d.DrainMeta(d.ctx)
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

// ConsumerDone implements the RowSource interface.
func (d *drainHelper) ConsumerDone() {}

// ConsumerClosed implements the RowSource interface.
func (d *drainHelper) ConsumerClosed() {}

const materializerProcName = "materializer"

// NewMaterializer creates a new Materializer processor which processes the
// columnar data coming from input to return it as rows.
// Arguments:
// - typs is the output types scheme.
// - metadataSourcesQueue are all of the metadata sources that are planned on
// the same node as the Materializer and that need to be drained.
// - outputStatsToTrace (when tracing is enabled) finishes the stats.
// - cancelFlow should return the context cancellation function that cancels
// the context of the flow (i.e. it is Flow.ctxCancel). It should only be
// non-nil in case of a root Materializer (i.e. not when we're wrapping a row
// source).
// NOTE: the constructor does *not* take in an execinfrapb.PostProcessSpec
// because we expect input to handle that for us.
func NewMaterializer(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input colexecbase.Operator,
	typs []*types.T,
	output execinfra.RowReceiver,
	metadataSourcesQueue []execinfrapb.MetadataSource,
	toClose []Closer,
	outputStatsToTrace func(),
	cancelFlow func() context.CancelFunc,
) (*Materializer, error) {
	vecIdxsToConvert := make([]int, len(typs))
	for i := range vecIdxsToConvert {
		vecIdxsToConvert[i] = i
	}
	m := &Materializer{
		input:       input,
		typs:        typs,
		drainHelper: newDrainHelper(metadataSourcesQueue),
		converter:   colconv.NewVecToDatumConverter(len(typs), vecIdxsToConvert),
		row:         make(rowenc.EncDatumRow, len(typs)),
		closers:     toClose,
	}

	if err := m.ProcessorBase.Init(
		m,
		// input must have handled any post-processing itself, so we pass in
		// an empty post-processing spec.
		&execinfrapb.PostProcessSpec{},
		typs,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{m.drainHelper},
			TrailingMetaCallback: func(ctx context.Context) []execinfrapb.ProducerMetadata {
				m.InternalClose()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	m.FinishTrace = outputStatsToTrace
	m.cancelFlow = cancelFlow
	return m, nil
}

var _ execinfra.OpNode = &Materializer{}

// ChildCount is part of the exec.OpNode interface.
func (m *Materializer) ChildCount(verbose bool) int {
	return 1
}

// Child is part of the exec.OpNode interface.
func (m *Materializer) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		return m.input
	}
	colexecerror.InternalError(errors.AssertionFailedf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// Start is part of the execinfra.RowSource interface.
func (m *Materializer) Start(ctx context.Context) context.Context {
	m.input.Init()
	ctx = m.drainHelper.Start(ctx)
	return m.ProcessorBase.StartInternal(ctx, materializerProcName)
}

// next is the logic of Next() extracted in a separate method to be used by an
// adapter to be able to wrap the latter with a catcher. nil is returned when a
// zero-length batch is encountered.
func (m *Materializer) next() rowenc.EncDatumRow {
	if m.batch == nil || m.curIdx >= m.batch.Length() {
		// Get a fresh batch.
		m.batch = m.input.Next(m.Ctx)
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

// InternalClose helps implement the execinfra.RowSource interface.
func (m *Materializer) InternalClose() bool {
	if m.ProcessorBase.InternalClose() {
		if m.cancelFlow != nil {
			m.cancelFlow()()
		}
		m.closers.CloseAndLogOnErr(m.Ctx, "materializer")
		return true
	}
	return false
}

// ConsumerDone is part of the execinfra.RowSource interface.
func (m *Materializer) ConsumerDone() {
	// Materializer will move into 'draining' state, and after all the metadata
	// has been drained - as part of TrailingMetaCallback - InternalClose() will
	// be called which will cancel the flow.
	m.MoveToDraining(nil /* err */)
}

// ConsumerClosed is part of the execinfra.RowSource interface.
func (m *Materializer) ConsumerClosed() {
	m.InternalClose()
}
