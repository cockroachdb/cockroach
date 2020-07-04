// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type BatchSource struct {
	execinfra.ProcessorBase
	OneInputNode
	NonExplainable

	typs []*types.T

	drainHelper *drainHelper

	// runtime fields --

	outputBatch    coldata.Batch
	outputMetadata *execinfrapb.ProducerMetadata

	// cancelFlow will return a function to cancel the context of the flow. It is
	// a function in order to be lazily evaluated, since the context cancellation
	// function is only available when Starting. This function differs from
	// ctxCancel in that it will cancel all components of the BatchSource's flow,
	// including those started asynchronously.
	cancelFlow func() context.CancelFunc

	// closers is a slice of IdempotentClosers that should be Closed on
	// termination.
	closers []IdempotentCloser

	ctx context.Context
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

// OutputTypes implements the BatchSource interface.
func (d *drainHelper) OutputTypes() []*types.T {
	colexecerror.InternalError("unimplemented")
	// Unreachable code.
	return nil
}

// Start implements the BatchSource interface.
func (d *drainHelper) Start(ctx context.Context) context.Context {
	d.ctx = ctx
	return ctx
}

// Next implements the BatchSource interface.
func (d *drainHelper) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
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

// ConsumerDone implements the BatchSource interface.
func (d *drainHelper) ConsumerDone() {}

// ConsumerClosed implements the BatchSource interface.
func (d *drainHelper) ConsumerClosed() {}

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
func NewBatchConsumer(
	flowCtx *execinfra.FlowCtx,
	input colexecbase.Operator,
	typs []*types.T,
	metadataSourcesQueue []execinfrapb.MetadataSource,
	toClose []IdempotentCloser,
	outputStatsToTrace func(),
	cancelFlow func() context.CancelFunc,
) (*BatchSource, error) {
	s := &BatchSource{
		OneInputNode: NewOneInputNode(input),
		typs:         typs,
		drainHelper:  newDrainHelper(metadataSourcesQueue),
		closers:      toClose,
	}

	if err := s.ProcessorBase.Init(
		s,
		// input must have handled any post-processing itself, so we pass in
		// an empty post-processing spec.
		&execinfrapb.PostProcessSpec{},
		typs,
		flowCtx,
		-1,
		nil, /* output */
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{s.drainHelper},
			TrailingMetaCallback: func(ctx context.Context) []execinfrapb.ProducerMetadata {
				s.InternalClose()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	s.FinishTrace = outputStatsToTrace
	s.cancelFlow = cancelFlow
	return s, nil
}

func (s *BatchSource) OutputTypes() []*types.T {
	return s.typs
}

func (s *BatchSource) Start(ctx context.Context) context.Context {
	s.input.Init()
	s.Ctx = s.drainHelper.Start(ctx)
	return s.Ctx
}

func (s *BatchSource) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	panic("unimplemented")
}

// nextAdapter calls next() and saves the returned results in s. For internal
// use only. The purpose of having this function is to not create an anonymous
// function on every call to Next().
func (s *BatchSource) nextAdapter() {
	s.outputBatch, s.outputMetadata = s.next()
}

// next is the logic of Next() extracted in a separate method to be used by an
// adapter to be able to wrap the latter with a catcher.
func (s *BatchSource) next() (coldata.Batch, *execinfrapb.ProducerMetadata) {
	if s.State == execinfra.StateRunning {
		batch := s.input.Next(s.Ctx)
		if batch.Length() == 0 {
			s.MoveToDraining(nil /* err */)
			return nil, s.DrainHelper()
		}
		return batch, nil
	}
	return nil, s.DrainHelper()
}

func (s *BatchSource) NextBatch() (coldata.Batch, *execinfrapb.ProducerMetadata) {
	if err := colexecerror.CatchVectorizedRuntimeError(s.nextAdapter); err != nil {
		s.MoveToDraining(err)
		return nil, s.DrainHelper()
	}
	return s.outputBatch, s.outputMetadata
}

func (s *BatchSource) InternalClose() bool {
	if s.ProcessorBase.InternalClose() {
		if s.cancelFlow != nil {
			s.cancelFlow()()
		}
		for _, closer := range s.closers {
			if err := closer.IdempotentClose(s.ctx); err != nil {
				if log.V(1) {
					log.Infof(s.ctx, "error closing Closer: %v", err)
				}
			}
		}
		return true
	}
	return false
}

func (s *BatchSource) ConsumerDone() {
	// BatchSource will move into 'draining' state, and after all the metadata
	// has been drained - as part of TrailingMetaCallback - InternalClose() will
	// be called which will cancel the flow.
	s.MoveToDraining(nil /* err */)
}

func (s *BatchSource) ConsumerClosed() {
	s.InternalClose()
}
