// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// materializer converts an exec.Operator input into a RowSource.
type materializer struct {
	ProcessorBase

	input exec.Operator

	da sqlbase.DatumAlloc

	// outputToInputColIdx is a mapping from output row index to the operator's
	// internal column schema. For example, if the input operator had 2 columns
	// [a, b], and the desired output was just [b], outputToInputColIdx would be
	// [1]: mapping the 0th column of the output row schema onto the 1st column
	// of the operator's row schema.
	outputToInputColIdx []int

	// runtime fields --

	// curIdx represents the current index into the column batch: the next row the
	// materializer will emit.
	curIdx uint16
	// batch is the current Batch the materializer is processing.
	batch coldata.Batch

	// row is the memory used for the output row.
	row sqlbase.EncDatumRow

	// Fields to store the returned results of next() to be passed through an
	// adapter.
	outputRow      sqlbase.EncDatumRow
	outputMetadata *distsqlpb.ProducerMetadata

	// ctxCancel will cancel the context that is passed to the input (which will
	// pass it down further). This allows for the cancellation of the tree rooted
	// at this materializer when it is closed.
	ctxCancel context.CancelFunc
}

// newMaterializer creates a new materializer processor which processes the
// columnar data coming from input to return it as rows.
// Arguments:
// - ctxFlow is the context of the flow (i.e. Flow.ctx). It should only be
// non-nil in case of a root materializer.
// - cancelFlow is the context cancellation function that cancels ctxFlow. It
// must be non-nil if ctxFlow is non-nil and must be nil if ctxFlow is nil.
// - typs is the output types scheme.
// - metadataSourcesQueue are all of the metadata sources that are planned on
// the same node as the materializer and that need to be drained.
// - outputStatsToTrace (when tracing is enabled) finishes the stats.
func newMaterializer(
	ctxFlow context.Context,
	cancelFlow context.CancelFunc,
	flowCtx *FlowCtx,
	processorID int32,
	input exec.Operator,
	typs []types.T,
	// TODO(yuzefovich): I feel like we should remove outputToInputColIdx
	// argument since it's always {0, 1, ..., len(typs)-1}.
	outputToInputColIdx []int,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
	metadataSourcesQueue []distsqlpb.MetadataSource,
	outputStatsToTrace func(),
) (*materializer, error) {
	m := &materializer{
		input:               input,
		outputToInputColIdx: outputToInputColIdx,
		row:                 make(sqlbase.EncDatumRow, len(outputToInputColIdx)),
	}

	if err := m.ProcessorBase.Init(
		m,
		post,
		typs,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		ProcStateOpts{
			TrailingMetaCallback: func(ctx context.Context) []distsqlpb.ProducerMetadata {
				var trailingMeta []distsqlpb.ProducerMetadata
				for _, src := range metadataSourcesQueue {
					trailingMeta = append(trailingMeta, src.DrainMeta(ctx)...)
				}
				m.InternalClose()
				return trailingMeta
			},
		},
	); err != nil {
		return nil, err
	}
	m.finishTrace = outputStatsToTrace
	if ctxFlow == nil && cancelFlow != nil {
		return nil, errors.AssertionFailedf("ctxFlow is nil but cancelFlow is non-nil")
	}
	if ctxFlow != nil && cancelFlow == nil {
		return nil, errors.AssertionFailedf("ctxFlow is non-nil but cancelFlow is nil")
	}
	m.Ctx = ctxFlow
	m.ctxCancel = cancelFlow
	return m, nil
}

func (m *materializer) Start(ctx context.Context) context.Context {
	m.input.Init()
	if m.ctxCancel == nil {
		m.Ctx, m.ctxCancel = context.WithCancel(ctx)
	}
	return m.Ctx
}

// nextAdapter calls next() and saves the returned results in m. For internal
// use only. The purpose of having this function is to not create an anonymous
// function on every call to Next().
func (m *materializer) nextAdapter() {
	m.outputRow, m.outputMetadata = m.next()
}

// next is the logic of Next() extracted in a separate method to be used by an
// adapter to be able to wrap the latter with a catcher.
func (m *materializer) next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for m.State == StateRunning {
		if m.batch == nil || m.curIdx >= m.batch.Length() {
			// Get a fresh batch.
			m.batch = m.input.Next(m.Ctx)

			if m.batch.Length() == 0 {
				m.MoveToDraining(nil /* err */)
				return nil, m.DrainHelper()
			}
			m.curIdx = 0
		}
		sel := m.batch.Selection()

		rowIdx := m.curIdx
		if sel != nil {
			rowIdx = sel[m.curIdx]
		}
		m.curIdx++

		typs := m.OutputTypes()
		for outIdx, cIdx := range m.outputToInputColIdx {
			col := m.batch.ColVec(cIdx)
			// TODO(asubiotto): we shouldn't have to do this check. Figure out who's
			// not setting nulls.
			if col.MaybeHasNulls() {
				if col.Nulls().NullAt(rowIdx) {
					m.row[outIdx].Datum = tree.DNull
					continue
				}
			}
			ct := typs[outIdx]

			m.row[outIdx].Datum = exec.PhysicalTypeColElemToDatum(col, rowIdx, m.da, ct)
		}
		return m.ProcessRowHelper(m.row), nil
	}
	return nil, m.DrainHelper()
}

func (m *materializer) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	if err := exec.CatchVectorizedRuntimeError(m.nextAdapter); err != nil {
		m.MoveToDraining(err)
		return nil, m.DrainHelper()
	}
	return m.outputRow, m.outputMetadata
}

func (m *materializer) InternalClose() bool {
	if m.ProcessorBase.InternalClose() {
		m.ctxCancel()
		return true
	}
	return false
}

func (m *materializer) ConsumerDone() {
	// Materializer will move into 'draining' state, and after all the metadata
	// has been drained - as part of TrailingMetaCallback - InternalClose() will
	// be called which will cancel the flow.
	m.MoveToDraining(nil /* err */)
}

func (m *materializer) ConsumerClosed() {
	m.InternalClose()
}
