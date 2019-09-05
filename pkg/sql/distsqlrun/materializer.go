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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// materializer converts an exec.Operator input into a RowSource.
type materializer struct {
	ProcessorBase
	exec.NonExplainable

	input exec.Operator

	da sqlbase.DatumAlloc

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

	// cancelFlow will return a function to cancel the context of the flow. It is
	// a function in order to be lazily evaluated, since the context cancellation
	// function is only available when Starting. This function differs from
	// ctxCancel in that it will cancel all components of the materializer's flow,
	// including those started asynchronously.
	cancelFlow func() context.CancelFunc
}

const materializerProcName = "materializer"

// newMaterializer creates a new materializer processor which processes the
// columnar data coming from input to return it as rows.
// Arguments:
// - typs is the output types scheme.
// - metadataSourcesQueue are all of the metadata sources that are planned on
// the same node as the materializer and that need to be drained.
// - outputStatsToTrace (when tracing is enabled) finishes the stats.
// - cancelFlow should return the context cancellation function that cancels
// the context of the flow (i.e. it is Flow.ctxCancel). It should only be
// non-nil in case of a root materializer (i.e. not when we're wrapping a row
// source).
func newMaterializer(
	flowCtx *FlowCtx,
	processorID int32,
	input exec.Operator,
	typs []types.T,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
	metadataSourcesQueue []distsqlpb.MetadataSource,
	outputStatsToTrace func(),
	cancelFlow func() context.CancelFunc,
) (*materializer, error) {
	m := &materializer{
		input: input,
		row:   make(sqlbase.EncDatumRow, len(typs)),
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
	m.cancelFlow = cancelFlow
	return m, nil
}

var _ exec.OpNode = &materializer{}

func (m *materializer) ChildCount() int {
	return 1
}

func (m *materializer) Child(nth int) exec.OpNode {
	if nth == 0 {
		return m.input
	}
	panic(fmt.Sprintf("invalid index %d", nth))
}

func (m *materializer) Start(ctx context.Context) context.Context {
	m.input.Init()
	return m.ProcessorBase.StartInternal(ctx, materializerProcName)
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
		for colIdx := 0; colIdx < len(typs); colIdx++ {
			col := m.batch.ColVec(colIdx)
			// TODO(asubiotto): we shouldn't have to do this check. Figure out who's
			// not setting nulls.
			if col.MaybeHasNulls() {
				if col.Nulls().NullAt(rowIdx) {
					m.row[colIdx].Datum = tree.DNull
					continue
				}
			}
			m.row[colIdx].Datum = exec.PhysicalTypeColElemToDatum(col, rowIdx, m.da, typs[colIdx])
		}
		return m.ProcessRowHelper(m.row), nil
	}
	return nil, m.DrainHelper()
}

func (m *materializer) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	if err := execerror.CatchVectorizedRuntimeError(m.nextAdapter); err != nil {
		m.MoveToDraining(err)
		return nil, m.DrainHelper()
	}
	return m.outputRow, m.outputMetadata
}

func (m *materializer) InternalClose() bool {
	if m.ProcessorBase.InternalClose() {
		if m.cancelFlow != nil {
			m.cancelFlow()()
		}
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
