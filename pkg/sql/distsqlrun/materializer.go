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
}

func newMaterializer(
	flowCtx *FlowCtx,
	processorID int32,
	input exec.Operator,
	typs []types.T,
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
				return trailingMeta
			},
		},
	); err != nil {
		return nil, err
	}
	m.finishTrace = outputStatsToTrace
	return m, nil
}

func (m *materializer) Start(ctx context.Context) context.Context {
	m.input.Init()
	m.Ctx = ctx
	return ctx
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

func (m *materializer) ConsumerClosed() {
	// TODO(yuzefovich): this seems like a hack to me, but in order to close an
	// Inbox, we need to drain it.
	m.trailingMetaCallback(m.Ctx)
	m.InternalClose()
}
