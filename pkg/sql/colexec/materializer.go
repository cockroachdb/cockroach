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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// Materializer converts an Operator input into a execinfra.RowSource.
type Materializer struct {
	execinfra.ProcessorBase
	NonExplainable

	input colexecbase.Operator
	typs  []*types.T

	// runtime fields --

	// curIdx represents the current index into the column batch: the next row
	// the Materializer will emit.
	curIdx int
	// batch is the current Batch the Materializer is processing.
	batch coldata.Batch
	// converter contains the converted vectors of the current batch. Note that
	// if the batch had a selection vector on top of it, the converted vectors
	// will be "dense" and contain only tuples that were selected.
	converter *vecToDatumConverter

	// row is the memory used for the output row.
	row sqlbase.EncDatumRow

	// Fields to store the returned results of next() to be passed through an
	// adapter.
	outputRow      sqlbase.EncDatumRow
	outputMetadata *execinfrapb.ProducerMetadata
}

const materializerProcName = "materializer"

// NewMaterializer creates a new Materializer processor which processes the
// columnar data coming from input to return it as rows.
// Arguments:
// - typs is the output types scheme.
// - outputStatsToTrace (when tracing is enabled) finishes the stats.
// NOTE: the constructor does *not* take in an execinfrapb.PostProcessSpec
// because we expect input to handle that for us.
func NewMaterializer(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input colexecbase.Operator,
	typs []*types.T,
	output execinfra.RowReceiver,
	outputStatsToTrace func(),
) (*Materializer, error) {
	m := &Materializer{
		input: input,
		typs:  typs,
		// nil vecIdxsToConvert indicates that we want to convert all vectors.
		converter: newVecToDatumConverter(len(typs), nil /* vecIdxsToConvert */),
		row:       make(sqlbase.EncDatumRow, len(typs)),
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
		execinfra.ProcStateOpts{},
	); err != nil {
		return nil, err
	}
	m.FinishTrace = outputStatsToTrace
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
	colexecerror.InternalError(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// Start is part of the execinfra.RowSource interface.
func (m *Materializer) Start(ctx context.Context) context.Context {
	m.input.Init()
	return m.ProcessorBase.StartInternal(ctx, materializerProcName)
}

// nextAdapter calls next() and saves the returned results in m. For internal
// use only. The purpose of having this function is to not create an anonymous
// function on every call to Next().
func (m *Materializer) nextAdapter() {
	m.outputRow, m.outputMetadata = m.next()
}

// next is the logic of Next() extracted in a separate method to be used by an
// adapter to be able to wrap the latter with a catcher.
func (m *Materializer) next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if m.State == execinfra.StateRunning {
		if m.batch == nil || m.curIdx >= m.batch.Length() {
			// Get a fresh batch.
			m.batch = m.input.Next(m.Ctx)
			if m.batch.Length() == 0 {
				m.MoveToDraining(nil /* err */)
				return nil, m.DrainHelper()
			}
			m.curIdx = 0
			m.converter.convertBatch(m.batch)
		}

		for colIdx := range m.typs {
			// Note that we don't need to apply the selection vector of the
			// batch to index m.curIdx because vecToDatumConverter returns a
			// "dense" datum column.
			m.row[colIdx].Datum = m.converter.getDatumColumn(colIdx)[m.curIdx]
		}
		m.curIdx++
		// Note that there is no post-processing to be done in the
		// materializer, so we do not use ProcessRowHelper and emit the row
		// directly.
		return m.row, nil
	}
	return nil, m.DrainHelper()
}

// Next is part of the execinfra.RowSource interface.
func (m *Materializer) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if err := colexecerror.CatchVectorizedRuntimeError(m.nextAdapter); err != nil {
		m.MoveToDraining(err)
		return nil, m.DrainHelper()
	}
	return m.outputRow, m.outputMetadata
}

// ConsumerClosed is part of the execinfra.RowSource interface.
func (m *Materializer) ConsumerClosed() {
	m.InternalClose()
}
