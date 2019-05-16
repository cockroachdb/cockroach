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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/lib/pq/oid"
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

// nextBatch saves the next batch from input in m.batch. For internal use only.
// The purpose of having this function is to not create an anonymous function
// on every call to Next().
func (m *materializer) nextBatch() {
	m.batch = m.input.Next(m.Ctx)
}

func (m *materializer) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for m.State == StateRunning {
		if m.batch == nil || m.curIdx >= m.batch.Length() {
			// Get a fresh batch.
			if err := exec.CatchVectorizedRuntimeError(m.nextBatch); err != nil {
				m.MoveToDraining(err)
				return nil, m.DrainHelper()
			}

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
			if col.HasNulls() {
				if col.Nulls().NullAt(rowIdx) {
					m.row[outIdx].Datum = tree.DNull
					continue
				}
			}

			ct := typs[outIdx]
			switch ct.Family() {
			case types.BoolFamily:
				if col.Bool()[rowIdx] {
					m.row[outIdx].Datum = tree.DBoolTrue
				} else {
					m.row[outIdx].Datum = tree.DBoolFalse
				}
			case types.IntFamily:
				switch ct.Width() {
				case 8:
					m.row[outIdx].Datum = m.da.NewDInt(tree.DInt(col.Int8()[rowIdx]))
				case 16:
					m.row[outIdx].Datum = m.da.NewDInt(tree.DInt(col.Int16()[rowIdx]))
				case 32:
					m.row[outIdx].Datum = m.da.NewDInt(tree.DInt(col.Int32()[rowIdx]))
				default:
					m.row[outIdx].Datum = m.da.NewDInt(tree.DInt(col.Int64()[rowIdx]))
				}
			case types.FloatFamily:
				m.row[outIdx].Datum = m.da.NewDFloat(tree.DFloat(col.Float64()[rowIdx]))
			case types.DecimalFamily:
				m.row[outIdx].Datum = m.da.NewDDecimal(tree.DDecimal{Decimal: col.Decimal()[rowIdx]})
			case types.DateFamily:
				m.row[outIdx].Datum = tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(col.Int64()[rowIdx]))
			case types.StringFamily:
				b := col.Bytes()[rowIdx]
				if ct.Oid() == oid.T_name {
					m.row[outIdx].Datum = m.da.NewDString(tree.DString(*(*string)(unsafe.Pointer(&b))))
				} else {
					m.row[outIdx].Datum = m.da.NewDName(tree.DString(*(*string)(unsafe.Pointer(&b))))
				}
			case types.BytesFamily:
				m.row[outIdx].Datum = m.da.NewDBytes(tree.DBytes(col.Bytes()[rowIdx]))
			case types.OidFamily:
				m.row[outIdx].Datum = m.da.NewDOid(tree.MakeDOid(tree.DInt(col.Int64()[rowIdx])))
			default:
				panic(fmt.Sprintf("Unsupported column type %s", ct.String()))
			}
		}
		return m.ProcessRowHelper(m.row), nil
	}
	return nil, m.DrainHelper()
}

func (m *materializer) ConsumerClosed() {
	m.InternalClose()
}
