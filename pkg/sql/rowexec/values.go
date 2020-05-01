// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// valuesProcessor is a processor that has no inputs and generates "pre-canned"
// rows.
type valuesProcessor struct {
	execinfra.ProcessorBase

	columns []execinfrapb.DatumInfo
	data    [][]byte
	// numRows is only guaranteed to be set if there are zero columns (because of
	// backward compatibility). If it set and there are columns, it matches the
	// number of rows that are encoded in data.
	numRows uint64

	sd     flowinfra.StreamDecoder
	rowBuf sqlbase.EncDatumRow
}

var _ execinfra.Processor = &valuesProcessor{}
var _ execinfra.RowSource = &valuesProcessor{}
var _ execinfra.OpNode = &valuesProcessor{}

const valuesProcName = "values"

func newValuesProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.ValuesCoreSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*valuesProcessor, error) {
	v := &valuesProcessor{
		columns: spec.Columns,
		numRows: spec.NumRows,
		data:    spec.RawBytes,
	}
	types := make([]*types.T, len(v.columns))
	for i := range v.columns {
		types[i] = v.columns[i].Type
	}
	if err := v.Init(
		v, post, types, flowCtx, processorID, output, nil /* memMonitor */, execinfra.ProcStateOpts{},
	); err != nil {
		return nil, err
	}
	return v, nil
}

// Start is part of the RowSource interface.
func (v *valuesProcessor) Start(ctx context.Context) context.Context {
	ctx = v.StartInternal(ctx, valuesProcName)

	// Add a bogus header to appease the StreamDecoder, which wants to receive a
	// header before any data.
	m := &execinfrapb.ProducerMessage{
		Typing: v.columns,
		Header: &execinfrapb.ProducerHeader{},
	}
	if err := v.sd.AddMessage(ctx, m); err != nil {
		v.MoveToDraining(err)
		return ctx
	}

	v.rowBuf = make(sqlbase.EncDatumRow, len(v.columns))
	return ctx
}

// Next is part of the RowSource interface.
func (v *valuesProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for v.State == execinfra.StateRunning {
		row, meta, err := v.sd.GetRow(v.rowBuf)
		if err != nil {
			v.MoveToDraining(err)
			break
		}

		if meta != nil {
			return nil, meta
		}

		if row == nil {
			// Push a chunk of data to the stream decoder.
			m := &execinfrapb.ProducerMessage{}
			if len(v.columns) == 0 {
				if v.numRows == 0 {
					v.MoveToDraining(nil /* err */)
					break
				}
				m.Data.NumEmptyRows = int32(v.numRows)
				v.numRows = 0
			} else {
				if len(v.data) == 0 {
					v.MoveToDraining(nil /* err */)
					break
				}
				m.Data.RawBytes = v.data[0]
				v.data = v.data[1:]
			}
			if err := v.sd.AddMessage(context.TODO(), m); err != nil {
				v.MoveToDraining(err)
				break
			}
			continue
		}

		if outRow := v.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, v.DrainHelper()

}

// ConsumerClosed is part of the RowSource interface.
func (v *valuesProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	v.InternalClose()
}

// ChildCount is part of the execinfra.OpNode interface.
func (v *valuesProcessor) ChildCount(verbose bool) int {
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (v *valuesProcessor) Child(nth int, verbose bool) execinfra.OpNode {
	panic(fmt.Sprintf("invalid index %d", nth))
}
