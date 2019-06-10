// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package distsqlrun

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// valuesProcessor is a processor that has no inputs and generates "pre-canned"
// rows.
type valuesProcessor struct {
	ProcessorBase

	columns []distsqlpb.DatumInfo
	data    [][]byte
	// numRows is only guaranteed to be set if there are zero columns (because of
	// backward compatibility). If it set and there are columns, it matches the
	// number of rows that are encoded in data.
	numRows uint64

	sd     StreamDecoder
	rowBuf sqlbase.EncDatumRow
}

var _ Processor = &valuesProcessor{}
var _ RowSource = &valuesProcessor{}

const valuesProcName = "values"

func newValuesProcessor(
	flowCtx *FlowCtx,
	processorID int32,
	spec *distsqlpb.ValuesCoreSpec,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
) (*valuesProcessor, error) {
	v := &valuesProcessor{
		columns: spec.Columns,
		numRows: spec.NumRows,
		data:    spec.RawBytes,
	}
	types := make([]types.T, len(v.columns))
	for i := range v.columns {
		types[i] = v.columns[i].Type
	}
	if err := v.Init(
		v, post, types, flowCtx, processorID, output, nil /* memMonitor */, ProcStateOpts{},
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
	m := &distsqlpb.ProducerMessage{
		Typing: v.columns,
		Header: &distsqlpb.ProducerHeader{},
	}
	if err := v.sd.AddMessage(ctx, m); err != nil {
		v.MoveToDraining(err)
		return ctx
	}

	v.rowBuf = make(sqlbase.EncDatumRow, len(v.columns))
	return ctx
}

// Next is part of the RowSource interface.
func (v *valuesProcessor) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for v.State == StateRunning {
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
			m := &distsqlpb.ProducerMessage{}
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
