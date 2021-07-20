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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// valuesProcessor is a processor that has no inputs and generates "pre-canned"
// rows.
type valuesProcessor struct {
	execinfra.ProcessorBase

	typs []*types.T
	data [][]byte
	// If there are columns, numRows matches len(data). If there are no columns,
	// len(data) is zero. numRows is decremented as rows are emitted.
	numRows uint64
	rowBuf  rowenc.EncDatumRow
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
	if len(spec.Columns) > 0 && uint64(len(spec.RawBytes)) != spec.NumRows {
		return nil, errors.AssertionFailedf(
			"malformed ValuesCoreSpec: len(RawBytes) = %d does not equal NumRows = %d",
			len(spec.RawBytes), spec.NumRows,
		)
	}
	v := &valuesProcessor{
		typs:    make([]*types.T, len(spec.Columns)),
		data:    spec.RawBytes,
		numRows: spec.NumRows,
		rowBuf:  make(rowenc.EncDatumRow, len(spec.Columns)),
	}
	for i := range spec.Columns {
		v.typs[i] = spec.Columns[i].Type
	}
	if err := v.Init(
		v, post, v.typs, flowCtx, processorID, output, nil /* memMonitor */, execinfra.ProcStateOpts{},
	); err != nil {
		return nil, err
	}
	return v, nil
}

// Start is part of the RowSource interface.
func (v *valuesProcessor) Start(ctx context.Context) {
	v.StartInternal(ctx, valuesProcName)
}

// Next is part of the RowSource interface.
func (v *valuesProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for v.State == execinfra.StateRunning {
		if v.numRows == 0 {
			v.MoveToDraining(nil /* err */)
			continue
		}

		if len(v.typs) != 0 {
			rowData := v.data[0]
			for i, typ := range v.typs {
				var err error
				v.rowBuf[i], rowData, err = rowenc.EncDatumFromBuffer(
					typ, descpb.DatumEncoding_VALUE, rowData,
				)
				if err != nil {
					v.MoveToDraining(err)
					return nil, v.DrainHelper()
				}
			}
			if len(rowData) != 0 {
				panic(errors.AssertionFailedf(
					"malformed ValuesCoreSpec row: %x, numRows %d", rowData, v.numRows,
				))
			}
			v.data = v.data[1:]
		}
		v.numRows--

		if outRow := v.ProcessRowHelper(v.rowBuf); outRow != nil {
			return outRow, nil
		}
	}

	return nil, v.DrainHelper()
}

// ChildCount is part of the execinfra.OpNode interface.
func (v *valuesProcessor) ChildCount(verbose bool) int {
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (v *valuesProcessor) Child(nth int, verbose bool) execinfra.OpNode {
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
