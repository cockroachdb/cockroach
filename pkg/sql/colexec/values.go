// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// valuesOp holds a fixed set of encoded rows, which are decoded and emitted in
// batches.
type valuesOp struct {
	colexecop.ZeroInputNode
	colexecop.InitHelper

	// Type of each column.
	typs []*types.T
	// Raw bytes of serialized rows, one row per []byte.
	data [][]byte

	allocator *colmem.Allocator
	dalloc    rowenc.DatumAlloc
	batch     coldata.Batch
	rowsBuf   rowenc.EncDatumRows
}

var _ colexecop.Operator = &valuesOp{}

// NewValuesOp returns a new values operator, which has no input and outputs a
// fixed set of rows.
func NewValuesOp(allocator *colmem.Allocator, spec *execinfrapb.ValuesCoreSpec) colexecop.Operator {
	// For zero-column sets, ValuesCoreSpec uses a nil RawBytes as an
	// optimization, only using NumRows to represent the cardinality. To simplify
	// valuesOp slightly we do not handle this case.
	if uint64(len(spec.RawBytes)) != spec.NumRows {
		colexecerror.InternalError(errors.AssertionFailedf(
			"Values operator cannot handle ValuesCoreSpec with rows %d != NumRows %d",
			len(spec.RawBytes),
			spec.NumRows,
		))
	}

	v := &valuesOp{
		typs:      make([]*types.T, len(spec.Columns)),
		data:      spec.RawBytes,
		allocator: allocator,
	}

	for i := range spec.Columns {
		v.typs[i] = spec.Columns[i].Type
	}
	return v
}

func (v *valuesOp) Init(ctx context.Context) {
	if !v.InitHelper.Init(ctx) {
		return
	}

	capacity := len(v.data)
	if capacity > coldata.BatchSize() {
		capacity = coldata.BatchSize()
	}
	v.batch = v.allocator.NewMemBatchWithFixedCapacity(v.typs, capacity)

	v.rowsBuf = make(rowenc.EncDatumRows, v.batch.Capacity())
	for i := range v.rowsBuf {
		v.rowsBuf[i] = make(rowenc.EncDatumRow, len(v.typs))
	}
}

func (v *valuesOp) Next() coldata.Batch {
	if len(v.data) == 0 {
		return coldata.ZeroBatch
	}

	v.batch.ResetInternalBatch()

	// Decode rows up to the capacity of the batch.
	nRows := 0
	for ; nRows < v.batch.Capacity() && len(v.data) > 0; nRows++ {
		rowData := v.data[0]
		for i := 0; i < len(v.typs); i++ {
			var err error
			v.rowsBuf[nRows][i], rowData, err = rowenc.EncDatumFromBuffer(
				v.typs[i], descpb.DatumEncoding_VALUE, rowData,
			)
			if err != nil {
				colexecerror.InternalError(err)
			}
		}
		if len(rowData) != 0 {
			colexecerror.InternalError(errors.AssertionFailedf(
				"malformed ValuesCoreSpec row: %x, rows left %d", rowData, len(v.data),
			))
		}
		v.data = v.data[1:]
	}

	// Check if we have buffered more rows than the current allocation size and
	// increase it if so.
	if v.dalloc.AllocSize < nRows {
		v.dalloc.AllocSize = nRows
	}

	outputRows := v.rowsBuf[:nRows]
	for i, typ := range v.typs {
		err := EncDatumRowsToColVec(v.allocator, outputRows, v.batch.ColVec(i), i, typ, &v.dalloc)
		if err != nil {
			colexecerror.InternalError(err)
		}
	}
	v.batch.SetLength(nRows)
	return v.batch
}
