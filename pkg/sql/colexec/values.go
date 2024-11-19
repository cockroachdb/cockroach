// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

	helper colmem.SetAccountingHelper
	dalloc tree.DatumAlloc
	batch  coldata.Batch
	vecs   coldata.TypedVecs
	row    rowenc.EncDatumRow
}

var _ colexecop.Operator = &valuesOp{}

// NewValuesOp returns a new values operator, which has no input and outputs a
// fixed set of rows.
func NewValuesOp(
	allocator *colmem.Allocator, spec *execinfrapb.ValuesCoreSpec, memoryLimit int64,
) colexecop.Operator {
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
		typs: make([]*types.T, len(spec.Columns)),
		data: spec.RawBytes,
	}

	for i := range spec.Columns {
		v.typs[i] = spec.Columns[i].Type
	}
	v.helper.Init(allocator, memoryLimit, v.typs, false /* alwaysReallocate */)
	return v
}

func (v *valuesOp) Init(ctx context.Context) {
	if !v.InitHelper.Init(ctx) {
		return
	}
	v.row = make(rowenc.EncDatumRow, len(v.typs))
}

func (v *valuesOp) Next() coldata.Batch {
	if len(v.data) == 0 {
		return coldata.ZeroBatch
	}

	var reallocated bool
	v.batch, reallocated = v.helper.ResetMaybeReallocate(v.typs, v.batch, len(v.data))
	if reallocated {
		v.vecs.SetBatch(v.batch)
	}

	// Check if we will buffer more rows than the current allocation size and
	// increase it if so.
	willBuffer := v.batch.Capacity()
	if len(v.data) < willBuffer {
		willBuffer = len(v.data)
	}
	if v.dalloc.DefaultAllocSize < willBuffer {
		v.dalloc.DefaultAllocSize = willBuffer
	}

	nRows := 0
	for batchDone := false; !batchDone && len(v.data) > 0; {
		rowData := v.data[0]
		for i := 0; i < len(v.typs); i++ {
			var err error
			v.row[i], rowData, err = rowenc.EncDatumFromBuffer(
				catenumpb.DatumEncoding_VALUE, rowData,
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
		EncDatumRowToColVecs(v.row, nRows, v.vecs, v.typs, &v.dalloc)
		batchDone = v.helper.AccountForSet(nRows)
		v.data = v.data[1:]
		nRows++
	}

	v.batch.SetLength(nRows)
	return v.batch
}
