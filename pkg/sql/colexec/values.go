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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// valuesOp holds a fixed set of encoded rows, which are decoded and emitted in
// batches. The batching is very similar to the buffering Columnarizer.
type valuesOp struct {
	colexecop.ZeroInputNode

	typs []*types.T
	data [][]byte
	// numRows is only guaranteed to be set if there are zero columns (because of
	// backward compatibility). If it set and there are columns, it matches the
	// number of rows that are encoded in data.
	numRows uint64
	// There are at least rowsHint rows still to be emitted, maybe more.
	rowsHint int

	alloc   *colmem.Allocator
	dalloc  rowenc.DatumAlloc
	batch   coldata.Batch
	rowsBuf rowenc.EncDatumRows

	maxBatchMemSize int64
}

var _ colexecop.Operator = &valuesOp{}

// NewValuesOp returns a new values operator, which has no input and outputs a
// fixed set of rows.
func NewValuesOp(
	ctx context.Context,
	allocator *colmem.Allocator,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ValuesCoreSpec,
) colexecop.Operator {
	v := &valuesOp{
		typs:            make([]*types.T, len(spec.Columns)),
		data:            spec.RawBytes,
		numRows:         spec.NumRows,
		rowsHint:        int(spec.NumRows),
		alloc:           allocator,
		maxBatchMemSize: execinfra.GetWorkMemLimit(flowCtx.Cfg),
	}
	for i := range spec.Columns {
		v.typs[i] = spec.Columns[i].Type
	}
	if v.rowsHint < len(v.data) {
		v.rowsHint = len(v.data)
	}
	return v
}

func (v *valuesOp) Init() {}

func (v *valuesOp) Next(ctx context.Context) coldata.Batch {
	if len(v.data) == 0 {
		return coldata.ZeroBatch
	}

	var realloc bool
	v.batch, realloc = v.alloc.ResetMaybeReallocate(v.typs, v.batch, v.rowsHint, v.maxBatchMemSize)
	if realloc {
		oldRows := v.rowsBuf
		newRows := make(rowenc.EncDatumRows, v.batch.Capacity())
		_ = newRows[len(oldRows)]
		for i := 0; i < len(oldRows); i++ {
			//gcassert:bce
			newRows[i] = oldRows[i]
		}
		for i := len(oldRows); i < len(newRows); i++ {
			//gcassert:bce
			newRows[i] = make(rowenc.EncDatumRow, len(v.typs))
		}
		v.rowsBuf = newRows
	}
	// Decode rows up to the capacity of the batch.
	nRows := 0
	for ; nRows < v.batch.Capacity() && len(v.data) > 0; nRows++ {
		for i := 0; i < len(v.typs); i++ {
			var err error
			v.rowsBuf[nRows][i], v.data[0], err = rowenc.EncDatumFromBuffer(
				v.typs[i], descpb.DatumEncoding_VALUE, v.data[0],
			)
			if err != nil {
				colexecerror.InternalError(err)
			}
		}
		if len(v.data[0]) == 0 {
			v.data = v.data[1:]
		}
	}

	// Check if we have buffered more rows than the current allocation size and
	// increase it if so.
	if v.dalloc.AllocSize < nRows {
		v.dalloc.AllocSize = nRows
	}

	outputRows := v.rowsBuf[:nRows]
	for i, typ := range v.typs {
		err := EncDatumRowsToColVec(v.alloc, outputRows, v.batch.ColVec(i), i, typ, &v.dalloc)
		if err != nil {
			colexecerror.InternalError(err)
		}
	}
	v.batch.SetLength(nRows)

	v.rowsHint -= nRows
	if v.rowsHint < len(v.data) {
		v.rowsHint = len(v.data)
	}

	return v.batch
}
