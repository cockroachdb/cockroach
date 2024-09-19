// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execreleasable"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NewTupleProjOp creates a new tupleProjOp that projects newly-created tuples
// at position outputIdx taking the tuples' contents from corresponding values
// of the vectors at positions tupleContentsIdxs.
func NewTupleProjOp(
	allocator *colmem.Allocator,
	inputTypes []*types.T,
	tupleContentsIdxs []int,
	outputType *types.T,
	input colexecop.Operator,
	outputIdx int,
) colexecop.Operator {
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, outputType, outputIdx)
	return &tupleProjOp{
		OneInputHelper:    colexecop.MakeOneInputHelper(input),
		allocator:         allocator,
		converter:         colconv.NewVecToDatumConverter(len(inputTypes), tupleContentsIdxs, true /* willRelease */),
		tupleContentsIdxs: tupleContentsIdxs,
		outputType:        outputType,
		outputIdx:         outputIdx,
	}
}

type tupleProjOp struct {
	colexecop.OneInputHelper
	allocator         *colmem.Allocator
	converter         *colconv.VecToDatumConverter
	outputType        *types.T
	tupleContentsIdxs []int
	outputIdx         int
}

var _ colexecop.Operator = &tupleProjOp{}
var _ execreleasable.Releasable = &tupleProjOp{}

func (t *tupleProjOp) Next() coldata.Batch {
	batch := t.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	t.converter.ConvertBatchAndDeselect(batch)
	projVec := batch.ColVec(t.outputIdx)

	t.allocator.PerformOperation([]*coldata.Vec{projVec}, func() {
		// Preallocate the tuples and their underlying datums in a contiguous
		// slice to reduce allocations.
		tuples := make([]tree.DTuple, n)
		l := len(t.tupleContentsIdxs)
		datums := make(tree.Datums, n*l)
		projCol := projVec.Datum()
		projectInto := func(dst, src int) {
			tuples[src] = tree.MakeDTuple(
				t.outputType, datums[src*l:(src+1)*l:(src+1)*l]...,
			)
			projCol.Set(dst, t.projectInto(&tuples[src], src))
		}
		if sel := batch.Selection(); sel != nil {
			for convertedIdx, i := range sel[:n] {
				projectInto(i, convertedIdx)
			}
		} else {
			for i := 0; i < n; i++ {
				projectInto(i, i)
			}
		}
	})
	return batch
}

func (t *tupleProjOp) projectInto(tuple *tree.DTuple, convertedIdx int) tree.Datum {
	for i, columnIdx := range t.tupleContentsIdxs {
		tuple.D[i] = t.converter.GetDatumColumn(columnIdx)[convertedIdx]
	}
	return tuple
}

// Release is part of the execinfra.Releasable interface.
func (t *tupleProjOp) Release() {
	t.converter.Release()
}
