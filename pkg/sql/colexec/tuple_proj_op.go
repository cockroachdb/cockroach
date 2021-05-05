// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
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
	tupleContentsIdxs []int
	outputType        *types.T
	outputIdx         int
}

var _ colexecop.Operator = &tupleProjOp{}
var _ execinfra.Releasable = &tupleProjOp{}

func (t *tupleProjOp) Next() coldata.Batch {
	batch := t.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	t.converter.ConvertBatchAndDeselect(batch)
	projVec := batch.ColVec(t.outputIdx)
	if projVec.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		projVec.Nulls().UnsetNulls()
	}
	t.allocator.PerformOperation([]coldata.Vec{projVec}, func() {
		projCol := projVec.Datum()
		if sel := batch.Selection(); sel != nil {
			for convertedIdx, i := range sel[:n] {
				projCol.Set(i, t.createTuple(convertedIdx))
			}
		} else {
			for i := 0; i < n; i++ {
				projCol.Set(i, t.createTuple(i))
			}
		}
	})
	return batch
}

func (t *tupleProjOp) createTuple(convertedIdx int) tree.Datum {
	tuple := tree.NewDTupleWithLen(t.outputType, len(t.tupleContentsIdxs))
	for i, columnIdx := range t.tupleContentsIdxs {
		tuple.D[i] = t.converter.GetDatumColumn(columnIdx)[convertedIdx]
	}
	return tuple
}

// Release is part of the execinfra.Releasable interface.
func (t *tupleProjOp) Release() {
	t.converter.Release()
}
