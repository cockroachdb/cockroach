// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecwindow

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NewFirstValueOperator creates a new Operator that computes window function
// FIRST_VALUE. outputColIdx specifies in which coldata.Vec the operator should
// put its output (if there is no such column, a new column is appended).
func NewFirstValueOperator(
	allocator *colmem.Allocator, input colexecop.Operator, outputColIdx int, partitionColIdx int,
) colexecop.Operator {
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, types.Int, outputColIdx)
	base := firstValueBase{
		OneInputNode:    colexecop.NewOneInputNode(input),
		allocator:       allocator,
		outputColIdx:    outputColIdx,
		partitionColIdx: partitionColIdx,
	}
	if partitionColIdx == -1 {
		return &firstValueNoPartitionOp{base}
	}
	return &firstValueWithPartitionOp{base}
}

type firstValueBase struct {
	colexecop.OneInputNode
	allocator       *colmem.Allocator
	outputColIdx    int
	partitionColIdx int

	firstValue int64
}

func (r *firstValueBase) Init() {
	r.Input.Init()
}

type firstValueNoPartitionOp struct {
	firstValueBase
}

var _ colexecop.Operator = &firstValueNoPartitionOp{}

func (r *firstValueNoPartitionOp) Next(ctx context.Context) coldata.Batch {
	batch := r.Input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}

	firstValueVec := batch.ColVec(r.outputColIdx)
	if firstValueVec.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		firstValueVec.Nulls().UnsetNulls()
	}
	firstValueCol := firstValueVec.Int64()
	sel := batch.Selection()
	if sel != nil {
		firstRow := true
		for _, i := range sel[:n] {
			if firstRow {
				r.firstValue = firstValueCol[i]
				firstRow = false
			}
			firstValueCol[i] = r.firstValue
		}
	} else {
		_ = firstValueCol[n-1]
		r.firstValue = firstValueCol[0]
		for i := 0; i < n; i++ {
			firstValueCol[i] = r.firstValue
		}
	}
	return batch
}

type firstValueWithPartitionOp struct {
	firstValueBase
}

var _ colexecop.Operator = &firstValueWithPartitionOp{}

func (r *firstValueWithPartitionOp) Next(ctx context.Context) coldata.Batch {
	batch := r.Input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}

	partitionCol := batch.ColVec(r.partitionColIdx).Bool()
	firstValueVec := batch.ColVec(r.outputColIdx)
	if firstValueVec.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		firstValueVec.Nulls().UnsetNulls()
	}
	firstValueCol := firstValueVec.Int64()
	sel := batch.Selection()
	if sel != nil {
		for _, i := range sel[:n] {
			if partitionCol[i] {
				r.firstValue = firstValueCol[i]
			}
			firstValueCol[i] = r.firstValue
		}
	} else {
		_ = partitionCol[n-1]
		_ = firstValueCol[n-1]
		for i := 0; i < n; i++ {
			if partitionCol[i] {
				r.firstValue = firstValueCol[i]
			}
			firstValueCol[i] = r.firstValue
		}
	}
	return batch
}
