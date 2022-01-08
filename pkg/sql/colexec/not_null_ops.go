// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// notNullProjOp is an Operator that projects NULL into outputIdx Vec
type notNullProjOp struct {
	colexecop.OneInputHelper
	allocator *colmem.Allocator
	inputIdx  int
	outputIdx int
}

var _ colexecop.Operator = &notNullProjOp{}

// NewNotExprProjOp returns a new notExprProjOp.
func NewNotNullProjOp(
	allocator *colmem.Allocator, input colexecop.Operator, inputIdx, outputIdx int,
) colexecop.Operator {
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, types.Bool, outputIdx)
	return &notNullProjOp{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		allocator:      allocator,
		inputIdx:       inputIdx,
		outputIdx:      outputIdx,
	}
}

func (o *notNullProjOp) Next() coldata.Batch {
	batch := o.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	outputNulls := batch.ColVec(o.outputIdx).Nulls()
	if sel := batch.Selection(); sel != nil {
		sel = sel[:n]
		for _, idx := range sel {
			outputNulls.SetNull(idx)
		}
	} else {
		for idx := 0; idx < n; idx++ {
			outputNulls.SetNull(idx)
		}
	}
	return batch
}
