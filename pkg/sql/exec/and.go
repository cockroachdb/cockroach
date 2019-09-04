// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

type andOp struct {
	OneInputNode

	leftIdx   int
	rightIdx  int
	outputIdx int
}

// NewAndOp returns a new operator that logical-ANDs the boolean columns at
// leftIdx and rightIdx, returning the result in outputIdx.
func NewAndOp(input Operator, leftIdx, rightIdx, outputIdx int) Operator {
	return &andOp{
		OneInputNode: NewOneInputNode(input),
		leftIdx:      leftIdx,
		rightIdx:     rightIdx,
		outputIdx:    outputIdx,
	}
}

func (a *andOp) Init() {
	a.input.Init()
}

func (a *andOp) Next(ctx context.Context) coldata.Batch {
	batch := a.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return batch
	}
	if a.outputIdx == batch.Width() {
		batch.AppendCol(coltypes.Bool)
	}
	leftCol := batch.ColVec(a.leftIdx).Bool()
	rightCol := batch.ColVec(a.rightIdx).Bool()
	outputCol := batch.ColVec(a.outputIdx).Bool()

	if sel := batch.Selection(); sel != nil {
		for _, i := range sel[:n] {
			outputCol[i] = leftCol[i] && rightCol[i]
		}
	} else {
		_ = rightCol[n-1]
		_ = outputCol[n-1]
		for i := range leftCol[:n] {
			outputCol[i] = leftCol[i] && rightCol[i]
		}
	}
	return batch
}
