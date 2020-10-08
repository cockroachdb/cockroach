// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// ordinalityOp is an operator that implements WITH ORDINALITY, which adds
// an additional column to the result with an ordinal number.
type ordinalityOp struct {
	OneInputNode

	allocator *colmem.Allocator
	// outputIdx is the index of the column in which ordinalityOp will write the
	// ordinal number.
	outputIdx int
	// counter is the number of tuples seen so far.
	counter int64
}

var _ colexecbase.Operator = &ordinalityOp{}

// NewOrdinalityOp returns a new WITH ORDINALITY operator.
func NewOrdinalityOp(
	allocator *colmem.Allocator, input colexecbase.Operator, outputIdx int,
) colexecbase.Operator {
	input = newVectorTypeEnforcer(allocator, input, types.Int, outputIdx)
	c := &ordinalityOp{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		outputIdx:    outputIdx,
		counter:      1,
	}
	return c
}

func (c *ordinalityOp) Init() {
	c.input.Init()
}

func (c *ordinalityOp) Next(ctx context.Context) coldata.Batch {
	bat := c.input.Next(ctx)
	if bat.Length() == 0 {
		return coldata.ZeroBatch
	}

	outputVec := bat.ColVec(c.outputIdx)
	if outputVec.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		outputVec.Nulls().UnsetNulls()
	}
	col := outputVec.Int64()
	sel := bat.Selection()

	if sel != nil {
		// Bounds check elimination.
		for _, i := range sel[:bat.Length()] {
			col[i] = c.counter
			c.counter++
		}
	} else {
		// Bounds check elimination.
		col = col[:bat.Length()]
		for i := range col {
			col[i] = c.counter
			c.counter++
		}
	}

	return bat
}
