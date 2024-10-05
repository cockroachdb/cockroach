// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecbase

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// ordinalityOp is an operator that implements WITH ORDINALITY, which adds
// an additional column to the result with an ordinal number.
type ordinalityOp struct {
	colexecop.OneInputHelper

	allocator *colmem.Allocator
	// outputIdx is the index of the column in which ordinalityOp will write the
	// ordinal number.
	outputIdx int
	// counter is the number of tuples seen so far.
	counter int64
}

var _ colexecop.Operator = &ordinalityOp{}

// NewOrdinalityOp returns a new WITH ORDINALITY operator.
func NewOrdinalityOp(
	allocator *colmem.Allocator, input colexecop.Operator, outputIdx int,
) colexecop.Operator {
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, types.Int, outputIdx)
	c := &ordinalityOp{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		allocator:      allocator,
		outputIdx:      outputIdx,
		counter:        1,
	}
	return c
}

func (c *ordinalityOp) Next() coldata.Batch {
	bat := c.Input.Next()
	if bat.Length() == 0 {
		return coldata.ZeroBatch
	}

	outputVec := bat.ColVec(c.outputIdx)
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
