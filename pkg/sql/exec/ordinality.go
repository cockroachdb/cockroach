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

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// ordinalityOp is an operator that implements WITH ORDINALITY, which adds
// an additional column to the result with an ordinal number.
type ordinalityOp struct {
	input Operator

	// hasOrdinalityColumn is a flag to indicate if the ordinality column has
	// already been added to the batch. It is needed because batches are reused
	// between subsequent calls to Next().
	hasOrdinalityColumn bool
	// counter is the number of tuples seen so far.
	counter int64
}

var _ Operator = &ordinalityOp{}

// NewOrdinalityOp returns a new WITH ORDINALITY operator.
func NewOrdinalityOp(input Operator) Operator {
	c := &ordinalityOp{
		input:               input,
		hasOrdinalityColumn: false,
		counter:             1,
	}
	return c
}

func (c *ordinalityOp) Init() {
	c.input.Init()
}

func (c *ordinalityOp) Next(ctx context.Context) coldata.Batch {
	bat := c.input.Next(ctx)
	if !c.hasOrdinalityColumn {
		bat.AppendCol(types.Int64)
		c.hasOrdinalityColumn = true
	}
	vec := bat.ColVec(bat.Width() - 1).Int64()
	sel := bat.Selection()

	if sel != nil {
		// Bounds check elimination.
		for _, i := range sel[:bat.Length()] {
			vec[i] = c.counter
			c.counter++
		}
	} else {
		// Bounds check elimination.
		vec = vec[:bat.Length()]
		for i := range vec {
			vec[i] = c.counter
			c.counter++
		}
	}

	return bat
}
