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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

// ordinalityOp is an operator that implements WITH ORDINALITY, which adds
// an additional column to the result with an ordinal number.
type ordinalityOp struct {
	OneInputNode

	allocator *Allocator
	// ordinalityCol is the index of the column in which ordinalityOp will write
	// the ordinal number. It is colNotAppended if the column has not been
	// appended yet.
	ordinalityCol int
	// counter is the number of tuples seen so far.
	counter int64
}

var _ Operator = &ordinalityOp{}

const colNotAppended = -1

// NewOrdinalityOp returns a new WITH ORDINALITY operator.
func NewOrdinalityOp(allocator *Allocator, input Operator) Operator {
	c := &ordinalityOp{
		OneInputNode:  NewOneInputNode(input),
		allocator:     allocator,
		ordinalityCol: colNotAppended,
		counter:       1,
	}
	return c
}

func (c *ordinalityOp) Init() {
	c.input.Init()
}

func (c *ordinalityOp) Next(ctx context.Context) coldata.Batch {
	bat := c.input.Next(ctx)
	if c.ordinalityCol == colNotAppended {
		c.ordinalityCol = bat.Width()
		c.allocator.AppendColumn(bat, coltypes.Int64)
	}

	if bat.Length() == 0 {
		return bat
	}

	vec := bat.ColVec(c.ordinalityCol).Int64()
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
