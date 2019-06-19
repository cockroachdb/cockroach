// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package exec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// ordinalityOp is an operator that implements WITH ORDINALITY, which adds
// an additional column to the result with an ordinal number.
type ordinalityOp struct {
	input Operator

	// the number of columns in the output batch.
	numOutCols int
	// the number of tuples seen so far.
	counter int64
}

var _ Operator = &ordinalityOp{}

// NewOrdinalityOp returns a new WITH ORDINALITY operator.
func NewOrdinalityOp(input Operator, numOutCols int) Operator {
	c := &ordinalityOp{
		input:      input,
		numOutCols: numOutCols,
		counter:    1,
	}
	return c
}

func (c *ordinalityOp) Init() {
	c.input.Init()
}

func (c *ordinalityOp) Next(ctx context.Context) coldata.Batch {
	bat := c.input.Next(ctx)
	numBatCols := bat.Width()
	if numBatCols < c.numOutCols {
		bat.AppendCol(types.Int64)
	} else if numBatCols != c.numOutCols {
		panic(fmt.Sprintf("Incorrect number of columns in batch: %d. Expected %d or %d",
			numBatCols, c.numOutCols-1, c.numOutCols))
	}
	vec := bat.ColVec(bat.Width() - 1).Int64()
	sel := bat.Selection()

	for i := uint16(0); i < bat.Length(); i++ {
		if sel != nil {
			vec[sel[i]] = c.counter
		} else {
			vec[i] = c.counter
		}

		c.counter++
	}

	return bat
}
