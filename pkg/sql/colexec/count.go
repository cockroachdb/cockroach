// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// countOp is an operator that counts the number of input rows it receives,
// consuming its entire input and outputting a batch with a single integer
// column containing a single integer, the count of rows received from the
// upstream.
type countOp struct {
	colexecop.OneInputHelper

	internalBatch coldata.Batch
	done          bool
	count         int64
}

var _ colexecop.Operator = &countOp{}

// NewCountOp returns a new count operator that counts the rows in its input.
func NewCountOp(allocator *colmem.Allocator, input colexecop.Operator) colexecop.Operator {
	c := &countOp{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
	}
	c.internalBatch = allocator.NewMemBatchWithFixedCapacity(
		[]*types.T{types.Int}, 1, /* capacity */
	)
	return c
}

func (c *countOp) Next() coldata.Batch {
	if c.done {
		return coldata.ZeroBatch
	}
	c.internalBatch.ResetInternalBatch()
	for {
		bat := c.Input.Next()
		length := bat.Length()
		if length == 0 {
			c.done = true
			c.internalBatch.ColVec(0).Int64()[0] = c.count
			c.internalBatch.SetLength(1)
			return c.internalBatch
		}
		c.count += int64(length)
	}
}
