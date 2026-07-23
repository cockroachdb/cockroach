// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// limitOp is an operator that implements limit, returning only the first n
// tuples from its input.
type limitOp struct {
	colexecop.OneInputInitCloserHelper

	limit uint64

	// seen is the number of tuples seen so far.
	seen uint64
	// done is true if the limit has been reached.
	done bool
}

var _ colexecop.Operator = &limitOp{}
var _ colexecop.ClosableOperator = &limitOp{}

// NewLimitOp returns a new limit operator with the given limit.
func NewLimitOp(input colexecop.Operator, limit uint64) colexecop.Operator {
	c := &limitOp{
		OneInputInitCloserHelper: colexecop.MakeOneInputInitCloserHelper(input),
		limit:                    limit,
	}
	return c
}

func (c *limitOp) Next() (coldata.Batch, *execinfrapb.ProducerMetadata) {
	if c.done {
		return coldata.ZeroBatch, nil
	}
	bat, meta := c.Input.Next()
	if meta != nil {
		return nil, meta
	}
	length := bat.Length()
	if length == 0 {
		return bat, nil
	}
	newSeen := c.seen + uint64(length)
	if newSeen >= c.limit {
		// Note that it's ok to mark the limitOp as done from metadata handling
		// perspective - if there is any remaining metadata coming from the
		// input, it'll be handled via the DrainMeta call by another operator.
		c.done = true
		bat.SetLength(int(c.limit - c.seen))
		return bat, nil
	}
	c.seen = newSeen
	return bat, nil
}
