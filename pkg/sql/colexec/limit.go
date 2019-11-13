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
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
)

// limitOp is an operator that implements limit, returning only the first n
// tuples from its input.
type limitOp struct {
	OneInputNode

	allocator *Allocator
	zeroBatch coldata.Batch
	limit     uint64

	// seen is the number of tuples seen so far.
	seen uint64
	// done is true if the limit has been reached.
	done bool
}

var _ Operator = &limitOp{}

// NewLimitOp returns a new limit operator with the given limit.
func NewLimitOp(allocator *Allocator, input Operator, limit uint64) Operator {
	c := &limitOp{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		limit:        limit,
	}
	return c
}

func (c *limitOp) Init() {
	c.zeroBatch = c.allocator.NewMemBatchWithSize(nil /* types */, 0 /* size */)
	c.input.Init()
}

func (c *limitOp) Next(ctx context.Context) coldata.Batch {
	if c.done {
		c.zeroBatch.SetLength(0)
		return c.zeroBatch
	}
	bat := c.input.Next(ctx)
	length := bat.Length()
	if length == 0 {
		return bat
	}
	newSeen := c.seen + uint64(length)
	if newSeen >= c.limit {
		c.done = true
		bat.SetLength(uint16(c.limit - c.seen))
		return bat
	}
	c.seen = newSeen
	return bat
}
