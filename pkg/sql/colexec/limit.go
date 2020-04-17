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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
)

// limitOp is an operator that implements limit, returning only the first n
// tuples from its input.
type limitOp struct {
	OneInputNode
	closerHelper

	limit int

	// seen is the number of tuples seen so far.
	seen int
	// done is true if the limit has been reached.
	done bool
}

var _ colexecbase.Operator = &limitOp{}
var _ closableOperator = &limitOp{}

// NewLimitOp returns a new limit operator with the given limit.
func NewLimitOp(input colexecbase.Operator, limit int) colexecbase.Operator {
	c := &limitOp{
		OneInputNode: NewOneInputNode(input),
		limit:        limit,
	}
	return c
}

func (c *limitOp) Init() {
	c.input.Init()
}

func (c *limitOp) Next(ctx context.Context) coldata.Batch {
	if c.done {
		return coldata.ZeroBatch
	}
	bat := c.input.Next(ctx)
	length := bat.Length()
	if length == 0 {
		return bat
	}
	newSeen := c.seen + length
	if newSeen >= c.limit {
		c.done = true
		bat.SetLength(c.limit - c.seen)
		return bat
	}
	c.seen = newSeen
	return bat
}

// Close closes the limitOp's input.
// TODO(asubiotto): Remove this method. It only exists so that we can call Close
//  from some runTests subtests when not draining the input fully. The test
//  should pass in the testing.T object used so that the caller can decide to
//  explicitly close the input after checking the test.
func (c *limitOp) IdempotentClose(ctx context.Context) error {
	if !c.close() {
		return nil
	}
	if closer, ok := c.input.(IdempotentCloser); ok {
		return closer.IdempotentClose(ctx)
	}
	return nil
}
