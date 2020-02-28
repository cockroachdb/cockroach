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
	"io"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
)

// limitOp is an operator that implements limit, returning only the first n
// tuples from its input.
type limitOp struct {
	OneInputNode

	limit int

	// seen is the number of tuples seen so far.
	seen int
	// done is true if the limit has been reached.
	done bool
}

var _ Operator = &limitOp{}

// NewLimitOp returns a new limit operator with the given limit.
func NewLimitOp(input Operator, limit int) Operator {
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

// Close is a temporary method to support the specific case in which an upstream
// operator must be Closed (e.g. an external sorter) to assert a certain state
// during tests.
// TODO(asubiotto): This method only exists because an external sorter is
//  wrapped with a limit op when doing a top K sort and some tests that don't
//  exhaust the sorter (e.g. allNullsInjection) need to close the operator
//  explicitly. This should be removed once we have a better way of closing
//  operators even when they are not exhausted.
func (c *limitOp) Close() error {
	if closer, ok := c.input.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
