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
)

// offsetOp is an operator that implements offset, returning everything
// after the first n tuples in its input.
type offsetOp struct {
	OneInputNode

	offset uint64

	// seen is the number of tuples seen so far.
	seen uint64
}

var _ Operator = &offsetOp{}

// NewOffsetOp returns a new offset operator with the given offset.
func NewOffsetOp(input Operator, offset uint64) Operator {
	c := &offsetOp{
		OneInputNode: NewOneInputNode(input),
		offset:       offset,
	}
	return c
}

func (c *offsetOp) Init() {
	c.input.Init()
}

func (c *offsetOp) Next(ctx context.Context) coldata.Batch {
	for {
		bat := c.input.Next(ctx)
		length := bat.Length()
		if length == 0 {
			return bat
		}

		c.seen += uint64(length)

		delta := c.seen - c.offset
		// If the current batch encompasses the offset "boundary",
		// add the elements after the boundary to the selection vector.
		if delta > 0 && delta < uint64(length) {
			sel := bat.Selection()
			outputStartIdx := length - uint16(delta)
			if sel != nil {
				copy(sel, sel[outputStartIdx:length])
			} else {
				bat.SetSelection(true)
				sel = bat.Selection()[:delta] // slice for bounds check elimination
				for i := range sel {
					sel[i] = outputStartIdx + uint16(i)
				}
			}
			bat.SetLength(uint16(delta))
		}

		if c.seen > c.offset {
			return bat
		}
	}
}

// Reset resets the offsetOp for another run. Primarily used for
// benchmarks.
func (c *offsetOp) Reset() {
	c.seen = 0
}
