// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
)

// offsetOp is an operator that implements offset, returning everything
// after the first n tuples in its input.
type offsetOp struct {
	colexecop.OneInputHelper

	offset uint64

	// seen is the number of tuples seen so far.
	seen uint64
}

var _ colexecop.Operator = &offsetOp{}

// NewOffsetOp returns a new offset operator with the given offset.
func NewOffsetOp(input colexecop.Operator, offset uint64) colexecop.Operator {
	return &offsetOp{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		offset:         offset,
	}
}

func (c *offsetOp) Next() coldata.Batch {
	for {
		bat := c.Input.Next()
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
			outputStartIdx := length - int(delta)
			if sel != nil {
				copy(sel, sel[outputStartIdx:length])
			} else {
				bat.SetSelection(true)
				sel = bat.Selection()[:delta] // slice for bounds check elimination
				for i := range sel {
					//gcassert:bce
					sel[i] = outputStartIdx + i
				}
			}
			bat.SetLength(int(delta))
		}

		if c.seen > c.offset {
			return bat
		}
	}
}
