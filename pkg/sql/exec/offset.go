// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package exec

// offsetOp is an operator that implements offset, returning everything
// after the first n tuples in its input.
type offsetOp struct {
	input Operator

	internalBatch ColBatch
	offset        uint64

	// seen is the number of tuples seen so far.
	seen uint64
}

// NewOffsetOp returns a new offset operator with the given offset.
func NewOffsetOp(input Operator, offset uint64) Operator {
	c := &offsetOp{
		input:  input,
		offset: offset,
	}
	return c
}

func (c *offsetOp) Init() {
	c.internalBatch = NewMemBatch(nil)
	c.input.Init()
}

func (c *offsetOp) Next() ColBatch {
	for {
		bat := c.input.Next()
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
