// Copyright 2018 The Cockroach Authors.
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

import (
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// countOp is an operator that counts the number of input rows it receives,
// consuming its entire input and outputting a batch with a single integer
// column containing a single integer, the count of rows received from the
// upstream.
type countOp struct {
	input Operator

	internalBatch ColBatch
	done          bool
	count         int64
}

// NewCountOp returns a new count operator that counts the rows in its input.
func NewCountOp(input Operator) Operator {
	c := &countOp{
		input: input,
	}
	c.internalBatch = newMemBatchWithSize([]types.T{types.Int64}, 1)
	return c
}

func (c *countOp) Init() {
	c.input.Init()
	// Our output is always just one row.
	c.internalBatch.SetLength(1)
	c.count = 0
	c.done = false
}

func (c *countOp) Next() ColBatch {
	if c.done {
		c.internalBatch.SetLength(0)
		return c.internalBatch
	}
	for {
		bat := c.input.Next()
		length := bat.Length()
		if length == 0 {
			c.done = true
			c.internalBatch.ColVec(0).Int64()[0] = c.count
			return c.internalBatch
		}
		c.count += int64(length)
	}
}
