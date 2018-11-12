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

// limitOp is an operator that implements limit, returning only the first n
// tuples from its input.
type limitOp struct {
	input Operator

	internalBatch ColBatch
	limit         uint64

	// seen is the number of tuples seen so far.
	seen uint64
	// done is true if the limit has been reached.
	done bool
}

// NewLimitOp returns a new limit operator with the given limit.
func NewLimitOp(input Operator, limit uint64) Operator {
	c := &limitOp{
		input: input,
		limit: limit,
	}
	return c
}

func (c *limitOp) Init() {
	c.internalBatch = NewMemBatch(nil)
	c.input.Init()
}

func (c *limitOp) Next() ColBatch {
	if c.done {
		c.internalBatch.SetLength(0)
		return c.internalBatch
	}
	bat := c.input.Next()
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
