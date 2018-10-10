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

// Operator is a column vector operator that produces a ColBatch as output.
type Operator interface {
	// Init initializes this operator. Will be called once at operator setup time.
	Init()

	// Next returns the next ColBatch from this operator. Once the operator is
	// finished, it will return a ColBatch with length 0. Subsequent calls to
	// Next at that point will always return a ColBatch with length 0.
	//
	// Calling Next may invalidate the contents of the last ColBatch returned by
	// Next.
	Next() ColBatch
}

type noopOperator struct {
	input Operator
}

var _ Operator = &noopOperator{}

func (n *noopOperator) Init() {}

func (n *noopOperator) Next() ColBatch {
	return n.input.Next()
}
