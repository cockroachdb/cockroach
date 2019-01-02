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

// oneShotOp is an operator that does an arbitrary operation on the first batch
// that it gets, then deletes itself from the operator tree. This is useful for
// first-Next initialization that has to happen in an operator.
type oneShotOp struct {
	input Operator

	outputSourceRef *Operator

	fn func(batch ColBatch)
}

func (o *oneShotOp) Init() {
	o.input.Init()
}

func (o *oneShotOp) Next() ColBatch {
	batch := o.input.Next()

	// Do our one-time work.
	o.fn(batch)
	// Swap out our output's input with our input, so we don't have to get called
	// anymore.
	*o.outputSourceRef = o.input

	return batch
}
