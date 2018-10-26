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

// discardOp is an operator that implements "simple projection" - removal of
// columns that aren't needed by later operators.
type discardOp struct {
	input Operator

	batch *discardingBatch
}

// NewDiscardOp returns a new discardOp that discards all of the columns in its
// input batch except for the ones listed in colsToKeep.
func NewDiscardOp(input Operator, colsToKeep []uint32) Operator {
	return &discardOp{
		input: input,
		batch: newProjectionBatch(colsToKeep),
	}
}

func (d *discardOp) Init() {
	d.input.Init()
}

func (d *discardOp) Next() ColBatch {
	batch := d.input.Next()
	d.batch.ColBatch = batch

	return d.batch
}
