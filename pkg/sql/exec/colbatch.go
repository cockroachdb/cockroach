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

// ColBatch is the type that columnar operators receive and produce. It
// represents a set of column vectors (partial data columns) as well as
// metadata about a batch, like the selection vector (which rows in the column
// batch are selected).
type ColBatch interface {
	// Length returns the number of values in the columns in the batch.
	Length() int
	// ColVec returns the ith ColVec in this batch.
	ColVec(i int) ColVec
	// Selection, if not nil, returns the selection vector on this batch: a
	// densely-packed list of the indices in each column that have not been
	// filtered out by a previous step.
	Selection() []int8
}

var _ ColBatch = memBatch{}

type memBatch struct {
	// length of batch or sel in tuples
	n int
	// slice of columns in this batch.
	b      []ColVec
	useSel bool
	// if useSel is true, a selection vector from upstream. a selection vector is
	// a list of selected column indexes in this dataFlow's columns.
	sel []int8
}

func (m memBatch) Length() int {
	return m.n
}

func (m memBatch) ColVec(i int) ColVec {
	return m.b[i]
}

func (m memBatch) Selection() []int8 {
	if !m.useSel {
		return nil
	}
	return m.sel
}
