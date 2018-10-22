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

// sortedDistinctInt64Op runs a distinct on the column in sortedDistinctCol,
// writing the result to the bool column in outputColIdx by or'ing the
// difference between the current and last value in the column with what's
// already in the output column. this has the effect of setting the output
// column to true when the input is distinct.
type sortedDistinctInt64Op struct {
	input Operator

	// sortedDistinctCol is the index of the column to distinct upon.
	sortedDistinctCol int

	// outputColIdx is the index of the boolean output column in the input batch.
	outputColIdx int

	// Starts at 1, flips to 0.
	firstColToLookAt uint16

	// lastVal is the last value seen by the operator, so that the distincting
	// still works across batch boundaries.
	lastVal int64 // template
}

var _ Operator = &sortedDistinctInt64Op{}

func (p *sortedDistinctInt64Op) Init() {
	p.firstColToLookAt = 1
}

func (p *sortedDistinctInt64Op) Next() ColBatch {
	batch := p.input.Next()
	if batch.Length() == 0 {
		return batch
	}
	outputCol := batch.ColVec(p.outputColIdx).Bool()

	col := batch.ColVec(p.sortedDistinctCol).Int64()[:ColBatchSize]

	// we always output the first row.
	for i := uint16(0); i < p.firstColToLookAt; i++ {
		p.lastVal = col[0]
		outputCol[i] = true
	}

	n := batch.Length()
	var i uint16
	if batch.Selection() != nil {
		sel := batch.Selection()
		for s := p.firstColToLookAt; s < n; s++ {
			i = sel[s]
			outputCol[i] = outputCol[i] || (col[i] != p.lastVal)
			p.lastVal = col[i]
		}
	} else {
		for i = p.firstColToLookAt; i < n; i++ {
			outputCol[i] = outputCol[i] || (col[i] != p.lastVal)
			p.lastVal = col[i]
		}
	}

	p.firstColToLookAt = 0

	return batch
}

// This finalizer op transforms the vector in outputColIdx to the selection
// vector, by adding an index to the selection for each true value in the column
// at outputColIdx.
type sortedDistinctFinalizerOp struct {
	input Operator

	// outputColIdx is the index of the boolean output column from previous
	// distinct ops in the input batch.
	outputColIdx int
}

var _ Operator = &sortedDistinctFinalizerOp{}

func (p *sortedDistinctFinalizerOp) Next() ColBatch {
	batch := p.input.Next()
	if batch.Length() == 0 {
		return batch
	}
	outputCol := batch.ColVec(p.outputColIdx).Bool()

	// convert outputVec to sel
	idx := uint16(0)
	sel := batch.Selection()
	n := batch.Length()
	if sel != nil {
		max := sel[n-1]
		for i := uint16(0); i < max; i++ {
			if outputCol[i] {
				sel[idx] = i
				idx++
			}
		}
	} else {
		batch.SetSelection(true)
		sel = batch.Selection()
		for i := uint16(0); i < n; i++ {
			if outputCol[i] {
				sel[idx] = i
				idx++
			}
		}
	}

	batch.SetLength(idx)
	return batch
}

func (p *sortedDistinctFinalizerOp) Init() {}
