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

// boolVecToSelOp transforms a boolean column into a selection vector by adding
// an index to the selection for each true value in the boolean column.
type boolVecToSelOp struct {
	input Operator

	// outputCol is the boolean output column. It should be shared by other
	// operators that write to it.
	outputCol []bool
}

var _ Operator = &boolVecToSelOp{}

var zeroBoolVec = make([]bool, ColBatchSize)

func (p *boolVecToSelOp) Next() ColBatch {
	// Loop until we have non-zero amount of output to return, or our input's been
	// exhausted.
	for {
		batch := p.input.Next()
		if batch.Length() == 0 {
			return batch
		}
		outputCol := p.outputCol

		// Convert outputCol to a selection vector by outputting the index of each
		// tuple whose outputCol value is true.
		// Note that, if the input already had a selection vector, the output
		// selection vector will be a subset of the input selection vector.
		idx := uint16(0)
		n := batch.Length()
		if sel := batch.Selection(); sel != nil {
			for s := uint16(0); s < n; s++ {
				i := sel[s]
				if outputCol[i] {
					sel[idx] = i
					idx++
				}
			}
		} else {
			batch.SetSelection(true)
			sel := batch.Selection()
			for i := uint16(0); i < n; i++ {
				if outputCol[i] {
					sel[idx] = i
					idx++
				}
			}
		}

		if idx == 0 {
			continue
		}

		// Zero our output column for next time.
		copy(p.outputCol, zeroBoolVec)

		batch.SetLength(idx)
		return batch
	}
}

func (p *boolVecToSelOp) Init() {
	p.input.Init()
}
