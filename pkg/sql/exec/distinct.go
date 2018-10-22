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
// writing true to the resultant bool column for every value that differs from
// the previous one.
type sortedDistinctInt64Op struct {
	input Operator

	// sortedDistinctCol is the index of the column to distinct upon.
	sortedDistinctCol int

	// outputColIdx is the index of the boolean output column in the input batch.
	outputColIdx int

	// Set to true at runtime when we've seen the first row. Distinct always
	// outputs the first row that it sees.
	foundFirstRow bool

	// lastVal is the last value seen by the operator, so that the distincting
	// still works across batch boundaries.
	lastVal int64 // template
}

var _ Operator = &sortedDistinctInt64Op{}

func (p *sortedDistinctInt64Op) Init() {}

func (p *sortedDistinctInt64Op) Next() ColBatch {
	batch := p.input.Next()
	if batch.Length() == 0 {
		return batch
	}
	outputCol := batch.ColVec(p.outputColIdx).Bool()
	col := batch.ColVec(p.sortedDistinctCol).Int64()

	// We always output the first row.
	lastVal := p.lastVal
	sel := batch.Selection()
	if !p.foundFirstRow {
		if sel != nil {
			lastVal = col[sel[0]]
			outputCol[sel[0]] = true
		} else {
			lastVal = col[0]
			outputCol[0] = true
		}
	}

	startIdx := uint16(0)
	if !p.foundFirstRow {
		startIdx = 1
	}

	n := batch.Length()
	if sel != nil {
		// Bounds check elimination.
		sel = sel[startIdx:n]
		for _, i := range sel {
			v := col[i]
			// Note that not inlining this unique var actually makes a non-trivial
			// performance difference.
			unique := v != lastVal
			outputCol[i] = outputCol[i] || unique
			lastVal = v
		}
	} else {
		// Bounds check elimination.
		col = col[startIdx:n]
		outputCol = outputCol[startIdx:n]
		for i := range col {
			v := col[i]
			// Note that not inlining this unique var actually makes a non-trivial
			// performance difference.
			unique := v != lastVal
			outputCol[i] = outputCol[i] || unique
			lastVal = v
		}
	}

	p.lastVal = lastVal
	p.foundFirstRow = true

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
	// Loop until we have non-zero amount of output to return, or our input's been
	// exhausted.
	for {
		batch := p.input.Next()
		if batch.Length() == 0 {
			return batch
		}
		outputCol := batch.ColVec(p.outputColIdx).Bool()

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

		batch.SetLength(idx)
		return batch
	}
}

func (p *sortedDistinctFinalizerOp) Init() {}
