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

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestDeselector(t *testing.T) {
	tcs := []struct {
		colTypes []types.T
		tuples   []tuple
		sel      []uint16
		expected []tuple
	}{
		{
			colTypes: []types.T{types.Int64},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      nil,
			expected: tuples{{0}, {1}, {2}},
		},
		{
			colTypes: []types.T{types.Int64},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      []uint16{},
			expected: tuples{},
		},
		{
			colTypes: []types.T{types.Int64},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      []uint16{1},
			expected: tuples{{1}},
		},
		{
			colTypes: []types.T{types.Int64},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      []uint16{0, 2},
			expected: tuples{{0}, {2}},
		},
		{
			colTypes: []types.T{types.Int64},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      []uint16{0, 1, 2},
			expected: tuples{{0}, {1}, {2}},
		},
	}

	for _, tc := range tcs {
		runTestsWithFixedSel(t, []tuples{tc.tuples}, tc.sel, func(t *testing.T, input []Operator) {
			op := NewDeselectorOp(input[0], tc.colTypes)
			out := newOpTestOutput(op, []int{0}, tc.expected)

			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkDeselector(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	nCols := 1
	inputTypes := make([]types.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		inputTypes[colIdx] = types.Int64
	}

	batch := coldata.NewMemBatch(inputTypes)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < coldata.BatchSize; i++ {
			col[i] = int64(i)
		}
	}
	for _, probOfOmitting := range []float64{0.1, 0.9} {
		sel := randomSel(rng, coldata.BatchSize, probOfOmitting)
		batchLen := uint16(len(sel))

		for _, nBatches := range []int{1 << 1, 1 << 2, 1 << 4, 1 << 8} {
			b.Run(fmt.Sprintf("rows=%d/after selection=%d", nBatches*coldata.BatchSize, nBatches*int(batchLen)), func(b *testing.B) {
				// We're measuring the amount of data that is not selected out.
				b.SetBytes(int64(8 * nBatches * int(batchLen) * nCols))
				batch.SetSelection(true)
				copy(batch.Selection(), sel)
				batch.SetLength(batchLen)
				input := newRepeatableBatchSource(batch)
				op := NewDeselectorOp(input, inputTypes)
				op.Init()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					input.resetBatchesToReturn(nBatches)
					for b := op.Next(); b.Length() != 0; b = op.Next() {
					}
					// We don't need to reset the deselector because it doesn't keep any
					// state. We do, however, want to keep its already allocated memory
					// so that this memory allocation doesn't impact the benchmark.
				}
				b.StopTimer()
			})
		}
	}
}
