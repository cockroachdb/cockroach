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

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

func TestMergeJoiner(t *testing.T) {
	tcs := []struct {
		description     string
		leftTuples      []tuple
		leftTypes       []types.T
		leftOutCols     []uint32
		rightTuples     []tuple
		rightTypes      []types.T
		rightOutCols    []uint32
		expected        []tuple
		expectedOutCols []int
		outputBatchSize int
	}{
		{
			description:     "basic test",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			expected:        tuples{{1}, {2}, {3}, {4}},
			expectedOutCols: []int{0},
			outputBatchSize: ColBatchSize,
		},
		{
			description:     "basic test, no out cols",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{},
			rightOutCols:    []uint32{},
			expected:        tuples{{}, {}, {}, {}},
			expectedOutCols: []int{},
			outputBatchSize: ColBatchSize,
		},
		{
			description:     "basic test, L missing",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {3}, {4}},
			rightTuples:     tuples{{1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			expected:        tuples{{1}, {3}, {4}},
			expectedOutCols: []int{0},
			outputBatchSize: ColBatchSize,
		},
		{
			description:     "basic test, R missing",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			expected:        tuples{{1}, {3}, {4}},
			expectedOutCols: []int{0},
			outputBatchSize: ColBatchSize,
		},
		{
			description:     "basic test, L duplicate",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			expected:        tuples{{1}, {1}, {2}, {3}, {4}},
			expectedOutCols: []int{0},
			outputBatchSize: ColBatchSize,
		},
		{
			description:     "basic test, R duplicate",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			expected:        tuples{{1}, {1}, {2}, {3}, {4}},
			expectedOutCols: []int{0},
			outputBatchSize: ColBatchSize,
		},
		{
			description:     "basic test, L+R duplicates",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			expected:        tuples{{1}, {1}, {1}, {1}, {2}, {3}, {4}},
			expectedOutCols: []int{0},
			outputBatchSize: ColBatchSize,
		},
		{
			description:     "basic test, L+R duplicate, multiple runs",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {2}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			expected:        tuples{{1}, {1}, {2}, {2}, {2}, {3}, {4}},
			expectedOutCols: []int{0},
			outputBatchSize: ColBatchSize,
		},
		{
			description:     "cross product test, batch size = 1024 (ColBatchSize)",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {1}, {1}},
			rightTuples:     tuples{{1}, {1}, {1}, {1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			expected:        tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			expectedOutCols: []int{0},
			outputBatchSize: ColBatchSize,
		},
		{
			description:     "cross product test, batch size = 4 (small even)",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {1}, {1}},
			rightTuples:     tuples{{1}, {1}, {1}, {1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			expected:        tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			expectedOutCols: []int{0},
			outputBatchSize: 4,
		},
		{
			description:     "cross product test, batch size = 3 (small odd)",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {1}, {1}},
			rightTuples:     tuples{{1}, {1}, {1}, {1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			expected:        tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			expectedOutCols: []int{0},
			outputBatchSize: 3,
		},
		{
			description:     "cross product test, batch size = 1 (unit)",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {1}, {1}},
			rightTuples:     tuples{{1}, {1}, {1}, {1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			expected:        tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			expectedOutCols: []int{0},
			outputBatchSize: 1,
		},
		{
			description:     "multi output column test, basic",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			expected:        tuples{{1, 10, 1, 11}, {2, 20, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			expectedOutCols: []int{0, 1, 2, 3},
			outputBatchSize: ColBatchSize,
		},
		{
			description:     "multi output column test, batch size = 1",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			expected:        tuples{{1, 10, 1, 11}, {2, 20, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			expectedOutCols: []int{0, 1, 2, 3},
			outputBatchSize: 1,
		},
		{
			description:     "multi output column test, test output col projection",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			expected:        tuples{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			expectedOutCols: []int{0, 2},
			outputBatchSize: ColBatchSize,
		},
		{
			description:     "multi output column test, test output col projection",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{1},
			rightOutCols:    []uint32{1},
			expected:        tuples{{10, 11}, {20, 12}, {30, 13}, {40, 14}},
			expectedOutCols: []int{1, 3},
			outputBatchSize: ColBatchSize,
		},
		{
			description:     "multi output column test, L run",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {2, 21}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			expected:        tuples{{1, 10, 1, 11}, {2, 20, 2, 12}, {2, 21, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			expectedOutCols: []int{0, 1, 2, 3},
			outputBatchSize: ColBatchSize,
		},
		{
			description:     "multi output column test, L run, batch size = 1",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {2, 21}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			expected:        tuples{{1, 10, 1, 11}, {2, 20, 2, 12}, {2, 21, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			expectedOutCols: []int{0, 1, 2, 3},
			outputBatchSize: 1,
		},
		{
			description:     "multi output column test, R run",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {1, 111}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			expected:        tuples{{1, 10, 1, 11}, {1, 10, 1, 111}, {2, 20, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			expectedOutCols: []int{0, 1, 2, 3},
			outputBatchSize: ColBatchSize,
		},
		{
			description:     "multi output column test, R run, batch size = 1",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {1, 111}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			expected:        tuples{{1, 10, 1, 11}, {1, 10, 1, 111}, {2, 20, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			expectedOutCols: []int{0, 1, 2, 3},
			outputBatchSize: 1,
		},
		{
			description:     "logic test",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{-1, -1}, {0, 4}, {2, 1}, {3, 4}, {5, 4}},
			rightTuples:     tuples{{0, 5}, {1, 3}, {3, 2}, {4, 6}},
			leftOutCols:     []uint32{1},
			rightOutCols:    []uint32{1},
			expected:        tuples{{5, 4}, {2, 4}},
			expectedOutCols: []int{3, 1},
			outputBatchSize: ColBatchSize,
		},
		{
			description:  "multi output column test, batch size = 1 and runs (to test saved output), reordered out columns",
			leftTypes:    []types.T{types.Int64, types.Int64},
			rightTypes:   []types.T{types.Int64, types.Int64},
			leftTuples:   tuples{{1, 10}, {1, 10}, {1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  tuples{{1, 11}, {1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{1, 0},
			rightOutCols: []uint32{1, 0},
			expected: tuples{
				{10, 1, 11, 1},
				{10, 1, 11, 1},
				{10, 1, 11, 1},
				{10, 1, 11, 1},
				{10, 1, 11, 1},
				{10, 1, 11, 1},
				{20, 2, 12, 2},
				{30, 3, 13, 3},
				{40, 4, 14, 4},
			},
			expectedOutCols: []int{1, 0, 3, 2},
			outputBatchSize: 1,
		},
		{
			description:  "multi output column test, batch size = 1 and runs (to test saved output), reordered out columns that dont start at 0",
			leftTypes:    []types.T{types.Int64, types.Int64},
			rightTypes:   []types.T{types.Int64, types.Int64},
			leftTuples:   tuples{{1, 10}, {1, 10}, {1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  tuples{{1, 11}, {1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{1, 0},
			rightOutCols: []uint32{1},
			expected: tuples{
				{10, 1, 11},
				{10, 1, 11},
				{10, 1, 11},
				{10, 1, 11},
				{10, 1, 11},
				{10, 1, 11},
				{20, 2, 12},
				{30, 3, 13},
				{40, 4, 14},
			},
			expectedOutCols: []int{1, 0, 3},
			outputBatchSize: 1,
		},
	}

	for _, tc := range tcs {
		runTests(t, []tuples{tc.leftTuples, tc.rightTuples}, func(t *testing.T, input []Operator) {
			s := NewMergeJoinOp(input[0], input[1], tc.leftOutCols, tc.rightOutCols, tc.leftTypes, tc.rightTypes, []uint32{0}, []uint32{0})

			out := newOpTestOutput(s, tc.expectedOutCols, tc.expected)
			s.(*mergeJoinOp).initWithBatchSize(tc.outputBatchSize)

			if err := out.Verify(); err != nil {
				t.Fatalf("Test case description: '%s'\n%v", tc.description, err)
			}
		})
	}
}

func TestMergeJoinerLongMultiBatch(t *testing.T) {
	// This test creates one long input of a 1:1 join, and keeps track of the expected
	// output to make sure the join output is batched correctly.
	for _, groupSize := range []int{1, 2, ColBatchSize / 4, ColBatchSize / 2} {
		for _, numInputBatches := range []int{1, 2, 16} {
			for _, outBatchSize := range []int{1, 16, ColBatchSize} {
				t.Run(fmt.Sprintf("groupSize=%d/numInputBatches=%d", groupSize, numInputBatches),
					func(t *testing.T) {
						nTuples := ColBatchSize * numInputBatches
						typs := []types.T{types.Int64}
						cols := []ColVec{newMemColumn(typs[0], nTuples)}
						groups := cols[0].Int64()
						for i := range groups {
							groups[i] = int64(i)
						}

						leftSource := newChunkingBatchSource(typs, cols, uint64(nTuples))
						rightSource := newChunkingBatchSource(typs, cols, uint64(nTuples))

						a := NewMergeJoinOp(
							leftSource,
							rightSource,
							[]uint32{0},
							[]uint32{0},
							typs,
							typs,
							[]uint32{0},
							[]uint32{0},
						)

						a.(*mergeJoinOp).initWithBatchSize(outBatchSize)

						i := 0
						// Keep track of the last comparison value.
						lastVal := int64(0)
						for b := a.Next(); b.Length() != 0; b = a.Next() {
							outCol := b.ColVec(0).Int64()
							for j := int64(0); j < int64(b.Length()); j++ {
								outVal := outCol[j]
								expVal := lastVal
								if outVal != expVal {
									t.Fatalf("Found val %d, expected %d, idx %d of batch %d", outVal, expVal, j, i)
								}
								lastVal++
							}
							i++
						}
					})
			}
		}
	}
}

func TestMergeJoinerLongMultiBatchCount(t *testing.T) {
	// This test creates one long input of a 1:1 join, and keeps track of the expected
	// count to make sure the join output is batched correctly.
	for _, groupSize := range []int{1, 2, ColBatchSize / 4, ColBatchSize / 2} {
		for _, numInputBatches := range []int{1, 2, 16} {
			for _, outBatchSize := range []int{1, 16, ColBatchSize} {
				t.Run(fmt.Sprintf("groupSize=%d/numInputBatches=%d", groupSize, numInputBatches),
					func(t *testing.T) {
						nTuples := ColBatchSize * numInputBatches
						typs := []types.T{types.Int64}
						cols := []ColVec{newMemColumn(typs[0], nTuples)}
						groups := cols[0].Int64()
						for i := range groups {
							groups[i] = int64(i)
						}

						leftSource := newChunkingBatchSource(typs, cols, uint64(nTuples))
						rightSource := newChunkingBatchSource(typs, cols, uint64(nTuples))

						a := NewMergeJoinOp(
							leftSource,
							rightSource,
							[]uint32{},
							[]uint32{},
							typs,
							typs,
							[]uint32{0},
							[]uint32{0},
						)

						a.(*mergeJoinOp).initWithBatchSize(outBatchSize)

						count := 0
						for b := a.Next(); b.Length() != 0; b = a.Next() {
							count += int(b.Length())
						}
						if count != nTuples {
							t.Fatalf("Found count %d, expected count %d", count, nTuples)
						}
					})
			}
		}
	}
}

func newBatchOfIntRows(nCols int, batch ColBatch) ColBatch {
	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < ColBatchSize; i++ {
			col[i] = int64(i)
		}
	}

	batch.SetLength(ColBatchSize)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		vec := batch.ColVec(colIdx)
		vec.UnsetNulls()
	}
	return batch
}

func newBatchOfRepeatedIntRows(nCols int, batch ColBatch, numRepeats int) ColBatch {
	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < ColBatchSize; i++ {
			col[i] = int64((i + 1) / numRepeats)
		}
	}

	batch.SetLength(ColBatchSize)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		vec := batch.ColVec(colIdx)
		vec.UnsetNulls()
	}
	return batch
}

func BenchmarkMergeJoiner(b *testing.B) {
	nCols := 4
	sourceTypes := make([]types.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		sourceTypes[colIdx] = types.Int64
	}

	batch := NewMemBatch(sourceTypes)

	// 1:1
	for _, nBatches := range []int{1, 4, 16, 1024} {
		b.Run(fmt.Sprintf("rows=%d", nBatches*ColBatchSize), func(b *testing.B) {
			// 8 (bytes / int64) * nBatches (number of batches) * ColBatchSize (rows /
			// batch) * nCols (number of columns / row) * 2 (number of sources).
			b.SetBytes(int64(8 * nBatches * ColBatchSize * nCols * 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				leftSource := newFiniteBatchSource(newBatchOfIntRows(nCols, batch), nBatches)
				rightSource := newFiniteBatchSource(newBatchOfIntRows(nCols, batch), nBatches)

				s := mergeJoinOp{
					left: mergeJoinInput{
						eqCols:      []uint32{0},
						outCols:     []uint32{0, 1},
						sourceTypes: sourceTypes,
						source:      leftSource,
					},

					right: mergeJoinInput{
						eqCols:      []uint32{0},
						outCols:     []uint32{2, 3},
						sourceTypes: sourceTypes,
						source:      rightSource,
					},
				}

				s.Init()

				b.StartTimer()
				for i := 0; i < nBatches; i++ {
					s.Next()
				}
				b.StopTimer()
			}
		})
	}

	// left repeats
	for _, nBatches := range []int{1, 4, 16, 1024} {
		b.Run(fmt.Sprintf("oneSideRepeat-rows=%d", nBatches*ColBatchSize), func(b *testing.B) {
			// 8 (bytes / int64) * nBatches (number of batches) * ColBatchSize (rows /
			// batch) * nCols (number of columns / row) * 2 (number of sources).
			b.SetBytes(int64(8 * nBatches * ColBatchSize * nCols * 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				leftSource := newFiniteBatchSource(newBatchOfRepeatedIntRows(nCols, batch, nBatches), nBatches)
				rightSource := newFiniteBatchSource(newBatchOfIntRows(nCols, batch), nBatches)

				s := mergeJoinOp{
					left: mergeJoinInput{
						eqCols:      []uint32{0},
						outCols:     []uint32{0, 1},
						sourceTypes: sourceTypes,
						source:      leftSource,
					},

					right: mergeJoinInput{
						eqCols:      []uint32{0},
						outCols:     []uint32{2, 3},
						sourceTypes: sourceTypes,
						source:      rightSource,
					},
				}

				s.Init()

				b.StartTimer()
				for i := 0; i < nBatches; i++ {
					s.Next()
				}
				b.StopTimer()
			}
		})
	}

	// both repeats
	for _, nBatches := range []int{1, 4, 16, 1024} {
		b.Run(fmt.Sprintf("bothSidesRepeat-rows=%d", nBatches*ColBatchSize), func(b *testing.B) {
			// 8 (bytes / int64) * nBatches (number of batches) * ColBatchSize (rows /
			// batch) * nCols (number of columns / row) * 2 (number of sources).
			b.SetBytes(int64(8 * nBatches * ColBatchSize * nCols * 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				leftSource := newFiniteBatchSource(newBatchOfRepeatedIntRows(nCols, batch, nBatches), nBatches)
				rightSource := newFiniteBatchSource(newBatchOfRepeatedIntRows(nCols, batch, nBatches), nBatches)

				s := mergeJoinOp{
					left: mergeJoinInput{
						eqCols:      []uint32{0},
						outCols:     []uint32{0, 1},
						sourceTypes: sourceTypes,
						source:      leftSource,
					},

					right: mergeJoinInput{
						eqCols:      []uint32{0},
						outCols:     []uint32{2, 3},
						sourceTypes: sourceTypes,
						source:      rightSource,
					},
				}

				s.Init()

				b.StartTimer()
				for i := 0; i < nBatches; i++ {
					s.Next()
				}
				b.StopTimer()
			}
		})
	}

}
