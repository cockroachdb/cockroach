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

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestHashJoinerInt64(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up the apd.Decimal values used in tests.
	floats := []float64{0.314, 3.14, 31.4, 314}
	decs := make([]apd.Decimal, len(floats))
	for i, f := range floats {
		_, err := decs[i].SetFloat64(f)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
	}

	tcs := []struct {
		leftTypes  []types.T
		rightTypes []types.T

		leftTuples  tuples
		rightTuples tuples

		leftEqCols  []uint32
		rightEqCols []uint32

		leftOutCols  []uint32
		rightOutCols []uint32

		buildRightSide bool
		buildDistinct  bool

		expectedTuples tuples
	}{
		{
			// Test handling of multiple column non-distinct equality keys.
			leftTypes:  []types.T{types.Int64, types.Int64, types.Int64},
			rightTypes: []types.T{types.Int64, types.Int64, types.Int64},

			leftTuples: tuples{
				{0, 0, 1},
				{0, 0, 2},
				{1, 0, 3},
				{1, 1, 4},
				{1, 1, 5},
				{0, 0, 6},
			},
			rightTuples: tuples{
				{1, 0, 7},
				{0, 0, 8},
				{0, 0, 9},
				{0, 1, 10},
			},

			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			leftOutCols:  []uint32{2},
			rightOutCols: []uint32{2},

			expectedTuples: tuples{
				{3, 7},
				{6, 8},
				{1, 8},
				{2, 8},
				{6, 9},
				{1, 9},
				{2, 9},
			},
		},
		{
			// Test handling of duplicate equality keys that map to same buckets.
			leftTypes:  []types.T{types.Int64},
			rightTypes: []types.T{types.Int64},

			leftTuples: tuples{
				{0},
				{hashTableBucketSize},
				{hashTableBucketSize},
				{hashTableBucketSize},
				{0},
				{hashTableBucketSize * 2},
				{1},
				{1},
				{hashTableBucketSize + 1},
			},
			rightTuples: tuples{
				{hashTableBucketSize},
				{hashTableBucketSize * 2},
				{hashTableBucketSize * 3},
				{0},
				{1},
				{hashTableBucketSize + 1},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},

			buildDistinct: false,

			expectedTuples: tuples{
				{hashTableBucketSize, hashTableBucketSize},
				{hashTableBucketSize, hashTableBucketSize},
				{hashTableBucketSize, hashTableBucketSize},
				{hashTableBucketSize * 2, hashTableBucketSize * 2},
				{0, 0},
				{0, 0},
				{1, 1},
				{1, 1},
				{hashTableBucketSize + 1, hashTableBucketSize + 1},
			},
		},
		{
			// Test handling of duplicate equality keys.
			leftTypes:  []types.T{types.Int64},
			rightTypes: []types.T{types.Int64},

			leftTuples: tuples{
				{0},
				{0},
				{1},
				{1},
				{1},
				{2},
			},
			rightTuples: tuples{
				{1},
				{0},
				{2},
				{2},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},

			buildDistinct: false,

			expectedTuples: tuples{
				{1},
				{1},
				{1},
				{0},
				{0},
				{2},
				{2},
			},
		},
		{
			// Test handling of various output column types.
			leftTypes:  []types.T{types.Bool, types.Int64, types.Bytes, types.Int64},
			rightTypes: []types.T{types.Int64, types.Float64, types.Int32},

			leftTuples: tuples{
				{false, 5, "a", 10},
				{true, 3, "b", 30},
				{false, 2, "foo", 20},
				{false, 6, "bar", 50},
			},
			rightTuples: tuples{
				{1, 1.1, int32(1)},
				{2, 2.2, int32(2)},
				{3, 3.3, int32(4)},
				{4, 4.4, int32(8)},
				{5, 5.5, int32(16)},
			},

			leftEqCols:   []uint32{1},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{1, 2},
			rightOutCols: []uint32{0, 2},

			buildDistinct: true,

			expectedTuples: tuples{
				{2, "foo", 2, int32(2)},
				{3, "b", 3, int32(4)},
				{5, "a", 5, int32(16)},
			},
		},
		{
			leftTypes:  []types.T{types.Int64},
			rightTypes: []types.T{types.Int64},

			// Reverse engineering hash table hash heuristic to find key values that
			// hash to the same bucket.
			leftTuples: tuples{
				{0},
				{hashTableBucketSize},
				{hashTableBucketSize * 2},
				{hashTableBucketSize * 3},
			},
			rightTuples: tuples{
				{0},
				{hashTableBucketSize},
				{hashTableBucketSize * 3},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},

			buildDistinct: true,

			expectedTuples: tuples{
				{0},
				{hashTableBucketSize},
				{hashTableBucketSize * 3},
			},
		},
		{
			leftTypes:  []types.T{types.Int64},
			rightTypes: []types.T{types.Int64},

			// Test a N:1 inner join where the right side key has duplicate values.
			leftTuples: tuples{
				{0},
				{1},
				{2},
				{3},
				{4},
			},
			rightTuples: tuples{
				{1},
				{1},
				{1},
				{2},
				{2},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},

			buildDistinct: true,

			expectedTuples: tuples{
				{1, 1},
				{1, 1},
				{1, 1},
				{2, 2},
				{2, 2},
			},
		},
		{
			leftTypes:  []types.T{types.Int64, types.Int64, types.Int64},
			rightTypes: []types.T{types.Int64, types.Int64, types.Int64},

			// Test inner join on multiple equality columns.
			leftTuples: tuples{
				{0, 0, 10},
				{0, 1, 20},
				{0, 2, 30},
				{1, 1, 40},
				{1, 2, 50},
				{2, 0, 60},
				{2, 1, 70},
			},
			rightTuples: tuples{
				{0, 100, 2},
				{1, 200, 1},
				{2, 300, 0},
				{2, 400, 1},
			},

			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 2},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{1},

			buildDistinct: true,

			expectedTuples: tuples{
				{0, 2, 30, 100},
				{1, 1, 40, 200},
				{2, 0, 60, 300},
				{2, 1, 70, 400},
			},
		},
		{
			leftTypes:  []types.T{types.Int64, types.Int64, types.Int64},
			rightTypes: []types.T{types.Int64, types.Int64},

			// Test multiple column with values that hash to the same bucket.
			leftTuples: tuples{
				{10, 0, 0},
				{20, 0, hashTableBucketSize},
				{40, hashTableBucketSize, 0},
				{50, hashTableBucketSize, hashTableBucketSize},
				{60, hashTableBucketSize * 2, 0},
				{70, hashTableBucketSize * 2, hashTableBucketSize},
			},
			rightTuples: tuples{
				{0, hashTableBucketSize},
				{hashTableBucketSize * 2, hashTableBucketSize},
				{0, 0},
				{0, hashTableBucketSize * 2},
			},

			leftEqCols:   []uint32{1, 2},
			rightEqCols:  []uint32{0, 1},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{},

			buildDistinct: true,

			expectedTuples: tuples{
				{20, 0, hashTableBucketSize},
				{70, hashTableBucketSize * 2, hashTableBucketSize},
				{10, 0, 0},
			},
		},
		{
			leftTypes:  []types.T{types.Bytes, types.Bool, types.Int8, types.Int16, types.Int32, types.Int64, types.Bytes},
			rightTypes: []types.T{types.Int64, types.Int32, types.Int16, types.Int8, types.Bool, types.Bytes},

			// Test multiple equality columns of different types.
			leftTuples: tuples{
				{"foo", false, int8(10), int16(100), int32(1000), int64(10000), "aaa"},
				{"foo", true, 10, 100, 1000, 10000, "bbb"},
				{"foo1", false, 10, 100, 1000, 10000, "ccc"},
				{"foo", false, 20, 100, 1000, 10000, "ddd"},
				{"foo", false, 10, 200, 1000, 10000, "eee"},
				{"bar", true, 30, 300, 3000, 30000, "fff"},
			},
			rightTuples: tuples{
				{int64(10000), int32(1000), int16(100), int8(10), false, "foo1"},
				{10000, 1000, 100, 10, false, "foo"},
				{30000, 3000, 300, 30, true, "bar"},
				{10000, 1000, 100, 20, false, "foo"},
				{30000, 3000, 300, 30, false, "bar"},
				{10000, 1000, 100, 10, false, "random"},
			},

			leftEqCols:   []uint32{0, 1, 2, 3, 4, 5},
			rightEqCols:  []uint32{5, 4, 3, 2, 1, 0},
			leftOutCols:  []uint32{6},
			rightOutCols: []uint32{},

			buildDistinct: true,

			expectedTuples: tuples{
				{"ccc"},
				{"aaa"},
				{"fff"},
				{"ddd"},
			},
		},
		{
			leftTypes:  []types.T{types.Float32, types.Float64},
			rightTypes: []types.T{types.Float64, types.Float32},

			// Test equality columns of type float.
			leftTuples: tuples{
				{float32(1.1), float64(33.333)},
				{float32(1.1), float64(44.4444)},
				{float32(2.22), float64(55.55555)},
				{float32(2.22), float64(44.4444)},
			},
			rightTuples: tuples{
				{float64(44.4444), float32(2.22)},
				{float64(55.55555), float32(1.1)},
				{float64(33.333), float32(1.1)},
			},

			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{1, 0},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},

			buildDistinct: true,

			expectedTuples: tuples{
				{float32(2.22), float64(44.4444)},
				{float32(1.1), float64(33.333)},
			},
		},
		{
			leftTypes:  []types.T{types.Int64, types.Int64, types.Int64, types.Int64},
			rightTypes: []types.T{types.Int64, types.Int64, types.Int64, types.Int64},

			// Test use right side as build table.
			leftTuples: tuples{
				{2, 4, 8, 16},
				{3, 3, 2, 2},
				{3, 7, 2, 1},
				{5, 4, 3, 2},
			},
			rightTuples: tuples{
				{1, 3, 5, 7},
				{1, 1, 1, 1},
				{1, 2, 3, 4},
			},

			leftEqCols:   []uint32{2, 0},
			rightEqCols:  []uint32{1, 2},
			leftOutCols:  []uint32{0, 1, 2, 3},
			rightOutCols: []uint32{0, 1, 2, 3},

			buildRightSide: true,
			buildDistinct:  true,

			expectedTuples: tuples{
				{3, 3, 2, 2, 1, 2, 3, 4},
				{3, 7, 2, 1, 1, 2, 3, 4},
				{5, 4, 3, 2, 1, 3, 5, 7},
			},
		},
		{
			leftTypes:  []types.T{types.Decimal},
			rightTypes: []types.T{types.Decimal},

			// Test types.Decimal type as equality column.
			leftTuples: tuples{
				{decs[0]},
				{decs[1]},
				{decs[2]},
			},
			rightTuples: tuples{
				{decs[2]},
				{decs[3]},
				{decs[0]},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{},
			rightOutCols: []uint32{0},

			buildDistinct: true,

			expectedTuples: tuples{
				{decs[2]},
				{decs[0]},
			},
		},
	}

	for _, tc := range tcs {
		inputs := []tuples{tc.leftTuples, tc.rightTuples}

		buildFlags := []bool{false}
		if tc.buildDistinct {
			buildFlags = append(buildFlags, true)
		}

		for _, buildDistinct := range buildFlags {
			t.Run(fmt.Sprintf("buildDistinct=%v", buildDistinct), func(t *testing.T) {
				runTests(t, inputs, []types.T{types.Bool}, func(t *testing.T, sources []Operator) {
					leftSource, rightSource := sources[0], sources[1]

					spec := hashJoinerSpec{
						left: hashJoinerSourceSpec{
							eqCols:      tc.leftEqCols,
							outCols:     tc.leftOutCols,
							sourceTypes: tc.leftTypes,
							source:      leftSource,
						},

						right: hashJoinerSourceSpec{
							eqCols:      tc.rightEqCols,
							outCols:     tc.rightOutCols,
							sourceTypes: tc.rightTypes,
							source:      rightSource,
						},

						buildRightSide: tc.buildRightSide,
						buildDistinct:  tc.buildDistinct,
					}

					hj := &hashJoinEqInnerOp{
						spec: spec,
					}

					nOutCols := len(tc.leftOutCols) + len(tc.rightOutCols)
					nLeftOutCols := uint32(len(tc.leftOutCols))
					nLeftCols := uint32(len(tc.leftTypes))

					cols := make([]int, nOutCols)

					for i, colIdx := range tc.leftOutCols {
						cols[i] = int(colIdx)
					}
					for i, colIdx := range tc.rightOutCols {
						cols[uint32(i)+nLeftOutCols] = int(colIdx + nLeftCols)
					}

					out := newOpTestOutput(hj, cols, tc.expectedTuples)

					if err := out.Verify(); err != nil {
						t.Fatal(err)
					}
				})
			})

		}
	}
}

func BenchmarkHashJoiner(b *testing.B) {
	nCols := 4
	sourceTypes := make([]types.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		sourceTypes[colIdx] = types.Int64
	}

	batch := NewMemBatch(sourceTypes)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < ColBatchSize; i++ {
			col[i] = int64(i)
		}
	}

	batch.SetLength(ColBatchSize)

	for _, buildDistinct := range []bool{true, false} {
		b.Run(fmt.Sprintf("distinct=%v", buildDistinct), func(b *testing.B) {
			for _, nBatches := range []int{1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
				b.Run(fmt.Sprintf("rows=%d", nBatches*ColBatchSize), func(b *testing.B) {
					// 8 (bytes / int64) * nBatches (number of batches) * ColBatchSize (rows /
					// batch) * nCols (number of columns / row) * 2 (number of sources).
					b.SetBytes(int64(8 * nBatches * ColBatchSize * nCols * 2))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						leftSource := newFiniteBatchSource(batch, nBatches)
						rightSource := newRepeatableBatchSource(batch)

						spec := hashJoinerSpec{
							left: hashJoinerSourceSpec{
								eqCols:      []uint32{0, 2},
								outCols:     []uint32{0, 1},
								sourceTypes: sourceTypes,
								source:      leftSource,
							},

							right: hashJoinerSourceSpec{
								eqCols:      []uint32{1, 3},
								outCols:     []uint32{2, 3},
								sourceTypes: sourceTypes,
								source:      rightSource,
							},

							buildDistinct: buildDistinct,
						}

						hj := &hashJoinEqInnerOp{
							spec: spec,
						}

						hj.Init()

						for i := 0; i < nBatches; i++ {
							// Technically, the non-distinct hash join will produce much more
							// than nBatches of output.
							hj.Next()
						}
					}
				})
			}
		})
	}
}
