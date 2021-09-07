// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var topKSortTestCases []sortTestCase

func init() {
	topKSortTestCases = []sortTestCase{
		{
			description: "k < input length",
			tuples:      colexectestutils.Tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected:    colexectestutils.Tuples{{1}, {2}, {3}},
			typs:        []*types.T{types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}},
			k:           3,
		},
		{
			description: "k > input length",
			tuples:      colexectestutils.Tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected:    colexectestutils.Tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			typs:        []*types.T{types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}},
			k:           10,
		},
		{
			description: "nulls",
			tuples:      colexectestutils.Tuples{{1}, {2}, {nil}, {3}, {4}, {5}, {6}, {7}, {nil}},
			expected:    colexectestutils.Tuples{{nil}, {nil}, {1}},
			typs:        []*types.T{types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}},
			k:           3,
		},
		{
			description: "descending",
			tuples:      colexectestutils.Tuples{{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5}, {1, 5}},
			expected:    colexectestutils.Tuples{{0, 5}, {1, 5}, {0, 4}},
			typs:        []*types.T{types.Int, types.Int},
			ordCols: []execinfrapb.Ordering_Column{
				{ColIdx: 1, Direction: execinfrapb.Ordering_Column_DESC},
				{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC},
			},
			k: 3,
		},
		{
			description: "partial order single col",
			tuples:      colexectestutils.Tuples{{1, 5}, {0, 5}, {0, 4}, {0, 3}, {0, 2}, {0, 1}},
			expected:    colexectestutils.Tuples{{0, 5}, {1, 5}, {0, 4}},
			typs:        []*types.T{types.Int, types.Int},
			ordCols: []execinfrapb.Ordering_Column{
				{ColIdx: 1, Direction: execinfrapb.Ordering_Column_DESC},
				{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC},
			},
			matchLen: 1,
			k: 3,
		},
		{
			description: "partial order multi col",
			tuples:      colexectestutils.Tuples{{0, 5, 2}, {0, 5, 1}, {0, 4, 3}, {0, 3, 3}, {0, 2, 5}, {0, 1, 1}},
			expected:    colexectestutils.Tuples{{0, 5, 1}, {0, 5, 2}, {0, 4, 3}},
			typs:        []*types.T{types.Int, types.Int, types.Int},
			ordCols: []execinfrapb.Ordering_Column{
				{ColIdx: 1, Direction: execinfrapb.Ordering_Column_DESC},
				{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC},
				{ColIdx: 2, Direction: execinfrapb.Ordering_Column_ASC},
			},
			matchLen: 2,
			k: 3,
		},
	}
}

func TestTopKSorter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range topKSortTestCases {
		log.Infof(context.Background(), "%s", tc.description)
		colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, tc.expected, colexectestutils.OrderedVerifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
			return NewTopKSorter(testAllocator, input[0], tc.typs, tc.ordCols, tc.matchLen, tc.k, execinfra.DefaultMemoryLimit)
		})
	}
}
func BenchmarkSortTopK(b *testing.B) {
	defer log.Scope(b).Close(b)
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()
	k := uint64(128)

	for _, nBatches := range []int{1 << 1, 1 << 4, 1 << 8} {
		for _, nCols := range []int{1, 2, 4} {
			for _, matchLen := range []int{0, 1, 2, 3} {
				for _, avgChunkSize := range []int{1 << 3, 1 << 7} {
						if matchLen >= nCols {
							continue
						}
						b.Run(
							fmt.Sprintf("rows=%d/cols=%d/matchLen=%d/avgChunkSize=%d",
								nBatches*coldata.BatchSize(), nCols, matchLen, avgChunkSize),
							func(b *testing.B) {
								// 8 (bytes / int64) * nBatches (number of batches) * coldata.BatchSize() (rows /
								// batch) * nCols (number of columns / row).
								b.SetBytes(int64(8 * nBatches * coldata.BatchSize() * nCols))
								typs := make([]*types.T, nCols)
								for i := range typs {
									typs[i] = types.Int
								}
								batch := testAllocator.NewMemBatchWithMaxCapacity(typs)
								batch.SetLength(coldata.BatchSize())
								ordCols := make([]execinfrapb.Ordering_Column, nCols)
								for i := range ordCols {
									ordCols[i].ColIdx = uint32(i)
									if i < matchLen {
										ordCols[i].Direction = execinfrapb.Ordering_Column_ASC
									} else {
										ordCols[i].Direction = execinfrapb.Ordering_Column_Direction(rng.Int() % 2)
									}

									col := batch.ColVec(i).Int64()
									col[0] = 0
									for j := 1; j < coldata.BatchSize(); j++ {
										if i < matchLen {
											col[j] = col[j-1]
											if rng.Float64() < 1.0/float64(avgChunkSize) {
												col[j]++
											}
										} else {
											col[j] = rng.Int63() % int64((i*1024)+1)
										}
									}
								}
								b.ResetTimer()
								for n := 0; n < b.N; n++ {
									var sorter colexecop.Operator
									var err error
									source := colexectestutils.NewFiniteChunksSource(testAllocator, batch, typs, nBatches, matchLen)
									sorter, err = NewTopKSorter(testAllocator, source, typs, ordCols, matchLen, k, execinfra.DefaultMemoryLimit)
									if err != nil {
										b.Fatal(err)
									}
									sorter.Init(ctx)
									for out := sorter.Next(); out.Length() != 0; out = sorter.Next() {
									}
								}
								b.StopTimer()
							})
					}
			}
		}
	}
}
