// Copyright 2019 The Cockroach Authors.
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
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

var sortChunksTestCases []sortTestCase

func init() {
	sortChunksTestCases = []sortTestCase{
		{
			description: `three chunks`,
			tuples:      tuples{{1, 2}, {1, 2}, {1, 3}, {1, 1}, {5, 5}, {6, 6}, {6, 1}},
			expected:    tuples{{1, 1}, {1, 2}, {1, 2}, {1, 3}, {5, 5}, {6, 1}, {6, 6}},
			logTypes:    []types.T{*types.Int, *types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}},
			matchLen:    1,
		},
		{
			description: `simple nulls asc`,
			tuples:      tuples{{1, 2}, {1, nil}, {1, 3}, {1, 1}, {5, 5}, {6, 6}, {6, nil}},
			expected:    tuples{{1, nil}, {1, 1}, {1, 2}, {1, 3}, {5, 5}, {6, nil}, {6, 6}},
			logTypes:    []types.T{*types.Int, *types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}},
			matchLen:    1,
		},
		{
			description: `simple nulls desc`,
			tuples:      tuples{{1, 2}, {1, nil}, {1, 3}, {1, 1}, {5, 5}, {6, 6}, {6, nil}},
			expected:    tuples{{1, 3}, {1, 2}, {1, 1}, {1, nil}, {5, 5}, {6, 6}, {6, nil}},
			logTypes:    []types.T{*types.Int, *types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1, Direction: execinfrapb.Ordering_Column_DESC}},
			matchLen:    1,
		},
		{
			description: `one chunk, matchLen 1, three ordering columns`,
			tuples: tuples{
				{0, 1, 2},
				{0, 2, 0},
				{0, 1, 0},
				{0, 1, 1},
				{0, 2, 1},
			},
			expected: tuples{
				{0, 1, 0},
				{0, 1, 1},
				{0, 1, 2},
				{0, 2, 0},
				{0, 2, 1},
			},
			logTypes: []types.T{*types.Int, *types.Int, *types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}, {ColIdx: 2}},
			matchLen: 1,
		},
		{
			description: `two chunks, matchLen 1, three ordering columns`,
			tuples: tuples{
				{0, 1, 2},
				{0, 2, 0},
				{0, 1, 0},
				{1, 2, 1},
				{1, 1, 1},
			},
			expected: tuples{
				{0, 1, 0},
				{0, 1, 2},
				{0, 2, 0},
				{1, 1, 1},
				{1, 2, 1},
			},
			logTypes: []types.T{*types.Int, *types.Int, *types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}, {ColIdx: 2}},
			matchLen: 1,
		},
		{
			description: `two chunks, matchLen 2, three ordering columns`,
			tuples: tuples{
				{0, 1, 2},
				{0, 1, 0},
				{0, 1, 1},
				{0, 2, 1},
				{0, 2, 0},
			},
			expected: tuples{
				{0, 1, 0},
				{0, 1, 1},
				{0, 1, 2},
				{0, 2, 0},
				{0, 2, 1},
			},
			logTypes: []types.T{*types.Int, *types.Int, *types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}, {ColIdx: 2}},
			matchLen: 2,
		},
		{
			description: `four chunks, matchLen 2, three ordering columns`,
			tuples: tuples{
				{0, 1, 2},
				{0, 1, 0},
				{0, 2, 0},
				{1, 1, 1},
				{1, 2, 1},
			},
			expected: tuples{
				{0, 1, 0},
				{0, 1, 2},
				{0, 2, 0},
				{1, 1, 1},
				{1, 2, 1},
			},
			logTypes: []types.T{*types.Int, *types.Int, *types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}, {ColIdx: 2}},
			matchLen: 2,
		},
		{
			description: `three chunks, matchLen 1, three ordering columns (reordered)`,
			tuples: tuples{
				{0, 2, 0},
				{0, 1, 0},
				{1, 1, 1},
				{0, 1, 1},
				{0, 1, 2},
			},
			expected: tuples{
				{0, 1, 0},
				{0, 2, 0},
				{0, 1, 1},
				{1, 1, 1},
				{0, 1, 2},
			},
			logTypes: []types.T{*types.Int, *types.Int, *types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 2}, {ColIdx: 1}, {ColIdx: 0}},
			matchLen: 1,
		},
		{
			description: `four chunks, matchLen 2, three ordering columns (reordered)`,
			tuples: tuples{
				{0, 2, 0},
				{0, 1, 0},
				{1, 1, 1},
				{1, 2, 1},
				{0, 1, 2},
				{1, 2, 2},
				{1, 1, 2},
			},
			expected: tuples{
				{0, 1, 0},
				{0, 2, 0},
				{1, 1, 1},
				{1, 2, 1},
				{0, 1, 2},
				{1, 1, 2},
				{1, 2, 2},
			},
			logTypes: []types.T{*types.Int, *types.Int, *types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 2}, {ColIdx: 0}, {ColIdx: 1}},
			matchLen: 2,
		},
	}
}

func TestSortChunks(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range sortChunksTestCases {
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier, func(input []Operator) (Operator, error) {
			physTypes, err := typeconv.FromColumnTypes(tc.logTypes)
			if err != nil {
				return nil, err
			}
			return NewSortChunks(testAllocator, input[0], physTypes, tc.ordCols, tc.matchLen)
		})
	}
}

func TestSortChunksRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, _ := randutil.NewPseudoRand()
	nTups := 8
	maxCols := 5
	// TODO(yuzefovich): randomize types as well.
	typs := make([]coltypes.T, maxCols)
	for i := range typs {
		typs[i] = coltypes.Int64
	}

	for nCols := 1; nCols < maxCols; nCols++ {
		for nOrderingCols := 1; nOrderingCols <= nCols; nOrderingCols++ {
			for matchLen := 1; matchLen < nOrderingCols; matchLen++ {
				ordCols := generateColumnOrdering(rng, nCols, nOrderingCols)
				tups := make(tuples, nTups)
				for i := range tups {
					tups[i] = make(tuple, nCols)
					for j := range tups[i] {
						// Small range so we can test partitioning.
						tups[i][j] = rng.Int63() % 2048
					}
				}

				// Sort tups on the first matchLen columns as needed for sort chunks
				// operator.
				sortedTups := make(tuples, nTups)
				copy(sortedTups, tups)
				sort.Slice(sortedTups, less(sortedTups, ordCols[:matchLen]))

				// Sort tups on all ordering columns to get the expected results.
				expected := make(tuples, nTups)
				copy(expected, tups)
				sort.Slice(expected, less(expected, ordCols))

				runTests(t, []tuples{sortedTups}, expected, orderedVerifier, func(input []Operator) (Operator, error) {
					return NewSortChunks(testAllocator, input[0], typs[:nCols], ordCols, matchLen)
				})
			}
		}
	}
}

func BenchmarkSortChunks(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	sorterConstructors := []func(*Allocator, Operator, []coltypes.T, []execinfrapb.Ordering_Column, int) (Operator, error){
		NewSortChunks,
		func(allocator *Allocator, input Operator, inputTypes []coltypes.T, orderingCols []execinfrapb.Ordering_Column, _ int) (Operator, error) {
			return NewSorter(allocator, input, inputTypes, orderingCols)
		},
	}
	sorterNames := []string{"CHUNKS", "ALL"}
	for _, nBatches := range []int{1 << 2, 1 << 6} {
		for _, nCols := range []int{2, 4} {
			for _, matchLen := range []int{1, 2, 3} {
				for _, avgChunkSize := range []int{1 << 3, 1 << 7} {
					for sorterIdx, sorterConstructor := range sorterConstructors {
						if matchLen >= nCols {
							continue
						}
						b.Run(
							fmt.Sprintf("%s/rows=%d/cols=%d/matchLen=%d/avgChunkSize=%d",
								sorterNames[sorterIdx], nBatches*coldata.BatchSize(), nCols, matchLen, avgChunkSize),
							func(b *testing.B) {
								// 8 (bytes / int64) * nBatches (number of batches) * coldata.BatchSize() (rows /
								// batch) * nCols (number of columns / row).
								b.SetBytes(int64(8 * nBatches * coldata.BatchSize() * nCols))
								typs := make([]coltypes.T, nCols)
								for i := range typs {
									typs[i] = coltypes.Int64
								}
								batch := testAllocator.NewMemBatch(typs)
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
									source := newFiniteChunksSource(batch, nBatches, matchLen)
									sorter, err := sorterConstructor(testAllocator, source, typs, ordCols, matchLen)
									if err != nil {
										b.Fatal(err)
									}

									sorter.Init()
									for out := sorter.Next(ctx); out.Length() != 0; out = sorter.Next(ctx) {
									}
								}
								b.StopTimer()
							})
					}
				}
			}
		}
	}
}
