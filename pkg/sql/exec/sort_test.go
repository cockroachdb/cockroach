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
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestSort(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tcs := []struct {
		tuples   tuples
		expected tuples
		ordCols  []distsqlpb.Ordering_Column
		typ      []types.T
	}{
		{
			tuples:   tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected: tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			typ:      []types.T{types.Int64},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			expected: tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			typ:      []types.T{types.Int64},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{1, 1}, {3, 2}, {2, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}},
			expected: tuples{{1, 1}, {2, 3}, {3, 2}, {4, 4}, {5, 5}, {6, 6}, {7, 7}},
			typ:      []types.T{types.Int64, types.Int64},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{1, 1}, {5, 2}, {3, 3}, {7, 4}, {2, 5}, {6, 6}, {4, 7}},
			expected: tuples{{1, 1}, {2, 5}, {3, 3}, {4, 7}, {5, 2}, {6, 6}, {7, 4}},
			typ:      []types.T{types.Int64, types.Int64},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{1}, {5}, {3}, {3}, {2}, {6}, {4}},
			expected: tuples{{1}, {2}, {3}, {3}, {4}, {5}, {6}},
			typ:      []types.T{types.Int64},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{false}, {true}},
			expected: tuples{{false}, {true}},
			typ:      []types.T{types.Bool},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{true}, {false}},
			expected: tuples{{false}, {true}},
			typ:      []types.T{types.Bool},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{3.2}, {2.0}, {2.4}},
			expected: tuples{{2.0}, {2.4}, {3.2}},
			typ:      []types.T{types.Float64},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}},
		},

		{
			tuples:   tuples{{0, 1, 0}, {1, 2, 0}, {2, 3, 2}, {3, 7, 1}, {4, 2, 2}},
			expected: tuples{{0, 1, 0}, {1, 2, 0}, {3, 7, 1}, {4, 2, 2}, {2, 3, 2}},
			typ:      []types.T{types.Int64, types.Int64, types.Int64},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 2}, {ColIdx: 1}},
		},

		{
			// ensure that sort partitions stack: make sure that a run of identical
			// values in a later column doesn't get sorted if the run is broken up
			// by previous columns.
			tuples: tuples{
				{0, 1, 0},
				{0, 1, 0},
				{0, 1, 1},
				{0, 0, 1},
				{0, 0, 0},
			},
			expected: tuples{
				{0, 0, 0},
				{0, 0, 1},
				{0, 1, 0},
				{0, 1, 0},
				{0, 1, 1},
			},
			typ:     []types.T{types.Int64, types.Int64, types.Int64},
			ordCols: []distsqlpb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}, {ColIdx: 2}},
		},
	}
	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, func(t *testing.T, input []Operator) {
			sort, err := NewSorter(input[0], tc.typ, tc.ordCols)
			if err != nil {
				t.Fatal(err)
			}
			cols := make([]int, len(tc.typ))
			for i := range cols {
				cols[i] = i
			}
			out := newOpTestOutput(sort, cols, tc.expected)

			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSortRandomized(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	nCols := 2
	nTups := 8
	typs := make([]types.T, nCols)
	ordCols := make([]distsqlpb.Ordering_Column, nCols)
	for i := range typs {
		ordCols[i].ColIdx = uint32(i)
		ordCols[i].Direction = distsqlpb.Ordering_Column_Direction(rng.Int() % 2)
		typs[i] = types.Int64
	}
	tups := make(tuples, nTups)
	for i := range tups {
		tups[i] = make(tuple, nCols)
		for j := range tups[i] {
			// Small range so we can test partitioning
			tups[i][j] = rng.Int63() % 2048
		}
	}

	expected := make(tuples, nTups)
	copy(expected, tups)
	sort.Slice(expected, func(i, j int) bool {
		for k := 0; k < nCols; k++ {
			l := ordCols[k].ColIdx
			if expected[i][l].(int64) < expected[j][l].(int64) {
				return ordCols[k].Direction == distsqlpb.Ordering_Column_ASC
			} else if expected[i][l].(int64) > expected[j][l].(int64) {
				return ordCols[k].Direction == distsqlpb.Ordering_Column_DESC
			}
		}
		return false
	})

	runTests(t, []tuples{tups}, func(t *testing.T, input []Operator) {
		sorter, err := NewSorter(input[0], typs, ordCols)
		if err != nil {
			t.Fatal(err)
		}
		cols := make([]int, len(typs))
		for i := range cols {
			cols[i] = i
		}
		out := newOpTestOutput(sorter, cols, expected)

		if err := out.Verify(); err != nil {
			t.Fatalf("for input %v:\n%v", tups, err)
		}
	})
}

func BenchmarkSort(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	for _, nBatches := range []int{1 << 1, 1 << 4, 1 << 8} {
		for _, nCols := range []int{1, 2, 4} {
			b.Run(fmt.Sprintf("rows=%d/cols=%d", nBatches*ColBatchSize, nCols), func(b *testing.B) {
				// 8 (bytes / int64) * nBatches (number of batches) * ColBatchSize (rows /
				// batch) * nCols (number of columns / row).
				b.SetBytes(int64(8 * nBatches * ColBatchSize * nCols))
				typs := make([]types.T, nCols)
				for i := range typs {
					typs[i] = types.Int64
				}
				batch := NewMemBatch(typs)
				batch.SetLength(ColBatchSize)
				ordCols := make([]distsqlpb.Ordering_Column, nCols)
				for i := range ordCols {
					ordCols[i].ColIdx = uint32(i)
					ordCols[i].Direction = distsqlpb.Ordering_Column_Direction(rng.Int() % 2)

					col := batch.ColVec(i).Int64()
					for j := 0; j < ColBatchSize; j++ {
						col[j] = rng.Int63() % int64((j*1024)+1)
					}
				}
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					source := newFiniteBatchSource(batch, nBatches)
					sort, err := NewSorter(source, typs, ordCols)
					if err != nil {
						b.Fatal(err)
					}

					sort.Init()
					for i := 0; i < nBatches; i++ {
						out := sort.Next()
						if out.Length() == 0 {
							b.Fail()
						}
					}
				}
			})
		}
	}
}

func TestChunker(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tcs := []struct {
		tuples   tuples
		expected []tuples
		matchLen int
		typ      []types.T
	}{
		{
			tuples: tuples{{0, 2}},
			expected: []tuples{
				{{0, 2}},
			},
			typ:      []types.T{types.Int64, types.Int64},
			matchLen: 1,
		},
		{
			tuples: tuples{{0, 2}, {0, 2}},
			expected: []tuples{
				{{0, 2}, {0, 2}},
			},
			typ:      []types.T{types.Int64, types.Int64},
			matchLen: 1,
		},
		{
			tuples: tuples{{0, 2}, {0, 1}, {0, 3}, {0, 1}},
			expected: []tuples{
				{{0, 1}, {0, 2}, {0, 3}, {0, 1}},
			},
			typ:      []types.T{types.Int64, types.Int64},
			matchLen: 1,
		},
		{
			tuples: tuples{{0, 1}, {0, 1}, {0, 3}, {2, 3}, {2, 1}},
			expected: []tuples{
				{{0, 1}, {0, 1}, {0, 3}},
				{{2, 1}, {2, 3}},
			},
			typ:      []types.T{types.Int64, types.Int64},
			matchLen: 1,
		},
		{
			tuples: tuples{{0, 2}, {0, 1}, {0, 3}, {2, 3}, {2, 4}, {2, 1}},
			expected: []tuples{
				{{0, 2}, {0, 1}, {0, 3}},
				{{2, 3}, {2, 4}, {2, 1}},
			},
			typ:      []types.T{types.Int64, types.Int64},
			matchLen: 1,
		},
		{
			tuples: tuples{{0, 0, 2}, {0, 0, 1}, {0, 2, 3}, {0, 2, 3}, {0, 2, 4}, {1, 2, 1}},
			expected: []tuples{
				{{0, 0, 2}, {0, 0, 1}},
				{{0, 2, 3}, {0, 2, 3}, {0, 2, 4}},
				{{1, 2, 1}},
			},
			typ:      []types.T{types.Int64, types.Int64, types.Int64},
			matchLen: 2,
		},
	}
	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, func(t *testing.T, input []Operator) {
			s, err := newChunker(input[0], tc.typ, tc.matchLen)
			if err != nil {
				t.Fatal(err)
			}
			cols := make([]int, len(tc.typ))
			for i := range cols {
				cols[i] = i
			}
			chunk := 0
			for !s.inputDone {
				out := newOpTestOutput(s, cols, tc.expected[chunk])
				if err := out.VerifyAnyOrder(); err != nil {
					t.Fatal(err)
				}
				chunk++
			}
		})
	}
}

func TestSortChunks(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tcs := []struct {
		tuples   tuples
		expected tuples
		typ      []types.T
		ordCols  []distsqlpb.Ordering_Column
		matchLen int
	}{
		{
			tuples:   tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			expected: tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			typ:      []types.T{types.Int64},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}},
			matchLen: 1,
		},
		{
			tuples:   tuples{{1, 2}, {1, 2}, {1, 3}, {1, 1}, {5, 5}, {6, 6}, {6, 1}},
			expected: tuples{{1, 1}, {1, 2}, {1, 2}, {1, 3}, {5, 5}, {6, 1}, {6, 6}},
			typ:      []types.T{types.Int64, types.Int64},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}},
			matchLen: 1,
		},
		{
			tuples: tuples{
				{0, 1, 0},
				{0, 1, 0},
				{0, 1, 1},
				{0, 0, 1},
				{0, 0, 0},
			},
			expected: tuples{
				{0, 0, 0},
				{0, 0, 1},
				{0, 1, 0},
				{0, 1, 0},
				{0, 1, 1},
			},
			typ:      []types.T{types.Int64, types.Int64, types.Int64},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}, {ColIdx: 2}},
			matchLen: 1,
		},
		{
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
			typ:      []types.T{types.Int64, types.Int64, types.Int64},
			ordCols:  []distsqlpb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}, {ColIdx: 2}},
			matchLen: 2,
		},
	}
	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, func(t *testing.T, input []Operator) {
			sorter, err := NewSortChunks(input[0], tc.typ, tc.ordCols, tc.matchLen)
			if err != nil {
				t.Fatal(err)
			}
			cols := make([]int, len(tc.typ))
			for i := range cols {
				cols[i] = i
			}
			out := newOpTestOutput(sorter, cols, tc.expected)

			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSortChunksRandomized(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	nCols := 2
	nTups := 8
	typs := make([]types.T, nCols)
	ordCols := make([]distsqlpb.Ordering_Column, nCols)
	for i := range typs {
		ordCols[i].ColIdx = uint32(i)
		ordCols[i].Direction = distsqlpb.Ordering_Column_Direction(rng.Int() % 2)
		typs[i] = types.Int64
	}
	tups := make(tuples, nTups)
	for i := range tups {
		tups[i] = make(tuple, nCols)
		for j := range tups[i] {
			// Small range so we can test partitioning
			tups[i][j] = rng.Int63() % 2048
		}
	}

	// Sort tups on the first column needed for sort chunks operator.
	sortedTups := make(tuples, nTups)
	copy(sortedTups, tups)
	sort.Slice(sortedTups, func(i, j int) bool {
		if sortedTups[i][ordCols[0].ColIdx].(int64) < sortedTups[j][ordCols[0].ColIdx].(int64) {
			return ordCols[0].Direction == distsqlpb.Ordering_Column_ASC
		} else if sortedTups[i][ordCols[0].ColIdx].(int64) > sortedTups[j][ordCols[0].ColIdx].(int64) {
			return ordCols[0].Direction == distsqlpb.Ordering_Column_DESC
		}
		return false
	})

	expected := make(tuples, nTups)
	copy(expected, tups)
	sort.Slice(expected, func(i, j int) bool {
		for k := 0; k < nCols; k++ {
			l := ordCols[k].ColIdx
			if expected[i][l].(int64) < expected[j][l].(int64) {
				return ordCols[k].Direction == distsqlpb.Ordering_Column_ASC
			} else if expected[i][l].(int64) > expected[j][l].(int64) {
				return ordCols[k].Direction == distsqlpb.Ordering_Column_DESC
			}
		}
		return false
	})

	runTests(t, []tuples{sortedTups}, func(t *testing.T, input []Operator) {
		sorter, err := NewSortChunks(input[0], typs, ordCols, 1 /* matchLen */)
		if err != nil {
			t.Fatal(err)
		}
		cols := make([]int, len(typs))
		for i := range cols {
			cols[i] = i
		}
		out := newOpTestOutput(sorter, cols, expected)

		if err := out.Verify(); err != nil {
			t.Fatalf("for input %v:\n%v", sortedTups, err)
		}
	})
}

func BenchmarkSortChunks(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	sorterConstructors := []func(Operator, []types.T, []distsqlpb.Ordering_Column, int) (Operator, error){
		NewSortChunks,
		func(input Operator, inputTypes []types.T, orderingCols []distsqlpb.Ordering_Column, _ int) (Operator, error) {
			return NewSorter(input, inputTypes, orderingCols)
		},
	}
	sorterNames := []string{"CHUNKS", "ALL"}
	for _, nBatches := range []int{1 << 2, 1 << 6} {
		for _, nCols := range []int{2, 4} {
			for _, matchLen := range []int{1, 2, 3} {
				for _, avgChunkSize := range []int{1 << 2, 1 << 5, 1 << 8} {
					for sorterIdx, sorterConstructor := range sorterConstructors {
						if matchLen >= nCols {
							continue
						}
						b.Run(
							fmt.Sprintf("%s/rows=%d/cols=%d/matchLen=%d/avgChunkSize=%d",
								sorterNames[sorterIdx], nBatches*ColBatchSize, nCols, matchLen, avgChunkSize),
							func(b *testing.B) {
								// 8 (bytes / int64) * nBatches (number of batches) * ColBatchSize (rows /
								// batch) * nCols (number of columns / row).
								b.SetBytes(int64(8 * nBatches * ColBatchSize * nCols))
								typs := make([]types.T, nCols)
								for i := range typs {
									typs[i] = types.Int64
								}
								batch := NewMemBatch(typs)
								batch.SetLength(ColBatchSize)
								ordCols := make([]distsqlpb.Ordering_Column, nCols)
								for i := range ordCols {
									ordCols[i].ColIdx = uint32(i)
									if i < matchLen {
										ordCols[i].Direction = distsqlpb.Ordering_Column_Direction(distsqlpb.Ordering_Column_ASC)
									} else {
										ordCols[i].Direction = distsqlpb.Ordering_Column_Direction(rng.Int() % 2)
									}

									col := batch.ColVec(i).Int64()
									col[0] = 0
									for j := 1; j < ColBatchSize; j++ {
										if i < matchLen {
											col[j] = col[j-1]
											if rng.Float64() < 1.0/float64(avgChunkSize) {
												col[j]++
											}
										} else {
											col[j] = rng.Int63() % int64((j*1024)+1)
										}
									}
								}
								rowsTotal := nBatches * ColBatchSize
								b.ResetTimer()
								for n := 0; n < b.N; n++ {
									source := newFiniteChunksSource(batch, nBatches, matchLen)
									sorter, err := sorterConstructor(source, typs, ordCols, matchLen)
									if err != nil {
										b.Fatal(err)
									}

									sorter.Init()
									rowsEmitted := 0
									for rowsEmitted < rowsTotal {
										out := sorter.Next()
										if out.Length() == 0 {
											b.Fail()
										}
										rowsEmitted += int(out.Length())
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
