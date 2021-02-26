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
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

var sortAllTestCases []sortTestCase

func init() {
	sortAllTestCases = []sortTestCase{
		{
			tuples:   colexectestutils.Tuples{{1}, {2}, {nil}, {4}, {5}, {nil}},
			expected: colexectestutils.Tuples{{nil}, {nil}, {1}, {2}, {4}, {5}},
			typs:     []*types.T{types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   colexectestutils.Tuples{{1, 2}, {1, 1}, {1, nil}, {2, nil}, {2, 3}, {2, nil}, {5, 1}},
			expected: colexectestutils.Tuples{{1, nil}, {1, 1}, {1, 2}, {2, nil}, {2, nil}, {2, 3}, {5, 1}},
			typs:     []*types.T{types.Int, types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}},
		},
		{
			tuples:   colexectestutils.Tuples{{1, 2}, {1, 1}, {1, nil}, {2, nil}, {2, 3}, {2, nil}, {5, 1}},
			expected: colexectestutils.Tuples{{5, 1}, {2, 3}, {2, nil}, {2, nil}, {1, 2}, {1, 1}, {1, nil}},
			typs:     []*types.T{types.Int, types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_DESC}, {ColIdx: 1, Direction: execinfrapb.Ordering_Column_DESC}},
		},
		{
			tuples:   colexectestutils.Tuples{{nil, nil}, {nil, 3}, {1, nil}, {nil, 1}, {1, 2}, {nil, nil}, {5, nil}},
			expected: colexectestutils.Tuples{{nil, nil}, {nil, nil}, {nil, 1}, {nil, 3}, {1, nil}, {1, 2}, {5, nil}},
			typs:     []*types.T{types.Int, types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}},
		},
		{
			tuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected: colexectestutils.Tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			typs:     []*types.T{types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   colexectestutils.Tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			expected: colexectestutils.Tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			typs:     []*types.T{types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   colexectestutils.Tuples{{1, 1}, {3, 2}, {2, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}},
			expected: colexectestutils.Tuples{{1, 1}, {2, 3}, {3, 2}, {4, 4}, {5, 5}, {6, 6}, {7, 7}},
			typs:     []*types.T{types.Int, types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   colexectestutils.Tuples{{1, 1}, {5, 2}, {3, 3}, {7, 4}, {2, 5}, {6, 6}, {4, 7}},
			expected: colexectestutils.Tuples{{1, 1}, {2, 5}, {3, 3}, {4, 7}, {5, 2}, {6, 6}, {7, 4}},
			typs:     []*types.T{types.Int, types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   colexectestutils.Tuples{{1}, {5}, {3}, {3}, {2}, {6}, {4}},
			expected: colexectestutils.Tuples{{1}, {2}, {3}, {3}, {4}, {5}, {6}},
			typs:     []*types.T{types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   colexectestutils.Tuples{{false}, {true}},
			expected: colexectestutils.Tuples{{false}, {true}},
			typs:     []*types.T{types.Bool},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   colexectestutils.Tuples{{true}, {false}},
			expected: colexectestutils.Tuples{{false}, {true}},
			typs:     []*types.T{types.Bool},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   colexectestutils.Tuples{{3.2}, {2.0}, {2.4}, {math.NaN()}, {math.Inf(-1)}, {math.Inf(1)}},
			expected: colexectestutils.Tuples{{math.NaN()}, {math.Inf(-1)}, {2.0}, {2.4}, {3.2}, {math.Inf(1)}},
			typs:     []*types.T{types.Float},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},

		{
			tuples:   colexectestutils.Tuples{{0, 1, 0}, {1, 2, 0}, {2, 3, 2}, {3, 7, 1}, {4, 2, 2}},
			expected: colexectestutils.Tuples{{0, 1, 0}, {1, 2, 0}, {3, 7, 1}, {4, 2, 2}, {2, 3, 2}},
			typs:     []*types.T{types.Int, types.Int, types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 2}, {ColIdx: 1}},
		},

		{
			// ensure that sort partitions stack: make sure that a run of identical
			// values in a later column doesn't get sorted if the run is broken up
			// by previous columns.
			tuples: colexectestutils.Tuples{
				{0, 1, 0},
				{0, 1, 0},
				{0, 1, 1},
				{0, 0, 1},
				{0, 0, 0},
			},
			expected: colexectestutils.Tuples{
				{0, 0, 0},
				{0, 0, 1},
				{0, 1, 0},
				{0, 1, 0},
				{0, 1, 1},
			},
			typs:    []*types.T{types.Int, types.Int, types.Int},
			ordCols: []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}, {ColIdx: 2}},
		},
	}
}

func TestSort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	for _, tc := range sortAllTestCases {
		colexectestutils.RunTestsWithTyps(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, [][]*types.T{tc.typs}, tc.expected, colexectestutils.OrderedVerifier,
			func(input []colexecop.Operator) (colexecop.Operator, error) {
				return NewSorter(testAllocator, input[0], tc.typs, tc.ordCols)
			})
	}
}

func TestSortRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewPseudoRand()
	nTups := coldata.BatchSize()*2 + 1
	maxCols := 3
	// TODO(yuzefovich): randomize types as well.
	typs := make([]*types.T, maxCols)
	for i := range typs {
		typs[i] = types.Int
	}
	for nCols := 1; nCols < maxCols; nCols++ {
		for nOrderingCols := 1; nOrderingCols <= nCols; nOrderingCols++ {
			for _, k := range []int{0, rng.Intn(nTups) + 1} {
				topK := k != 0
				name := fmt.Sprintf("nCols=%d/nOrderingCols=%d/topK=%t", nCols, nOrderingCols, topK)
				log.Infof(context.Background(), "%s", name)
				tups, expected, ordCols := generateRandomDataForTestSort(rng, nTups, nCols, nOrderingCols)
				if topK {
					expected = expected[:k]
				}
				colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tups}, expected, colexectestutils.OrderedVerifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
					if topK {
						return NewTopKSorter(testAllocator, input[0], typs[:nCols], ordCols, uint64(k)), nil
					}
					return NewSorter(testAllocator, input[0], typs[:nCols], ordCols)
				})
			}
		}
	}
}

// generateRandomDataForTestSort is a utility function that generates data to
// be used in randomized unit test of a sort operation. It returns:
// - tups - the data to be sorted
// - expected - the same data but already sorted
// - ordCols - ordering columns used in the sort operation.
func generateRandomDataForTestSort(
	rng *rand.Rand, nTups, nCols, nOrderingCols int,
) (tups, expected colexectestutils.Tuples, ordCols []execinfrapb.Ordering_Column) {
	ordCols = generateColumnOrdering(rng, nCols, nOrderingCols)
	tups = make(colexectestutils.Tuples, nTups)
	for i := range tups {
		tups[i] = make(colexectestutils.Tuple, nCols)
		for j := range tups[i] {
			// Small range so we can test partitioning
			if rng.Float64() < nullProbability {
				tups[i][j] = nil
			} else {
				tups[i][j] = rng.Int63() % 2048
			}
		}
		// Enforce that the last ordering column is always unique. Otherwise
		// there would be multiple valid sort orders.
		tups[i][ordCols[nOrderingCols-1].ColIdx] = int64(i)
	}

	expected = make(colexectestutils.Tuples, nTups)
	copy(expected, tups)
	sort.Slice(expected, less(expected, ordCols))
	return tups, expected, ordCols
}

func TestAllSpooler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tcs := []struct {
		tuples colexectestutils.Tuples
		typ    []*types.T
	}{
		{
			tuples: colexectestutils.Tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			typ:    []*types.T{types.Int},
		},
		{
			tuples: colexectestutils.Tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			typ:    []*types.T{types.Int},
		},
		{
			tuples: colexectestutils.Tuples{{1, 1}, {3, 2}, {2, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}},
			typ:    []*types.T{types.Int, types.Int},
		},
		{
			tuples: colexectestutils.Tuples{{1, 1}, {5, 2}, {3, 3}, {7, 4}, {2, 5}, {6, 6}, {4, 7}},
			typ:    []*types.T{types.Int, types.Int},
		},
		{
			tuples: colexectestutils.Tuples{{1}, {5}, {3}, {3}, {2}, {6}, {4}},
			typ:    []*types.T{types.Int},
		},
		{
			tuples: colexectestutils.Tuples{{0, 1, 0}, {1, 2, 0}, {2, 3, 2}, {3, 7, 1}, {4, 2, 2}},
			typ:    []*types.T{types.Int, types.Int, types.Int},
		},
		{
			tuples: colexectestutils.Tuples{
				{0, 1, 0},
				{0, 1, 0},
				{0, 1, 1},
				{0, 0, 1},
				{0, 0, 0},
			},
			typ: []*types.T{types.Int, types.Int, types.Int},
		},
	}
	for _, tc := range tcs {
		colexectestutils.RunTestsWithFn(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, nil, func(t *testing.T, input []colexecop.Operator) {
			allSpooler := newAllSpooler(testAllocator, input[0], tc.typ)
			allSpooler.init(context.Background())
			allSpooler.spool()
			if len(tc.tuples) != allSpooler.getNumTuples() {
				t.Fatal(fmt.Sprintf("allSpooler spooled wrong number of tuples: expected %d, but received %d", len(tc.tuples), allSpooler.getNumTuples()))
			}
			if allSpooler.getPartitionsCol() != nil {
				t.Fatal("allSpooler returned non-nil partitionsCol")
			}
			for col := 0; col < len(tc.typ); col++ {
				colVec := allSpooler.getValues(col).Int64()
				for i := 0; i < allSpooler.getNumTuples(); i++ {
					if colVec[i] != int64(tc.tuples[i][col].(int)) {
						t.Fatal(fmt.Sprintf("allSpooler returned wrong value in %d column of %d'th tuple : expected %v, but received %v",
							col, i, tc.tuples[i][col].(int), colVec[i]))
					}
				}
			}
		})
	}
}

func BenchmarkSort(b *testing.B) {
	defer log.Scope(b).Close(b)
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()
	k := uint64(128)

	for _, nBatches := range []int{1 << 1, 1 << 4, 1 << 8} {
		for _, nCols := range []int{1, 2, 4} {
			for _, topK := range []bool{false, true} {
				name := fmt.Sprintf("rows=%d/cols=%d/topK=%t", nBatches*coldata.BatchSize(), nCols, topK)
				b.Run(name, func(b *testing.B) {
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
						ordCols[i].Direction = execinfrapb.Ordering_Column_Direction(rng.Int() % 2)

						col := batch.ColVec(i).Int64()
						for j := 0; j < coldata.BatchSize(); j++ {
							col[j] = rng.Int63() % int64((i*1024)+1)
						}
					}
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						source := colexectestutils.NewFiniteBatchSource(testAllocator, batch, typs, nBatches)
						var sorter colexecop.Operator
						if topK {
							sorter = NewTopKSorter(testAllocator, source, typs, ordCols, k)
						} else {
							var err error
							sorter, err = NewSorter(testAllocator, source, typs, ordCols)
							if err != nil {
								b.Fatal(err)
							}
						}
						sorter.Init(ctx)
						for out := sorter.Next(); out.Length() != 0; out = sorter.Next() {
						}
					}
				})
			}
		}
	}
}

func BenchmarkAllSpooler(b *testing.B) {
	defer log.Scope(b).Close(b)
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	for _, nBatches := range []int{1 << 1, 1 << 4, 1 << 8} {
		for _, nCols := range []int{1, 2, 4} {
			b.Run(fmt.Sprintf("rows=%d/cols=%d", nBatches*coldata.BatchSize(), nCols), func(b *testing.B) {
				// 8 (bytes / int64) * nBatches (number of batches) * col.BatchSize() (rows /
				// batch) * nCols (number of columns / row).
				b.SetBytes(int64(8 * nBatches * coldata.BatchSize() * nCols))
				typs := make([]*types.T, nCols)
				for i := range typs {
					typs[i] = types.Int
				}
				batch := testAllocator.NewMemBatchWithMaxCapacity(typs)
				batch.SetLength(coldata.BatchSize())
				for i := 0; i < nCols; i++ {
					col := batch.ColVec(i).Int64()
					for j := 0; j < coldata.BatchSize(); j++ {
						col[j] = rng.Int63() % int64((i*1024)+1)
					}
				}
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					source := colexectestutils.NewFiniteBatchSource(testAllocator, batch, typs, nBatches)
					allSpooler := newAllSpooler(testAllocator, source, typs)
					allSpooler.init(ctx)
					allSpooler.spool()
				}
			})
		}
	}
}

func less(
	tuples colexectestutils.Tuples, ordCols []execinfrapb.Ordering_Column,
) func(i, j int) bool {
	return func(i, j int) bool {
		for _, col := range ordCols {
			n1 := tuples[i][col.ColIdx] == nil
			n2 := tuples[j][col.ColIdx] == nil
			if col.Direction == execinfrapb.Ordering_Column_ASC {
				if n1 && n2 {
					continue
				} else if n1 {
					return true
				} else if n2 {
					return false
				}
			} else {
				if n1 && n2 {
					continue
				} else if n1 {
					return false
				} else if n2 {
					return true
				}
			}
			if tuples[i][col.ColIdx].(int64) < tuples[j][col.ColIdx].(int64) {
				return col.Direction == execinfrapb.Ordering_Column_ASC
			} else if tuples[i][col.ColIdx].(int64) > tuples[j][col.ColIdx].(int64) {
				return col.Direction == execinfrapb.Ordering_Column_DESC
			}
		}
		return false
	}
}

// generateColumnOrdering produces a random ordering of nOrderingCols columns
// on a table with nCols columns, so nOrderingCols must be not greater than
// nCols.
func generateColumnOrdering(
	rng *rand.Rand, nCols int, nOrderingCols int,
) []execinfrapb.Ordering_Column {
	if nOrderingCols > nCols {
		colexecerror.InternalError(errors.AssertionFailedf("nOrderingCols > nCols in generateColumnOrdering"))
	}
	orderingCols := make([]execinfrapb.Ordering_Column, nOrderingCols)
	for i, col := range rng.Perm(nCols)[:nOrderingCols] {
		orderingCols[i] = execinfrapb.Ordering_Column{ColIdx: uint32(col), Direction: execinfrapb.Ordering_Column_Direction(rng.Intn(2))}
	}
	return orderingCols
}
