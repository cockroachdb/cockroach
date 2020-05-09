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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestDeselector(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		typs     []*types.T
		tuples   []tuple
		sel      []int
		expected []tuple
	}{
		{
			typs:     []*types.T{types.Int},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      nil,
			expected: tuples{{0}, {1}, {2}},
		},
		{
			typs:     []*types.T{types.Int},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      []int{},
			expected: tuples{},
		},
		{
			typs:     []*types.T{types.Int},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      []int{1},
			expected: tuples{{1}},
		},
		{
			typs:     []*types.T{types.Int},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      []int{0, 2},
			expected: tuples{{0}, {2}},
		},
		{
			typs:     []*types.T{types.Int},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      []int{0, 1, 2},
			expected: tuples{{0}, {1}, {2}},
		},
	}

	for _, tc := range tcs {
		runTestsWithFixedSel(t, []tuples{tc.tuples}, tc.typs, tc.sel, func(t *testing.T, input []colexecbase.Operator) {
			op := NewDeselectorOp(testAllocator, input[0], tc.typs)
			out := newOpTestOutput(op, tc.expected)

			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkDeselector(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	nCols := 1
	inputTypes := make([]*types.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		inputTypes[colIdx] = types.Int
	}

	batch := testAllocator.NewMemBatch(inputTypes)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < coldata.BatchSize(); i++ {
			col[i] = int64(i)
		}
	}
	for _, probOfOmitting := range []float64{0.1, 0.9} {
		sel := coldatatestutils.RandomSel(rng, coldata.BatchSize(), probOfOmitting)
		batchLen := len(sel)

		for _, nBatches := range []int{1 << 1, 1 << 2, 1 << 4, 1 << 8} {
			b.Run(fmt.Sprintf("rows=%d/after selection=%d", nBatches*coldata.BatchSize(), nBatches*batchLen), func(b *testing.B) {
				// We're measuring the amount of data that is not selected out.
				b.SetBytes(int64(8 * nBatches * batchLen * nCols))
				batch.SetSelection(true)
				copy(batch.Selection(), sel)
				batch.SetLength(batchLen)
				input := colexecbase.NewRepeatableBatchSource(testAllocator, batch, inputTypes)
				op := NewDeselectorOp(testAllocator, input, inputTypes)
				op.Init()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					input.ResetBatchesToReturn(nBatches)
					for b := op.Next(ctx); b.Length() != 0; b = op.Next(ctx) {
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
