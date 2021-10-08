// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecutils

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestDeselector(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tcs := []struct {
		typs     []*types.T
		tuples   []colexectestutils.Tuple
		sel      []int
		expected []colexectestutils.Tuple
	}{
		{
			typs:     []*types.T{types.Int},
			tuples:   colexectestutils.Tuples{{0}, {1}, {2}},
			sel:      nil,
			expected: colexectestutils.Tuples{{0}, {1}, {2}},
		},
		{
			typs:     []*types.T{types.Int},
			tuples:   colexectestutils.Tuples{{0}, {1}, {2}},
			sel:      []int{},
			expected: colexectestutils.Tuples{},
		},
		{
			typs:     []*types.T{types.Int},
			tuples:   colexectestutils.Tuples{{0}, {1}, {2}},
			sel:      []int{1},
			expected: colexectestutils.Tuples{{1}},
		},
		{
			typs:     []*types.T{types.Int},
			tuples:   colexectestutils.Tuples{{0}, {1}, {2}},
			sel:      []int{0, 2},
			expected: colexectestutils.Tuples{{0}, {2}},
		},
		{
			typs:     []*types.T{types.Int},
			tuples:   colexectestutils.Tuples{{0}, {1}, {2}},
			sel:      []int{0, 1, 2},
			expected: colexectestutils.Tuples{{0}, {1}, {2}},
		},
	}

	for _, tc := range tcs {
		colexectestutils.RunTestsWithFixedSel(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, tc.typs, tc.sel, func(t *testing.T, input []colexecop.Operator) {
			op := NewDeselectorOp(testAllocator, input[0], tc.typs)
			out := colexectestutils.NewOpTestOutput(op, tc.expected)

			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkDeselector(b *testing.B) {
	defer log.Scope(b).Close(b)
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	nCols := 1
	inputTypes := make([]*types.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		inputTypes[colIdx] = types.Int
	}

	batch := testAllocator.NewMemBatchWithMaxCapacity(inputTypes)

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
				input := colexecop.NewRepeatableBatchSource(testAllocator, batch, inputTypes)
				op := NewDeselectorOp(testAllocator, input, inputTypes)
				op.Init(ctx)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					input.ResetBatchesToReturn(nBatches)
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
