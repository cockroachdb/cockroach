// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

func TestCoalescer(t *testing.T) {
	// Large tuple number for coalescing.
	nRows := coldata.BatchSize*3 + 7
	large := make(tuples, nRows)
	largeTypes := []types.T{types.Int64}

	for i := 0; i < nRows; i++ {
		large[i] = tuple{int64(i)}
	}

	tcs := []struct {
		colTypes []types.T
		tuples   tuples
	}{
		{
			colTypes: []types.T{types.Int64, types.Bytes},
			tuples: tuples{
				{0, "0"},
				{1, "1"},
				{2, "2"},
				{3, "3"},
				{4, "4"},
				{5, "5"},
			},
		},
		{
			colTypes: largeTypes,
			tuples:   large,
		},
	}

	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, func(t *testing.T, input []Operator) {
			coalescer := NewCoalescerOp(input[0], tc.colTypes)

			colIndices := make([]int, len(tc.colTypes))
			for i := 0; i < len(colIndices); i++ {
				colIndices[i] = i
			}

			out := newOpTestOutput(coalescer, colIndices, tc.tuples)

			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkCoalescer(b *testing.B) {
	ctx := context.Background()
	// The input operator to the coalescer returns a batch of random size from [1,
	// col.BatchSize) each time.
	nCols := 4
	sourceTypes := make([]types.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		sourceTypes[colIdx] = types.Int64
	}

	batch := coldata.NewMemBatch(sourceTypes)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := int64(0); i < coldata.BatchSize; i++ {
			col[i] = i
		}
	}

	for _, nBatches := range []int{1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		b.Run(fmt.Sprintf("rows=%d", nBatches*int(coldata.BatchSize)), func(b *testing.B) {
			b.SetBytes(int64(8 * nBatches * int(coldata.BatchSize) * nCols))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				source := newRandomLengthBatchSource(batch)

				co := &coalescerOp{
					input:      source,
					inputTypes: sourceTypes,
				}

				co.Init()

				for i := 0; i < nBatches; i++ {
					co.Next(ctx)
				}
			}
		})
	}
}
