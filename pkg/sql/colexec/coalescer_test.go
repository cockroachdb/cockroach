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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestCoalescer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Large tuple number for coalescing.
	nRows := int(coldata.BatchSize()*3 + 7)
	large := make(tuples, nRows)
	largeTypes := []coltypes.T{coltypes.Int64}

	for i := 0; i < nRows; i++ {
		large[i] = tuple{int64(i)}
	}

	tcs := []struct {
		colTypes []coltypes.T
		tuples   tuples
	}{
		{
			colTypes: []coltypes.T{coltypes.Int64, coltypes.Bytes},
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
		runTests(t, []tuples{tc.tuples}, tc.tuples, orderedVerifier, func(input []Operator) (Operator, error) {
			return NewCoalescerOp(input[0], tc.colTypes), nil
		})
	}
}

func BenchmarkCoalescer(b *testing.B) {
	ctx := context.Background()
	// The input operator to the coalescer returns a batch of random size from [1,
	// coldata.BatchSize()) each time.
	nCols := 4
	sourceTypes := make([]coltypes.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		sourceTypes[colIdx] = coltypes.Int64
	}

	batch := coldata.NewMemBatch(sourceTypes)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := int64(0); i < int64(coldata.BatchSize()); i++ {
			col[i] = i
		}
	}

	for _, nBatches := range []int{1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		b.Run(fmt.Sprintf("rows=%d", nBatches*int(coldata.BatchSize())), func(b *testing.B) {
			b.SetBytes(int64(8 * nBatches * int(coldata.BatchSize()) * nCols))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				source := newRandomLengthBatchSource(batch)

				co := &coalescerOp{
					OneInputNode: NewOneInputNode(source),
					inputTypes:   sourceTypes,
				}

				co.Init()

				for i := 0; i < nBatches; i++ {
					co.Next(ctx)
				}
			}
		})
	}
}
