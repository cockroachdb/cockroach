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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestSortedDistinct(t *testing.T) {
	tcs := []struct {
		distinctCols []uint32
		colTypes     []types.T
		numCols      int
		tuples       []tuple
		expected     []tuple
	}{
		{
			distinctCols: []uint32{0, 1, 2},
			colTypes:     []types.T{types.Float64, types.Int64, types.Bytes},
			numCols:      4,
			tuples: tuples{
				{1.0, 2, "30", 4},
				{1.0, 2, "30", 5},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{2.0, 3, "40", 4},
				{2.0, 3, "40", 4},
			},
			expected: tuples{
				{1.0, 2, "30", 4},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{2.0, 3, "40", 4},
			},
		},
		{
			distinctCols: []uint32{1, 0, 2},
			colTypes:     []types.T{types.Float64, types.Int64, types.Bytes},
			numCols:      4,
			tuples: tuples{
				{1.0, 2, "30", 4},
				{1.0, 2, "30", 5},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{2.0, 3, "40", 4},
				{2.0, 3, "40", 4},
			},
			expected: tuples{
				{1.0, 2, "30", 4},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{2.0, 3, "40", 4},
			},
		},
	}

	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, func(t *testing.T, input []Operator) {
			distinct, err := NewOrderedDistinct(input[0], tc.distinctCols, tc.colTypes)
			if err != nil {
				t.Fatal(err)
			}
			out := newOpTestOutput(distinct, []int{0, 1, 2, 3}, tc.expected)

			if err := out.VerifyAnyOrder(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkSortedDistinct(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	batch := coldata.NewMemBatch([]types.T{types.Int64, types.Int64, types.Int64})
	aCol := batch.ColVec(1).Int64()
	bCol := batch.ColVec(2).Int64()
	lastA := int64(0)
	lastB := int64(0)
	for i := 0; i < coldata.BatchSize; i++ {
		// 1/4 chance of changing each distinct coldata.
		if rng.Float64() > 0.75 {
			lastA++
		}
		if rng.Float64() > 0.75 {
			lastB++
		}
		aCol[i] = lastA
		bCol[i] = lastB
	}
	batch.SetLength(coldata.BatchSize)
	source := NewRepeatableBatchSource(batch)
	source.Init()

	distinct, err := NewOrderedDistinct(source, []uint32{1, 2}, []types.T{types.Int64, types.Int64, types.Int64})
	if err != nil {
		b.Fatal(err)
	}

	// don't count the artificial zeroOp'd column in the throughput
	b.SetBytes(int64(8 * coldata.BatchSize * 3))
	for i := 0; i < b.N; i++ {
		distinct.Next(ctx)
	}
}
