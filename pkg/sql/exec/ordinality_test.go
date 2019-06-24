// Copyright 2019 The Cockroach Authors.
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
)

func TestOrdinality(t *testing.T) {
	tcs := []struct {
		tuples   []tuple
		expected []tuple
	}{
		{
			tuples:   tuples{{1}},
			expected: tuples{{1, 1}},
		},
		{
			tuples:   tuples{{}, {}, {}, {}, {}},
			expected: tuples{{1}, {2}, {3}, {4}, {5}},
		},
		{
			tuples:   tuples{{5}, {6}, {7}, {8}},
			expected: tuples{{5, 1}, {6, 2}, {7, 3}, {8, 4}},
		},
		{
			tuples:   tuples{{5, 'a'}, {6, 'b'}, {7, 'c'}, {8, 'd'}},
			expected: tuples{{5, 'a', 1}, {6, 'b', 2}, {7, 'c', 3}, {8, 'd', 4}},
		},
	}

	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, func(t *testing.T, input []Operator) {
			numExpectedCols := len(tc.expected[0])
			ordinality := NewOrdinalityOp(input[0])
			out := newOpTestOutput(ordinality, []int{0, 1, 2}[:numExpectedCols], tc.expected)

			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkOrdinality(b *testing.B) {
	ctx := context.Background()

	batch := coldata.NewMemBatch([]types.T{types.Int64, types.Int64, types.Int64})
	batch.SetLength(coldata.BatchSize)
	source := NewRepeatableBatchSource(batch)
	source.Init()

	ordinality := NewOrdinalityOp(source)

	b.SetBytes(int64(8 * coldata.BatchSize * batch.Width()))
	for i := 0; i < b.N; i++ {
		ordinality.Next(ctx)
	}
}
