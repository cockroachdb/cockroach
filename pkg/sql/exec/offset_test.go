// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package exec

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

func TestOffset(t *testing.T) {
	tcs := []struct {
		offset   uint64
		tuples   []tuple
		expected []tuple
	}{
		{
			offset:   0,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{{1}, {2}, {3}, {4}},
		},
		{
			offset:   1,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{{2}, {3}, {4}},
		},
		{
			offset:   2,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{{3}, {4}},
		},
		{
			offset:   4,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{},
		},
		{
			offset:   100000,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{},
		},
	}

	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, func(t *testing.T, input []Operator) {
			s := NewOffsetOp(input[0], tc.offset)
			out := newOpTestOutput(s, []int{0}, tc.expected)

			if err := out.VerifyAnyOrder(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkOffset(b *testing.B) {
	ctx := context.Background()
	batch := coldata.NewMemBatch([]types.T{types.Int64, types.Int64, types.Int64})
	batch.SetLength(coldata.BatchSize)
	source := NewRepeatableBatchSource(batch)
	source.Init()

	o := NewOffsetOp(source, 1)
	// Set throughput proportional to size of the selection vector.
	b.SetBytes(2 * coldata.BatchSize)
	for i := 0; i < b.N; i++ {
		o.(*offsetOp).Reset()
		o.Next(ctx)
	}
}
