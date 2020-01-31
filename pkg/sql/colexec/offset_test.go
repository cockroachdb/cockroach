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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		// The tuples consisting of all nulls still count as separate rows, so if
		// we replace all values with nulls, we should get the same output.
		runTestsWithoutAllNullsInjection(t, []tuples{tc.tuples}, nil /* typs */, tc.expected, unorderedVerifier, func(input []Operator) (Operator, error) {
			return NewOffsetOp(input[0], tc.offset), nil
		})
	}
}

func BenchmarkOffset(b *testing.B) {
	ctx := context.Background()
	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64})
	batch.SetLength(coldata.BatchSize())
	source := NewRepeatableBatchSource(testAllocator, batch)
	source.Init()

	o := NewOffsetOp(source, 1)
	// Set throughput proportional to size of the selection vector.
	b.SetBytes(int64(2 * coldata.BatchSize()))
	for i := 0; i < b.N; i++ {
		o.(*offsetOp).Reset()
		o.Next(ctx)
	}
}
