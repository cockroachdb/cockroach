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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tcs := []struct {
		offset   uint64
		tuples   []colexectestutils.Tuple
		expected []colexectestutils.Tuple
	}{
		{
			offset:   0,
			tuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			expected: colexectestutils.Tuples{{1}, {2}, {3}, {4}},
		},
		{
			offset:   1,
			tuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			expected: colexectestutils.Tuples{{2}, {3}, {4}},
		},
		{
			offset:   2,
			tuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			expected: colexectestutils.Tuples{{3}, {4}},
		},
		{
			offset:   4,
			tuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			expected: colexectestutils.Tuples{},
		},
		{
			offset:   100000,
			tuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			expected: colexectestutils.Tuples{},
		},
	}

	for _, tc := range tcs {
		// The tuples consisting of all nulls still count as separate rows, so if
		// we replace all values with nulls, we should get the same output.
		colexectestutils.RunTestsWithoutAllNullsInjection(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, nil, tc.expected, colexectestutils.UnorderedVerifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
			return NewOffsetOp(input[0], tc.offset), nil
		})
	}
}

func BenchmarkOffset(b *testing.B) {
	ctx := context.Background()
	typs := []*types.T{types.Int, types.Int, types.Int}
	batch := testAllocator.NewMemBatchWithMaxCapacity(typs)
	batch.SetLength(coldata.BatchSize())
	source := colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)

	o := NewOffsetOp(source, 1)
	o.Init(ctx)
	// Set throughput proportional to size of the selection vector.
	b.SetBytes(int64(2 * coldata.BatchSize()))
	for i := 0; i < b.N; i++ {
		o.(*offsetOp).seen = 0
		o.Next()
	}
}
