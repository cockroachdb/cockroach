// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
