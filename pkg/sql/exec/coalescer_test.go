// Copyright 2018 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

func TestCoalescer(t *testing.T) {
	// large tuple number for coalescing
	nRows := ColBatchSize*3 + 7
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
		runTests(t, []tuples{tc.tuples}, []types.T{}, func(t *testing.T, input []Operator) {
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
