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

func TestLimit(t *testing.T) {
	tcs := []struct {
		limit    uint64
		tuples   []tuple
		expected []tuple
	}{
		{
			limit:    2,
			tuples:   tuples{{1}},
			expected: tuples{{1}},
		},
		{
			limit:    1,
			tuples:   tuples{{1}},
			expected: tuples{{1}},
		},
		{
			limit:    0,
			tuples:   tuples{{1}},
			expected: tuples{},
		},
		{
			limit:    100000,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{{1}, {2}, {3}, {4}},
		},
		{
			limit:    2,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{{1}, {2}},
		},
		{
			limit:    1,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{{1}},
		},
		{
			limit:    0,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{},
		},
	}

	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, []types.T{}, func(t *testing.T, input []Operator) {
			limit := NewLimitOp(input[0], tc.limit)
			out := newOpTestOutput(limit, []int{0}, tc.expected)

			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
