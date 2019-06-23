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

import "testing"

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
		runTests(t, []tuples{tc.tuples}, func(t *testing.T, input []Operator) {
			limit := NewLimitOp(input[0], tc.limit)
			out := newOpTestOutput(limit, []int{0}, tc.expected)

			if err := out.VerifyAnyOrder(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
