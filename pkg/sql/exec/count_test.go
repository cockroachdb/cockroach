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

func TestCount(t *testing.T) {
	tcs := []struct {
		tuples   tuples
		expected tuples
	}{
		{
			tuples:   tuples{{1}, {1}},
			expected: tuples{{2}},
		},
		{
			tuples:   tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			expected: tuples{{10}},
		},
	}
	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, func(t *testing.T, input []Operator) {
			count := NewCountOp(input[0])
			out := newOpTestOutput(count, []int{0}, tc.expected)

			if err := out.VerifyAnyOrder(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
