// Copyright 2018 The Cockroach Authors.
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

import "testing"

func TestBoolVecToSelOp(t *testing.T) {
	tcs := []struct {
		boolCol  uint32
		tuples   tuples
		expected tuples
	}{
		{
			boolCol:  0,
			tuples:   tuples{{true}, {false}, {true}},
			expected: tuples{{true}, {true}},
		},
	}
	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, func(t *testing.T, input []Operator) {
			op := NewBoolVecToSelOp(input[0], 0)
			out := newOpTestOutput(op, []int{0}, tc.expected)
			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
