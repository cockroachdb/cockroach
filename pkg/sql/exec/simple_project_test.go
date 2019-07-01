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

func TestSimpleProjectOp(t *testing.T) {
	tcs := []struct {
		tuples     tuples
		expected   tuples
		colsToKeep []uint32
	}{
		{
			colsToKeep: []uint32{0, 2},
			tuples: tuples{
				{1, 2, 3},
				{1, 2, 3},
			},
			expected: tuples{
				{1, 3},
				{1, 3},
			},
		},
		{
			colsToKeep: []uint32{0, 1},
			tuples: tuples{
				{1, 2, 3},
				{1, 2, 3},
			},
			expected: tuples{
				{1, 2},
				{1, 2},
			},
		},
		{
			colsToKeep: []uint32{2, 1},
			tuples: tuples{
				{1, 2, 3},
				{1, 2, 3},
			},
			expected: tuples{
				{3, 2},
				{3, 2},
			},
		},
	}
	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier, []int{0, 1}, func(input []Operator) (Operator, error) {
			return NewSimpleProjectOp(input[0], tc.colsToKeep), nil
		})
	}

	// Empty projection.
	runTests(t, []tuples{{{1, 2, 3}, {1, 2, 3}}}, tuples{{}, {}}, orderedVerifier, []int{},
		func(input []Operator) (Operator, error) {
			return NewSimpleProjectOp(input[0], nil), nil
		})
}
