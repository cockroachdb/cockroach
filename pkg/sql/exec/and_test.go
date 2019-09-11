// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

func TestAndOp(t *testing.T) {
	tcs := []struct {
		tuples                []tuple
		expected              []tuple
		skipAllNullsInjection bool
	}{
		// All variations of pairs separately first.
		{
			tuples:   tuples{{false, true}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{false, nil}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{false, false}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{true, true}},
			expected: tuples{{true}},
		},
		{
			tuples:   tuples{{true, false}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{true, nil}},
			expected: tuples{{nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   tuples{{nil, true}},
			expected: tuples{{nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   tuples{{nil, false}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{nil, nil}},
			expected: tuples{{nil}},
		},
		// Now all variations of pairs combined together to make sure that nothing
		// funky going on with multiple tuples.
		{
			tuples: tuples{
				{false, true}, {false, nil}, {false, false},
				{true, true}, {true, false}, {true, nil},
				{nil, true}, {nil, false}, {nil, nil},
			},
			expected: tuples{
				{false}, {false}, {false},
				{true}, {false}, {nil},
				{nil}, {false}, {nil},
			},
		},
	}

	for _, tc := range tcs {
		var runner testRunner
		if tc.skipAllNullsInjection {
			// We're omitting all nulls injection test. See comments for each such
			// test case.
			runner = runTestsWithoutAllNullsInjection
		} else {
			runner = runTestsWithTyps
		}
		runner(
			t,
			[]tuples{tc.tuples},
			[]coltypes.T{coltypes.Bool, coltypes.Bool},
			tc.expected,
			orderedVerifier,
			[]int{2},
			func(input []Operator) (Operator, error) {
				return NewAndOp(
					input[0],
					0, /* leftIdx */
					1, /* rightIdx */
					2, /* outputIdx */
				), nil
			})
	}
}
