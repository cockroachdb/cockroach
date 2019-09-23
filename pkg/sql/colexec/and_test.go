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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestAndOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		tuples                []tuple
		expected              []tuple
		skipAllNullsInjection bool
	}{
		// All variations of pairs separately first.
		{
			tuples:   tuples{{false, true}},
			expected: tuples{{false, true, false}},
		},
		{
			tuples:   tuples{{false, nil}},
			expected: tuples{{false, nil, false}},
		},
		{
			tuples:   tuples{{false, false}},
			expected: tuples{{false, false, false}},
		},
		{
			tuples:   tuples{{true, true}},
			expected: tuples{{true, true, true}},
		},
		{
			tuples:   tuples{{true, false}},
			expected: tuples{{true, false, false}},
		},
		{
			tuples:   tuples{{true, nil}},
			expected: tuples{{true, nil, nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   tuples{{nil, true}},
			expected: tuples{{nil, true, nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   tuples{{nil, false}},
			expected: tuples{{nil, false, false}},
		},
		{
			tuples:   tuples{{nil, nil}},
			expected: tuples{{nil, nil, nil}},
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
				{false, true, false}, {false, nil, false}, {false, false, false},
				{true, true, true}, {true, false, false}, {true, nil, nil},
				{nil, true, nil}, {nil, false, false}, {nil, nil, nil},
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
