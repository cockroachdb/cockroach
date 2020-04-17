// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		limit    int
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
		// The tuples consisting of all nulls still count as separate rows, so if
		// we replace all values with nulls, we should get the same output.
		runTestsWithoutAllNullsInjection(t, []tuples{tc.tuples}, nil /* typs */, tc.expected, orderedVerifier, func(input []colexecbase.Operator) (colexecbase.Operator, error) {
			return NewLimitOp(input[0], tc.limit), nil
		})
	}
}
