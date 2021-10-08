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

	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tcs := []struct {
		limit    uint64
		tuples   []colexectestutils.Tuple
		expected []colexectestutils.Tuple
	}{
		{
			limit:    2,
			tuples:   colexectestutils.Tuples{{1}},
			expected: colexectestutils.Tuples{{1}},
		},
		{
			limit:    1,
			tuples:   colexectestutils.Tuples{{1}},
			expected: colexectestutils.Tuples{{1}},
		},
		{
			limit:    0,
			tuples:   colexectestutils.Tuples{{1}},
			expected: colexectestutils.Tuples{},
		},
		{
			limit:    100000,
			tuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			expected: colexectestutils.Tuples{{1}, {2}, {3}, {4}},
		},
		{
			limit:    2,
			tuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			expected: colexectestutils.Tuples{{1}, {2}},
		},
		{
			limit:    1,
			tuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			expected: colexectestutils.Tuples{{1}},
		},
		{
			limit:    0,
			tuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			expected: colexectestutils.Tuples{},
		},
	}

	for _, tc := range tcs {
		// The tuples consisting of all nulls still count as separate rows, so if
		// we replace all values with nulls, we should get the same output.
		colexectestutils.RunTestsWithoutAllNullsInjection(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, nil, tc.expected, colexectestutils.OrderedVerifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
			return NewLimitOp(input[0], tc.limit), nil
		})
	}
}
