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

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

func TestConst(t *testing.T) {
	tcs := []struct {
		tuples   tuples
		expected tuples
	}{
		{
			tuples:   tuples{{1}, {1}},
			expected: tuples{{1, 9}, {1, 9}},
		},
	}
	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier, []int{0, 1},
			func(input []Operator) (Operator, error) {
				return NewConstOp(input[0], types.Int64, int64(9), 1)
			})
	}
}

func TestConstNull(t *testing.T) {
	tcs := []struct {
		tuples   tuples
		expected tuples
	}{
		{
			tuples:   tuples{{1}, {1}},
			expected: tuples{{1, nil}, {1, nil}},
		},
	}
	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier, []int{0, 1},
			func(input []Operator) (Operator, error) {
				return NewConstNullOp(input[0], 1), nil
			})
	}
}
