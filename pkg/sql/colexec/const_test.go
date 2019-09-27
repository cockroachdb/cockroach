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

func TestConst(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		tuples   tuples
		expected tuples
	}{
		{
			tuples:   tuples{{1}, {1}},
			expected: tuples{{1, 9}, {1, 9}},
		},
		{
			tuples:   tuples{},
			expected: tuples{},
		},
	}
	for _, tc := range tcs {
		runTestsWithTyps(t, []tuples{tc.tuples}, [][]coltypes.T{{coltypes.Int64}}, tc.expected, orderedVerifier,
			func(input []Operator) (Operator, error) {
				return NewConstOp(input[0], coltypes.Int64, int64(9), 1)
			})
	}
}

func TestConstNull(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		tuples   tuples
		expected tuples
	}{
		{
			tuples:   tuples{{1}, {1}},
			expected: tuples{{1, nil}, {1, nil}},
		},
		{
			tuples:   tuples{},
			expected: tuples{},
		},
	}
	for _, tc := range tcs {
		runTestsWithTyps(t, []tuples{tc.tuples}, [][]coltypes.T{{coltypes.Int64}}, tc.expected, orderedVerifier,
			func(input []Operator) (Operator, error) {
				return NewConstNullOp(input[0], 1, coltypes.Int64), nil
			})
	}
}
