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

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestSimpleProjectOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier, func(input []Operator) (Operator, error) {
			return NewSimpleProjectOp(input[0], len(tc.tuples[0]), tc.colsToKeep), nil
		})
	}

	// Empty projection. The all nulls injection test case will also return
	// nothing.
	runTestsWithoutAllNullsInjection(t, []tuples{{{1, 2, 3}, {1, 2, 3}}}, nil /* typs */, tuples{{}, {}}, orderedVerifier,
		func(input []Operator) (Operator, error) {
			return NewSimpleProjectOp(input[0], 3 /* numInputCols */, nil), nil
		})

	t.Run("RedundantProjectionIsNotPlanned", func(t *testing.T) {
		typs := []coltypes.T{coltypes.Int64, coltypes.Int64}
		input := newFiniteBatchSource(testAllocator.NewMemBatch(typs), 1 /* usableCount */)
		projectOp := NewSimpleProjectOp(input, len(typs), []uint32{0, 1})
		require.IsType(t, input, projectOp)
	})
}
