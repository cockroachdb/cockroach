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
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier, func(input []colexecbase.Operator) (colexecbase.Operator, error) {
			return NewSimpleProjectOp(input[0], len(tc.tuples[0]), tc.colsToKeep), nil
		})
	}

	// Empty projection. The all nulls injection test case will also return
	// nothing.
	runTestsWithoutAllNullsInjection(t, []tuples{{{1, 2, 3}, {1, 2, 3}}}, nil /* typs */, tuples{{}, {}}, orderedVerifier,
		func(input []colexecbase.Operator) (colexecbase.Operator, error) {
			return NewSimpleProjectOp(input[0], 3 /* numInputCols */, nil), nil
		})

	t.Run("RedundantProjectionIsNotPlanned", func(t *testing.T) {
		typs := []*types.T{types.Int, types.Int}
		input := newFiniteBatchSource(testAllocator.NewMemBatch(typs), typs, 1 /* usableCount */)
		projectOp := NewSimpleProjectOp(input, len(typs), []uint32{0, 1})
		require.IsType(t, input, projectOp)
	})
}

// TestSimpleProjectOpWithUnorderedSynchronizer sets up the following
// structure:
//
//  input 1 --
//            | --> unordered synchronizer --> simpleProjectOp --> constInt64Op
//  input 2 --
//
// and makes sure that the output is as expected. The idea is to test
// simpleProjectOp in case when it receives multiple "different internally"
// batches. See #45686 for detailed discussion.
func TestSimpleProjectOpWithUnorderedSynchronizer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	inputTypes := []*types.T{types.Bytes, types.Float}
	constVal := int64(42)
	var wg sync.WaitGroup
	inputTuples := []tuples{
		{{"a", 1.0}, {"aa", 10.0}},
		{{"b", 2.0}, {"bb", 20.0}},
	}
	expected := tuples{
		{"a", constVal},
		{"aa", constVal},
		{"b", constVal},
		{"bb", constVal},
	}
	runTestsWithoutAllNullsInjection(t, inputTuples, [][]*types.T{inputTypes, inputTypes}, expected,
		unorderedVerifier, func(inputs []colexecbase.Operator) (colexecbase.Operator, error) {
			var input colexecbase.Operator
			parallelUnorderedSynchronizerInputs := make([]SynchronizerInput, len(inputs))
			for i := range parallelUnorderedSynchronizerInputs {
				parallelUnorderedSynchronizerInputs[i].Op = inputs[i]
			}
			input = NewParallelUnorderedSynchronizer(parallelUnorderedSynchronizerInputs, &wg)
			input = NewSimpleProjectOp(input, len(inputTypes), []uint32{0})
			return NewConstOp(testAllocator, input, types.Int, constVal, 1)
		})
	wg.Wait()
}
