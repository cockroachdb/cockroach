// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecbase_test

import (
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSimpleProjectOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tcs := []struct {
		tuples     colexectestutils.Tuples
		expected   colexectestutils.Tuples
		colsToKeep []uint32
	}{
		{
			colsToKeep: []uint32{0, 2},
			tuples: colexectestutils.Tuples{
				{1, 2, 3},
				{1, 2, 3},
			},
			expected: colexectestutils.Tuples{
				{1, 3},
				{1, 3},
			},
		},
		{
			colsToKeep: []uint32{0, 1},
			tuples: colexectestutils.Tuples{
				{1, 2, 3},
				{1, 2, 3},
			},
			expected: colexectestutils.Tuples{
				{1, 2},
				{1, 2},
			},
		},
		{
			colsToKeep: []uint32{2, 1},
			tuples: colexectestutils.Tuples{
				{1, 2, 3},
				{1, 2, 3},
			},
			expected: colexectestutils.Tuples{
				{3, 2},
				{3, 2},
			},
		},
	}
	for _, tc := range tcs {
		colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, tc.expected, colexectestutils.OrderedVerifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
			return colexecbase.NewSimpleProjectOp(input[0], len(tc.tuples[0]), tc.colsToKeep), nil
		})
	}

	// Empty projection. The all nulls injection test case will also return
	// nothing.
	colexectestutils.RunTestsWithoutAllNullsInjection(t, testAllocator, []colexectestutils.Tuples{{{1, 2, 3}, {1, 2, 3}}}, nil, colexectestutils.Tuples{{}, {}}, colexectestutils.OrderedVerifier,
		func(input []colexecop.Operator) (colexecop.Operator, error) {
			return colexecbase.NewSimpleProjectOp(input[0], 3 /* numInputCols */, nil), nil
		})

	t.Run("RedundantProjectionIsNotPlanned", func(t *testing.T) {
		typs := []*types.T{types.Int, types.Int}
		input := colexectestutils.NewFiniteBatchSource(testAllocator, testAllocator.NewMemBatchWithMaxCapacity(typs), typs, 1)
		projectOp := colexecbase.NewSimpleProjectOp(input, len(typs), []uint32{0, 1})
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
	defer log.Scope(t).Close(t)
	inputTypes := []*types.T{types.Bytes, types.Float}
	constVal := int64(42)
	var wg sync.WaitGroup
	inputTuples := []colexectestutils.Tuples{
		{{"a", 1.0}, {"aa", 10.0}},
		{{"b", 2.0}, {"bb", 20.0}},
	}
	expected := colexectestutils.Tuples{
		{"a", constVal},
		{"aa", constVal},
		{"b", constVal},
		{"bb", constVal},
	}
	colexectestutils.RunTestsWithoutAllNullsInjection(t, testAllocator, inputTuples, [][]*types.T{inputTypes, inputTypes}, expected, colexectestutils.UnorderedVerifier,
		func(inputs []colexecop.Operator) (colexecop.Operator, error) {
			var input colexecop.Operator
			parallelUnorderedSynchronizerInputs := make([]colexecargs.OpWithMetaInfo, len(inputs))
			for i := range parallelUnorderedSynchronizerInputs {
				parallelUnorderedSynchronizerInputs[i].Root = inputs[i]
			}
			input = colexec.NewParallelUnorderedSynchronizer(parallelUnorderedSynchronizerInputs, &wg)
			input = colexecbase.NewSimpleProjectOp(input, len(inputTypes), []uint32{0})
			return colexecbase.NewConstOp(testAllocator, input, types.Int, constVal, 1)
		})
	wg.Wait()
}
