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
	"context"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Adapted from the same-named test in the rowflow package.
func TestOrderedSync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		sources  []colexectestutils.Tuples
		ordering colinfo.ColumnOrdering
		expected colexectestutils.Tuples
	}{
		{
			sources: []colexectestutils.Tuples{
				{
					{0, 1, 4},
					{0, 1, 2},
					{0, 2, 3},
					{1, 1, 3},
				},
				{
					{1, 0, 4},
				},
				{
					{0, 0, 0},
					{4, 4, 4},
				},
			},
			ordering: colinfo.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
				{ColIdx: 1, Direction: encoding.Ascending},
			},
			expected: colexectestutils.Tuples{
				{0, 0, 0},
				{0, 1, 4},
				{0, 1, 2},
				{0, 2, 3},
				{1, 0, 4},
				{1, 1, 3},
				{4, 4, 4},
			},
		},
		{
			sources: []colexectestutils.Tuples{
				{
					{1, 0, 4},
				},
				{
					{3, 4, 1},
					{4, 4, 4},
					{3, 2, 0},
				},
				{
					{4, 4, 5},
					{3, 3, 0},
					{0, 0, 0},
				},
			},
			ordering: colinfo.ColumnOrdering{
				{ColIdx: 1, Direction: encoding.Descending},
				{ColIdx: 0, Direction: encoding.Ascending},
				{ColIdx: 2, Direction: encoding.Ascending},
			},
			expected: colexectestutils.Tuples{
				{3, 4, 1},
				{4, 4, 4},
				{4, 4, 5},
				{3, 3, 0},
				{3, 2, 0},
				{0, 0, 0},
				{1, 0, 4},
			},
		},
		{
			sources: []colexectestutils.Tuples{
				{
					{-1},
				},
				{
					{1},
				},
				{
					{nil},
				},
			},
			ordering: colinfo.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			},
			expected: colexectestutils.Tuples{
				{nil},
				{-1},
				{1},
			},
		},
		{
			sources: []colexectestutils.Tuples{
				{
					{-1},
				},
				{
					{1},
				},
				{
					{nil},
				},
			},
			ordering: colinfo.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Descending},
			},
			expected: colexectestutils.Tuples{
				{1},
				{-1},
				{nil},
			},
		},
	}
	for _, tc := range testCases {
		numCols := len(tc.sources[0][0])
		typs := make([]*types.T, numCols)
		for i := range typs {
			typs[i] = types.Int
		}
		colexectestutils.RunTests(t, testAllocator, tc.sources, tc.expected, colexectestutils.OrderedVerifier, func(inputs []colexecop.Operator) (colexecop.Operator, error) {
			return NewOrderedSynchronizer(testAllocator, execinfra.DefaultMemoryLimit, colexectestutils.MakeInputs(inputs), typs, tc.ordering), nil
		})
	}
}

func TestOrderedSyncRandomInput(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	numInputs := 3
	inputLen := 1024
	batchSize := 16
	if batchSize > coldata.BatchSize() {
		batchSize = coldata.BatchSize()
	}
	typs := []*types.T{types.Int}

	// Generate a random slice of sorted ints.
	randInts := make([]int, inputLen)
	for i := range randInts {
		randInts[i] = rand.Int()
	}
	sort.Ints(randInts)

	// Randomly distribute them among the inputs.
	expected := make(colexectestutils.Tuples, inputLen)
	sources := make([]colexectestutils.Tuples, numInputs)
	for i := range expected {
		t := colexectestutils.Tuple{randInts[i]}
		expected[i] = t
		sourceIdx := rand.Int() % 3
		if i < numInputs {
			// Make sure each input has at least one row.
			sourceIdx = i
		}
		sources[sourceIdx] = append(sources[sourceIdx], t)
	}
	inputs := make([]colexecargs.OpWithMetaInfo, numInputs)
	for i := range inputs {
		inputs[i].Root = colexectestutils.NewOpTestInput(testAllocator, batchSize, sources[i], typs)
	}
	ordering := colinfo.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}
	op := NewOrderedSynchronizer(testAllocator, execinfra.DefaultMemoryLimit, inputs, typs, ordering)
	op.Init(context.Background())
	out := colexectestutils.NewOpTestOutput(op, expected)
	if err := out.Verify(); err != nil {
		t.Error(err)
	}
}

func BenchmarkOrderedSynchronizer(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	numInputs := int64(3)
	typs := []*types.T{types.Int}
	batches := make([]coldata.Batch, numInputs)
	for i := range batches {
		batches[i] = testAllocator.NewMemBatchWithMaxCapacity(typs)
		batches[i].SetLength(coldata.BatchSize())
	}
	for i := int64(0); i < int64(coldata.BatchSize())*numInputs; i++ {
		batch := batches[i%numInputs]
		batch.ColVec(0).Int64()[i/numInputs] = i
	}

	inputs := make([]colexecargs.OpWithMetaInfo, len(batches))
	for i := range batches {
		inputs[i].Root = colexecop.NewRepeatableBatchSource(testAllocator, batches[i], typs)
	}

	ordering := colinfo.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}
	op := NewOrderedSynchronizer(testAllocator, execinfra.DefaultMemoryLimit, inputs, typs, ordering)
	op.Init(ctx)

	b.SetBytes(8 * int64(coldata.BatchSize()) * numInputs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op.Next()
	}
}
