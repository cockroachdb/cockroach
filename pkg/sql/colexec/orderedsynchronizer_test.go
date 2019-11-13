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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// Adapted from the same-named test in the rowflow package.
func TestOrderedSync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		sources  []tuples
		ordering sqlbase.ColumnOrdering
		expected tuples
	}{
		{
			sources: []tuples{
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
			ordering: sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
				{ColIdx: 1, Direction: encoding.Ascending},
			},
			expected: tuples{
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
			sources: []tuples{
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
			ordering: sqlbase.ColumnOrdering{
				{ColIdx: 1, Direction: encoding.Descending},
				{ColIdx: 0, Direction: encoding.Ascending},
				{ColIdx: 2, Direction: encoding.Ascending},
			},
			expected: tuples{
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
			sources: []tuples{
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
			ordering: sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			},
			expected: tuples{
				{nil},
				{-1},
				{1},
			},
		},
		{
			sources: []tuples{
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
			ordering: sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Descending},
			},
			expected: tuples{
				{1},
				{-1},
				{nil},
			},
		},
	}
	for _, tc := range testCases {
		numCols := len(tc.sources[0][0])
		columnTypes := make([]coltypes.T, numCols)
		for i := range columnTypes {
			columnTypes[i] = coltypes.Int64
		}
		runTests(t, tc.sources, tc.expected, orderedVerifier, func(inputs []Operator) (Operator, error) {
			return &OrderedSynchronizer{
				inputs:      inputs,
				ordering:    tc.ordering,
				columnTypes: columnTypes,
			}, nil
		})
	}
}

func TestOrderedSyncRandomInput(t *testing.T) {
	defer leaktest.AfterTest(t)()
	numInputs := 3
	inputLen := 1024
	batchSize := uint16(16)

	// Generate a random slice of sorted ints.
	randInts := make([]int, inputLen)
	for i := range randInts {
		randInts[i] = rand.Int()
	}
	sort.Ints(randInts)

	// Randomly distribute them among the inputs.
	expected := make(tuples, inputLen)
	sources := make([]tuples, numInputs)
	for i := range expected {
		t := tuple{randInts[i]}
		expected[i] = t
		sourceIdx := rand.Int() % 3
		if i < numInputs {
			// Make sure each input has at least one row.
			sourceIdx = i
		}
		sources[sourceIdx] = append(sources[sourceIdx], t)
	}
	inputs := make([]Operator, numInputs)
	for i := range inputs {
		inputs[i] = newOpTestInput(batchSize, sources[i], nil /* typs */)
	}

	op := OrderedSynchronizer{
		inputs: inputs,
		ordering: sqlbase.ColumnOrdering{
			{
				ColIdx:    0,
				Direction: encoding.Ascending,
			},
		},
		columnTypes: []coltypes.T{coltypes.Int64},
	}
	op.Init()
	out := newOpTestOutput(&op, expected)
	if err := out.Verify(); err != nil {
		t.Error(err)
	}
}

func BenchmarkOrderedSynchronizer(b *testing.B) {
	ctx := context.Background()

	numInputs := int64(3)
	batches := make([]coldata.Batch, numInputs)
	for i := range batches {
		batches[i] = testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64})
		batches[i].SetLength(coldata.BatchSize())
	}
	for i := int64(0); i < int64(coldata.BatchSize())*numInputs; i++ {
		batch := batches[i%numInputs]
		batch.ColVec(0).Int64()[i/numInputs] = i
	}

	inputs := make([]Operator, len(batches))
	for i := range batches {
		inputs[i] = NewRepeatableBatchSource(batches[i])
	}

	op := OrderedSynchronizer{
		inputs: inputs,
		ordering: sqlbase.ColumnOrdering{
			{ColIdx: 0, Direction: encoding.Ascending},
		},
		columnTypes: []coltypes.T{coltypes.Int64},
	}
	op.Init()

	b.SetBytes(8 * int64(coldata.BatchSize()) * numInputs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op.Next(ctx)
	}
}
