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
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

func TestSelectInInt64(t *testing.T) {
	tups := tuples{{0}, {1}, {2}}
	runTests(t, []tuples{tups}, func(t *testing.T, input []Operator) {
		op := selectInOpInt64{
			input:          input[0],
			colIdx:         0,
			filterRow:      []int64{0, 1},
			filterRowNulls: []bool{false, false},
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0}, tuples{{0}, {1}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

func TestSelectNotInt64(t *testing.T) {
	tups := tuples{{0}, {1}, {2}}
	runTests(t, []tuples{tups}, func(t *testing.T, input []Operator) {
		op := selectInOpInt64{
			input:          input[0],
			colIdx:         0,
			filterRow:      []int64{0, 1},
			filterRowNulls: []bool{false, false},
			negate:         true,
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0}, tuples{{2}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

func TestSelectInInt64Nulls(t *testing.T) {
	tups := tuples{{1, nil}, {2, 1}}
	runTests(t, []tuples{tups}, func(t *testing.T, input []Operator) {
		op := selectInOpInt64{
			input:          input[0],
			colIdx:         0,
			filterRow:      []int64{1, 0},
			filterRowNulls: []bool{false, true},
			hasNulls:       true,
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0}, tuples{{1}, {nil}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})

	runTests(t, []tuples{tups}, func(t *testing.T, input []Operator) {
		op := selectInOpInt64{
			input:          input[0],
			colIdx:         1,
			filterRow:      []int64{0, 2},
			filterRowNulls: []bool{false, false},
			hasNulls:       false,
		}
		op.Init()
		out := newOpTestOutput(&op, []int{1}, tuples{{nil}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

func TestSelectNotInInt64Nulls(t *testing.T) {
	tups := tuples{{1, nil}, {2, 1}}
	runTests(t, []tuples{tups}, func(t *testing.T, input []Operator) {
		op := selectInOpInt64{
			input:          input[0],
			colIdx:         0,
			filterRow:      []int64{1, 0},
			filterRowNulls: []bool{false, true},
			hasNulls:       true,
			negate:         true,
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0}, tuples{{nil}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})

	runTests(t, []tuples{tups}, func(t *testing.T, input []Operator) {
		op := selectInOpInt64{
			input:          input[0],
			colIdx:         1,
			filterRow:      []int64{0, 2},
			filterRowNulls: []bool{false, false},
			hasNulls:       false,
			negate:         true,
		}
		op.Init()
		out := newOpTestOutput(&op, []int{1}, tuples{{nil}, {1}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

// TODO: include sels and nulls
func benchmarkSelectInInt64(b *testing.B) {
	ctx := context.Background()
	batch := coldata.NewMemBatch([]types.T{types.Int64, types.Int64})
	col1 := batch.ColVec(0).Int64()
	col2 := batch.ColVec(1).Int64()

	for i := int64(0); i < coldata.BatchSize; i++ {
		if float64(i) < coldata.BatchSize*selectivity {
			col1[i], col2[i] = -1, 1
		} else {
			col1[i], col2[i] = 1, -1
		}
	}

	batch.SetLength(coldata.BatchSize)
	source := NewRepeatableBatchSource(batch)
	source.Init()
	inOp := &selectInOpInt64{
		input:          source,
		colIdx:         1,
		filterRow:      []int64{0, 1, 2, 3, 4},
		filterRowNulls: []bool{false, false, false, false, false},
	}
	inOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize * 2))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inOp.Next(ctx)
	}
}

func BenchmarkSelectInInt64(b *testing.B) {
	b.Run("selectInInt64", func(b *testing.B) {
		benchmarkSelectInInt64(b)
	})
}
