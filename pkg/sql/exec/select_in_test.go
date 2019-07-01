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
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

func TestSelectInInt64(t *testing.T) {
	testCases := []struct {
		desc         string
		inputTuples  tuples
		outputTuples tuples
		filterRow    []int64
		hasNulls     bool
		negate       bool
	}{
		{
			desc:         "Simple in test",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{{0}, {1}},
			filterRow:    []int64{0, 1},
			hasNulls:     false,
			negate:       false,
		},
		{
			desc:         "Simple not in test",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{{2}},
			filterRow:    []int64{0, 1},
			hasNulls:     false,
			negate:       true,
		},
		{
			desc:         "In test with NULLs",
			inputTuples:  tuples{{nil}, {1}, {2}},
			outputTuples: tuples{{1}},
			filterRow:    []int64{1},
			hasNulls:     true,
			negate:       false,
		},
		{
			desc:         "Not in test with NULLs",
			inputTuples:  tuples{{nil}, {1}, {2}},
			outputTuples: tuples{},
			filterRow:    []int64{1},
			hasNulls:     true,
			negate:       true,
		},
	}

	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			runTests(t, []tuples{c.inputTuples}, func(t *testing.T, input []Operator) {
				op := selectInOpInt64{
					input:     input[0],
					colIdx:    0,
					filterRow: c.filterRow,
					negate:    c.negate,
					hasNulls:  c.hasNulls,
				}
				op.Init()
				out := newOpTestOutput(&op, []int{0}, c.outputTuples)
				if err := out.Verify(); err != nil {
					t.Error(err)
				}
			})
		})
	}
}

func benchmarkSelectInInt64(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()
	batch := coldata.NewMemBatch([]types.T{types.Int64})
	col1 := batch.ColVec(0).Int64()

	for i := int64(0); i < coldata.BatchSize; i++ {
		if float64(i) < coldata.BatchSize*selectivity {
			col1[i] = -1
		} else {
			col1[i] = 1
		}
	}

	if hasNulls {
		for i := 0; i < coldata.BatchSize; i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(uint16(i))
			}
		}
	}

	batch.SetLength(coldata.BatchSize)

	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := int64(0); i < coldata.BatchSize; i++ {
			sel[i] = uint16(i)
		}
	}

	source := NewRepeatableBatchSource(batch)
	source.Init()
	inOp := &selectInOpInt64{
		input:     source,
		colIdx:    0,
		filterRow: []int64{1},
	}
	inOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inOp.Next(ctx)
	}
}

func BenchmarkSelectInInt64(b *testing.B) {
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				benchmarkSelectInInt64(b, useSel, hasNulls)
			})
		}
	}
}
