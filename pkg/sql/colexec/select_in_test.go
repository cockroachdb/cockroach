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
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestSelectInInt64(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
			opConstructor := func(input []Operator) (Operator, error) {
				op := selectInOpInt64{
					OneInputNode: NewOneInputNode(input[0]),
					colIdx:       0,
					filterRow:    c.filterRow,
					negate:       c.negate,
					hasNulls:     c.hasNulls,
				}
				return &op, nil
			}
			if !c.hasNulls || !c.negate {
				runTests(t, []tuples{c.inputTuples}, c.outputTuples, orderedVerifier, opConstructor)
			} else {
				// When the input tuples already have nulls and we have NOT IN
				// operator, then the nulls injection might not change the output. For
				// example, we have this test case "1 NOT IN (NULL, 1, 2)" with the
				// output of length 0; similarly, we will get the same zero-length
				// output for the corresponding nulls injection test case
				// "1 NOT IN (NULL, NULL, NULL)".
				runTestsWithoutAllNullsInjection(t, []tuples{c.inputTuples}, nil /* typs */, c.outputTuples, orderedVerifier, opConstructor)
			}
		})
	}
}

func benchmarkSelectInInt64(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()
	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64})
	col1 := batch.ColVec(0).Int64()

	for i := 0; i < int(coldata.BatchSize()); i++ {
		if float64(i) < float64(coldata.BatchSize())*selectivity {
			col1[i] = -1
		} else {
			col1[i] = 1
		}
	}

	if hasNulls {
		for i := 0; i < int(coldata.BatchSize()); i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(uint16(i))
			}
		}
	}

	batch.SetLength(coldata.BatchSize())

	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := uint16(0); i < coldata.BatchSize(); i++ {
			sel[i] = i
		}
	}

	source := NewRepeatableBatchSource(batch)
	source.Init()
	inOp := &selectInOpInt64{
		OneInputNode: NewOneInputNode(source),
		colIdx:       0,
		filterRow:    []int64{1, 2, 3},
	}
	inOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize()))
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

func TestProjectInInt64(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
			inputTuples:  tuples{{0}, {1}},
			outputTuples: tuples{{0, true}, {1, true}},
			filterRow:    []int64{0, 1},
			hasNulls:     false,
			negate:       false,
		},
		{
			desc:         "Simple not in test",
			inputTuples:  tuples{{2}},
			outputTuples: tuples{{2, true}},
			filterRow:    []int64{0, 1},
			hasNulls:     false,
			negate:       true,
		},
		{
			desc:         "In test with NULLs",
			inputTuples:  tuples{{1}, {2}, {nil}},
			outputTuples: tuples{{1, true}, {2, nil}, {nil, nil}},
			filterRow:    []int64{1},
			hasNulls:     true,
			negate:       false,
		},
		{
			desc:         "Not in test with NULLs",
			inputTuples:  tuples{{1}, {2}, {nil}},
			outputTuples: tuples{{1, false}, {2, nil}, {nil, nil}},
			filterRow:    []int64{1},
			hasNulls:     true,
			negate:       true,
		},
		{
			desc:         "Not in test with NULLs and no nulls in filter",
			inputTuples:  tuples{{1}, {2}, {nil}},
			outputTuples: tuples{{1, false}, {2, true}, {nil, nil}},
			filterRow:    []int64{1},
			hasNulls:     false,
			negate:       true,
		},
		{
			desc:         "Test with false values",
			inputTuples:  tuples{{1}, {2}},
			outputTuples: tuples{{1, false}, {2, false}},
			filterRow:    []int64{3},
			hasNulls:     false,
			negate:       false,
		},
	}

	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			runTests(t, []tuples{c.inputTuples}, c.outputTuples, orderedVerifier,
				func(input []Operator) (Operator, error) {
					op := projectInOpInt64{
						OneInputNode: NewOneInputNode(input[0]),
						colIdx:       0,
						outputIdx:    1,
						filterRow:    c.filterRow,
						negate:       c.negate,
						hasNulls:     c.hasNulls,
					}
					return &op, nil
				})
		})
	}
}
