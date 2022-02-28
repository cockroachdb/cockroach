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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestSelectInInt64(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		desc         string
		inputTuples  colexectestutils.Tuples
		outputTuples colexectestutils.Tuples
		filterRow    []int64
		hasNulls     bool
		negate       bool
	}{
		{
			desc:         "Simple in test",
			inputTuples:  colexectestutils.Tuples{{0}, {1}, {2}},
			outputTuples: colexectestutils.Tuples{{0}, {1}},
			filterRow:    []int64{0, 1},
			hasNulls:     false,
			negate:       false,
		},
		{
			desc:         "Simple not in test",
			inputTuples:  colexectestutils.Tuples{{0}, {1}, {2}},
			outputTuples: colexectestutils.Tuples{{2}},
			filterRow:    []int64{0, 1},
			hasNulls:     false,
			negate:       true,
		},
		{
			desc:         "In test with NULLs",
			inputTuples:  colexectestutils.Tuples{{nil}, {1}, {2}},
			outputTuples: colexectestutils.Tuples{{1}},
			filterRow:    []int64{1},
			hasNulls:     true,
			negate:       false,
		},
		{
			desc:         "Not in test with NULLs",
			inputTuples:  colexectestutils.Tuples{{nil}, {1}, {2}},
			outputTuples: colexectestutils.Tuples{},
			filterRow:    []int64{1},
			hasNulls:     true,
			negate:       true,
		},
	}

	for _, c := range testCases {
		log.Infof(context.Background(), "%s", c.desc)
		opConstructor := func(input []colexecop.Operator) (colexecop.Operator, error) {
			op := selectInOpInt64{
				OneInputHelper: colexecop.MakeOneInputHelper(input[0]),
				colIdx:         0,
				filterRow:      c.filterRow,
				negate:         c.negate,
				hasNulls:       c.hasNulls,
			}
			return &op, nil
		}
		if !c.hasNulls || !c.negate {
			colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{c.inputTuples}, c.outputTuples, colexectestutils.OrderedVerifier, opConstructor)
		} else {
			// When the input tuples already have nulls and we have NOT IN
			// operator, then the nulls injection might not change the output. For
			// example, we have this test case "1 NOT IN (NULL, 1, 2)" with the
			// output of length 0; similarly, we will get the same zero-length
			// output for the corresponding nulls injection test case
			// "1 NOT IN (NULL, NULL, NULL)".
			colexectestutils.RunTestsWithoutAllNullsInjection(t, testAllocator, []colexectestutils.Tuples{c.inputTuples}, nil, c.outputTuples, colexectestutils.OrderedVerifier, opConstructor)
		}
	}
}

func benchmarkSelectInInt64(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()
	typs := []*types.T{types.Int}
	batch := testAllocator.NewMemBatchWithMaxCapacity(typs)
	col1 := batch.ColVec(0).Int64()

	for i := 0; i < coldata.BatchSize(); i++ {
		if float64(i) < float64(coldata.BatchSize())*selectivity {
			col1[i] = int64(i % 10)
		} else {
			col1[i] = -1
		}
	}

	if hasNulls {
		for i := 0; i < coldata.BatchSize(); i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(i)
			}
		}
	}

	batch.SetLength(coldata.BatchSize())

	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := 0; i < coldata.BatchSize(); i++ {
			sel[i] = i
		}
	}

	source := colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)
	inOp := &selectInOpInt64{
		OneInputHelper: colexecop.MakeOneInputHelper(source),
		colIdx:         0,
		filterRow:      []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	inOp.Init(ctx)

	b.SetBytes(int64(8 * coldata.BatchSize()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inOp.Next()
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	testCases := []struct {
		desc         string
		inputTuples  colexectestutils.Tuples
		outputTuples colexectestutils.Tuples
		inClause     string
	}{
		{
			desc:         "Simple in test",
			inputTuples:  colexectestutils.Tuples{{0}, {1}},
			outputTuples: colexectestutils.Tuples{{0, true}, {1, true}},
			inClause:     "IN (0, 1)",
		},
		{
			desc:         "Simple not in test",
			inputTuples:  colexectestutils.Tuples{{2}},
			outputTuples: colexectestutils.Tuples{{2, true}},
			inClause:     "NOT IN (0, 1)",
		},
		{
			desc:         "In test with NULLs",
			inputTuples:  colexectestutils.Tuples{{1}, {2}, {nil}},
			outputTuples: colexectestutils.Tuples{{1, true}, {2, nil}, {nil, nil}},
			inClause:     "IN (1, NULL)",
		},
		{
			desc:         "Not in test with NULLs",
			inputTuples:  colexectestutils.Tuples{{1}, {2}, {nil}},
			outputTuples: colexectestutils.Tuples{{1, false}, {2, nil}, {nil, nil}},
			inClause:     "NOT IN (1, NULL)",
		},
		{
			desc:         "Not in test with NULLs and no nulls in filter",
			inputTuples:  colexectestutils.Tuples{{1}, {2}, {nil}},
			outputTuples: colexectestutils.Tuples{{1, false}, {2, true}, {nil, nil}},
			inClause:     "NOT IN (1)",
		},
		{
			desc:         "Test with false values",
			inputTuples:  colexectestutils.Tuples{{1}, {2}},
			outputTuples: colexectestutils.Tuples{{1, false}, {2, false}},
			inClause:     "IN (3)",
		},
	}

	for _, c := range testCases {
		log.Infof(ctx, "%s", c.desc)
		colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{c.inputTuples}, c.outputTuples, colexectestutils.OrderedVerifier,
			func(input []colexecop.Operator) (colexecop.Operator, error) {
				return colexectestutils.CreateTestProjectingOperator(
					ctx, flowCtx, input[0], []*types.T{types.Int},
					fmt.Sprintf("@1 %s", c.inClause), false /* canFallbackToRowexec */, testMemAcc,
				)
			})
	}
}
