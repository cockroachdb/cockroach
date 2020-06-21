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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
			opConstructor := func(input []colexecbase.Operator) (colexecbase.Operator, error) {
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
	typs := []*types.T{types.Int}
	batch := testAllocator.NewMemBatch(typs)
	col1 := batch.ColVec(0).Int64()

	for i := 0; i < coldata.BatchSize(); i++ {
		if float64(i) < float64(coldata.BatchSize())*selectivity {
			col1[i] = -1
		} else {
			col1[i] = 1
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

	source := colexecbase.NewRepeatableBatchSource(testAllocator, batch, typs)
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
		inputTuples  tuples
		outputTuples tuples
		inClause     string
	}{
		{
			desc:         "Simple in test",
			inputTuples:  tuples{{0}, {1}},
			outputTuples: tuples{{0, true}, {1, true}},
			inClause:     "IN (0, 1)",
		},
		{
			desc:         "Simple not in test",
			inputTuples:  tuples{{2}},
			outputTuples: tuples{{2, true}},
			inClause:     "NOT IN (0, 1)",
		},
		{
			desc:         "In test with NULLs",
			inputTuples:  tuples{{1}, {2}, {nil}},
			outputTuples: tuples{{1, true}, {2, nil}, {nil, nil}},
			inClause:     "IN (1, NULL)",
		},
		{
			desc:         "Not in test with NULLs",
			inputTuples:  tuples{{1}, {2}, {nil}},
			outputTuples: tuples{{1, false}, {2, nil}, {nil, nil}},
			inClause:     "NOT IN (1, NULL)",
		},
		{
			desc:         "Not in test with NULLs and no nulls in filter",
			inputTuples:  tuples{{1}, {2}, {nil}},
			outputTuples: tuples{{1, false}, {2, true}, {nil, nil}},
			inClause:     "NOT IN (1)",
		},
		{
			desc:         "Test with false values",
			inputTuples:  tuples{{1}, {2}},
			outputTuples: tuples{{1, false}, {2, false}},
			inClause:     "IN (3)",
		},
	}

	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			runTests(t, []tuples{c.inputTuples}, c.outputTuples, orderedVerifier,
				func(input []colexecbase.Operator) (colexecbase.Operator, error) {
					expr, err := parser.ParseExpr(fmt.Sprintf("@1 %s", c.inClause))
					if err != nil {
						return nil, err
					}
					p := &mockTypeContext{typs: []*types.T{types.Int, types.MakeTuple([]*types.T{types.Int})}}
					semaCtx := tree.MakeSemaContext()
					semaCtx.IVarContainer = p
					typedExpr, err := tree.TypeCheck(ctx, expr, &semaCtx, types.Any)
					if err != nil {
						return nil, err
					}
					spec := &execinfrapb.ProcessorSpec{
						Input: []execinfrapb.InputSyncSpec{{ColumnTypes: []*types.T{types.Int}}},
						Core: execinfrapb.ProcessorCoreUnion{
							Noop: &execinfrapb.NoopCoreSpec{},
						},
						Post: execinfrapb.PostProcessSpec{
							RenderExprs: []execinfrapb.Expression{
								{Expr: "@1"},
								{LocalExpr: typedExpr},
							},
						},
					}
					args := NewColOperatorArgs{
						Spec:                spec,
						Inputs:              input,
						StreamingMemAccount: testMemAcc,
						// TODO(yuzefovich): figure out how to make the second
						// argument of IN comparison as DTuple not Tuple.
						// TODO(yuzefovich): reuse createTestProjectingOperator
						// once we don't need to provide the processor
						// constructor.
						ProcessorConstructor: rowexec.NewProcessor,
					}
					args.TestingKnobs.UseStreamingMemAccountForBuffering = true
					result, err := TestNewColOperator(ctx, flowCtx, args)
					if err != nil {
						return nil, err
					}
					return result.Op, nil
				})
		})
	}
}
