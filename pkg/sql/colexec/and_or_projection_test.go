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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type andOrTestCase struct {
	tuples                []tuple
	expected              []tuple
	skipAllNullsInjection bool
}

var (
	andTestCases []andOrTestCase
	orTestCases  []andOrTestCase
)

func init() {
	andTestCases = []andOrTestCase{
		// All variations of pairs separately first.
		{
			tuples:   tuples{{false, true}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{false, nil}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{false, false}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{true, true}},
			expected: tuples{{true}},
		},
		{
			tuples:   tuples{{true, false}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{true, nil}},
			expected: tuples{{nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   tuples{{nil, true}},
			expected: tuples{{nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   tuples{{nil, false}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{nil, nil}},
			expected: tuples{{nil}},
		},
		// Now all variations of pairs combined together to make sure that nothing
		// funky going on with multiple tuples.
		{
			tuples: tuples{
				{false, true}, {false, nil}, {false, false},
				{true, true}, {true, false}, {true, nil},
				{nil, true}, {nil, false}, {nil, nil},
			},
			expected: tuples{
				{false}, {false}, {false},
				{true}, {false}, {nil},
				{nil}, {false}, {nil},
			},
		},
	}

	orTestCases = []andOrTestCase{
		// All variations of pairs separately first.
		{
			tuples:   tuples{{false, true}},
			expected: tuples{{true}},
		},
		{
			tuples:   tuples{{false, nil}},
			expected: tuples{{nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   tuples{{false, false}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{true, true}},
			expected: tuples{{true}},
		},
		{
			tuples:   tuples{{true, false}},
			expected: tuples{{true}},
		},
		{
			tuples:   tuples{{true, nil}},
			expected: tuples{{true}},
		},
		{
			tuples:   tuples{{nil, true}},
			expected: tuples{{true}},
		},
		{
			tuples:   tuples{{nil, false}},
			expected: tuples{{nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   tuples{{nil, nil}},
			expected: tuples{{nil}},
		},
		// Now all variations of pairs combined together to make sure that nothing
		// funky going on with multiple tuples.
		{
			tuples: tuples{
				{false, true}, {false, nil}, {false, false},
				{true, true}, {true, false}, {true, nil},
				{nil, true}, {nil, false}, {nil, nil},
			},
			expected: tuples{
				{true}, {nil}, {false},
				{true}, {true}, {true},
				{true}, {nil}, {nil},
			},
		},
	}
}

func TestAndOrOps(t *testing.T) {
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

	for _, test := range []struct {
		operation string
		cases     []andOrTestCase
	}{
		{
			operation: "AND",
			cases:     andTestCases,
		},
		{
			operation: "OR",
			cases:     orTestCases,
		},
	} {
		t.Run(test.operation, func(t *testing.T) {
			for _, tc := range test.cases {
				var runner testRunner
				if tc.skipAllNullsInjection {
					// We're omitting all nulls injection test. See comments for each such
					// test case.
					runner = runTestsWithoutAllNullsInjection
				} else {
					runner = runTestsWithTyps
				}
				runner(
					t,
					[]tuples{tc.tuples},
					[][]coltypes.T{{coltypes.Bool, coltypes.Bool}},
					tc.expected,
					orderedVerifier,
					func(input []Operator) (Operator, error) {
						spec := &execinfrapb.ProcessorSpec{
							Input: []execinfrapb.InputSyncSpec{{ColumnTypes: []types.T{*types.Bool, *types.Bool}}},
							Core: execinfrapb.ProcessorCoreUnion{
								Noop: &execinfrapb.NoopCoreSpec{},
							},
							Post: execinfrapb.PostProcessSpec{
								RenderExprs: []execinfrapb.Expression{{Expr: fmt.Sprintf("@1 %s @2", test.operation)}},
							},
						}
						args := NewColOperatorArgs{
							Spec:                spec,
							Inputs:              input,
							StreamingMemAccount: testMemAcc,
						}
						args.TestingKnobs.UseStreamingMemAccountForBuffering = true
						result, err := NewColOperator(ctx, flowCtx, args)
						if err != nil {
							return nil, err
						}
						return result.Op, nil
					})
			}
		})
	}
}

func benchmarkLogicalProjOp(
	b *testing.B, operation string, useSelectionVector bool, hasNulls bool,
) {
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
	rng, _ := randutil.NewPseudoRand()

	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Bool, coltypes.Bool})
	col1 := batch.ColVec(0).Bool()
	col2 := batch.ColVec(0).Bool()
	for i := 0; i < coldata.BatchSize(); i++ {
		col1[i] = rng.Float64() < 0.5
		col2[i] = rng.Float64() < 0.5
	}
	if hasNulls {
		nulls1 := batch.ColVec(0).Nulls()
		nulls2 := batch.ColVec(0).Nulls()
		for i := 0; i < coldata.BatchSize(); i++ {
			if rng.Float64() < nullProbability {
				nulls1.SetNull(i)
			}
			if rng.Float64() < nullProbability {
				nulls2.SetNull(i)
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
	input := NewRepeatableBatchSource(testAllocator, batch)

	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: []types.T{*types.Bool, *types.Bool}}},
		Core: execinfrapb.ProcessorCoreUnion{
			Noop: &execinfrapb.NoopCoreSpec{},
		},
		Post: execinfrapb.PostProcessSpec{
			RenderExprs: []execinfrapb.Expression{{Expr: fmt.Sprintf("@1 %s @2", operation)}},
		},
	}

	args := NewColOperatorArgs{
		Spec:                spec,
		Inputs:              []Operator{input},
		StreamingMemAccount: testMemAcc,
	}
	args.TestingKnobs.UseStreamingMemAccountForBuffering = true
	result, err := NewColOperator(ctx, flowCtx, args)
	if err != nil {
		b.Fatal(err)
	}
	logicalProjOp := result.Op
	logicalProjOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize()))
	for i := 0; i < b.N; i++ {
		logicalProjOp.Next(ctx)
	}
}

func BenchmarkLogicalProjOp(b *testing.B) {
	for _, operation := range []string{"AND", "OR"} {
		for _, useSel := range []bool{true, false} {
			for _, hasNulls := range []bool{true, false} {
				b.Run(fmt.Sprintf("%s,useSel=%t,hasNulls=%t", operation, useSel, hasNulls), func(b *testing.B) {
					benchmarkLogicalProjOp(b, operation, useSel, hasNulls)
				})
			}
		}
	}
}
