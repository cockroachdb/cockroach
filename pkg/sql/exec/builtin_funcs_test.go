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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// Mock typing context for the typechecker.
type mockTypeContext struct {
	typs []types.T
}

func (p *mockTypeContext) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return tree.DNull.Eval(ctx)
}

func (p *mockTypeContext) IndexedVarResolvedType(idx int) *types.T {
	return &p.typs[idx]
}

func (p *mockTypeContext) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("$%d", idx))
	return &n
}

func TestBasicBuiltinFunctions(t *testing.T) {
	// Trick to get the init() for the builtins package to run.
	_ = builtins.AllBuiltinNames

	testCases := []struct {
		desc         string
		expr         string
		inputCols    []int
		inputTuples  tuples
		inputTypes   []types.T
		outputTypes  []types.T
		outputTuples tuples
	}{
		{
			desc:         "AbsVal",
			expr:         "abs(@1)",
			inputCols:    []int{0},
			inputTuples:  tuples{{1}, {-1}},
			inputTypes:   []types.T{*types.Int},
			outputTuples: tuples{{1}, {1}},
			outputTypes:  []types.T{*types.Int, *types.Int},
		},
		{
			desc:         "StringLen",
			expr:         "length(@1)",
			inputCols:    []int{0},
			inputTuples:  tuples{{"Hello"}, {"The"}},
			inputTypes:   []types.T{*types.String},
			outputTuples: tuples{{5}, {3}},
			outputTypes:  []types.T{*types.String, *types.Int},
		},
	}

	tctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			runTests(t, []tuples{tc.inputTuples}, tc.outputTuples, orderedVerifier, []int{1},
				func(input []Operator) (Operator, error) {
					expr, err := parser.ParseExpr(tc.expr)
					if err != nil {
						t.Fatal(err)
					}

					p := &mockTypeContext{typs: tc.inputTypes}
					typedExpr, err := tree.TypeCheck(expr, &tree.SemaContext{IVarContainer: p}, types.Any)
					if err != nil {
						t.Fatal(err)
					}

					return NewBuiltinFunctionOperator(tctx, typedExpr.(*tree.FuncExpr), tc.outputTypes, tc.inputCols, 1, input[0]), nil
				})
		})
	}
}

func benchmarkBuiltinFunctions(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()
	tctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	batch := coldata.NewMemBatch([]coltypes.T{coltypes.Int64})
	col := batch.ColVec(0).Int64()

	for i := int64(0); i < coldata.BatchSize; i++ {
		if float64(i) < coldata.BatchSize*selectivity {
			col[i] = -1
		} else {
			col[i] = 1
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

	expr, err := parser.ParseExpr("abs(@1)")
	if err != nil {
		b.Fatal(err)
	}
	p := &mockTypeContext{typs: []types.T{*types.Int}}
	typedExpr, err := tree.TypeCheck(expr, &tree.SemaContext{IVarContainer: p}, types.Any)
	if err != nil {
		b.Fatal(err)
	}
	op := NewBuiltinFunctionOperator(tctx, typedExpr.(*tree.FuncExpr), []types.T{*types.Int}, []int{0}, 1, source)

	b.SetBytes(int64(8 * coldata.BatchSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op.Next(ctx)
	}
}

func BenchmarkBuiltinFunctions(b *testing.B) {
	_ = builtins.AllBuiltinNames
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				benchmarkBuiltinFunctions(b, useSel, hasNulls)
			})
		}
	}
}
