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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// Typing context to for the typechecker.
type mockTypeContext struct {
	ty []types.T
}

func (p *mockTypeContext) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return tree.DNull.Eval(ctx)
}

func (p *mockTypeContext) IndexedVarResolvedType(idx int) *types.T {
	return &p.ty[idx]
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
		inputTuples  tuples
		inputTypes   []types.T
		outputTypes  []types.T
		outputTuples tuples
	}{
		{
			desc:         "Substring test",
			expr:         "substring(@1, 1, 2)",
			inputTuples:  tuples{{"Hello"}, {"There"}},
			inputTypes:   []types.T{*types.String},
			outputTuples: tuples{{"He"}, {"Th"}},
			outputTypes:  []types.T{*types.String, *types.String},
		},
		{
			desc:         "Absolute value test",
			expr:         "abs(@1)",
			inputTuples:  tuples{{1}, {-1}},
			inputTypes:   []types.T{*types.Int},
			outputTuples: tuples{{1}, {1}},
			outputTypes:  []types.T{*types.Int, *types.Int},
		},
		{
			desc:         "String length test",
			expr:         "length(@1)",
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

					p := &mockTypeContext{ty: tc.inputTypes}
					typedExpr, err := tree.TypeCheck(expr, &tree.SemaContext{IVarContainer: p}, types.Any)
					if err != nil {
						t.Fatal(err)
					}

					return NewBuiltinFunctionOperator(tctx, tc.outputTypes, input[0], typedExpr.(*tree.FuncExpr), 1)
				})
		})
	}
}
