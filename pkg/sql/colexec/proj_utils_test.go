// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// Mock typing context for the typechecker.
type mockTypeContext struct {
	typs []*types.T
}

func (p *mockTypeContext) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return tree.DNull.Eval(ctx)
}

func (p *mockTypeContext) IndexedVarResolvedType(idx int) *types.T {
	return p.typs[idx]
}

func (p *mockTypeContext) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("$%d", idx))
	return &n
}

// createTestProjectingOperator creates a projecting operator that performs
// projectingExpr on input that has inputTypes as its output columns. It does
// so by making a noop processor core with post-processing step that passes
// through all input columns and renders an additional column using
// projectingExpr to create the render; then, the processor core is used to
// plan all necessary infrastructure using NewColOperator call.
// - canFallbackToRowexec determines whether NewColOperator will be able to use
// rowexec.NewProcessor to instantiate a wrapped rowexec processor. This should
// be false unless we expect that for some unit tests we will not be able to
// plan the "pure" vectorized operators.
func createTestProjectingOperator(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input colexecbase.Operator,
	inputTypes []*types.T,
	projectingExpr string,
	canFallbackToRowexec bool,
) (colexecbase.Operator, error) {
	expr, err := parser.ParseExpr(projectingExpr)
	if err != nil {
		return nil, err
	}
	p := &mockTypeContext{typs: inputTypes}
	semaCtx := tree.MakeSemaContext()
	semaCtx.IVarContainer = p
	typedExpr, err := tree.TypeCheck(ctx, expr, &semaCtx, types.Any)
	if err != nil {
		return nil, err
	}
	renderExprs := make([]execinfrapb.Expression, len(inputTypes)+1)
	for i := range inputTypes {
		renderExprs[i].Expr = fmt.Sprintf("@%d", i+1)
	}
	renderExprs[len(inputTypes)].LocalExpr = typedExpr
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: inputTypes}},
		Core: execinfrapb.ProcessorCoreUnion{
			Noop: &execinfrapb.NoopCoreSpec{},
		},
		Post: execinfrapb.PostProcessSpec{
			RenderExprs: renderExprs,
		},
		ResultTypes: append(inputTypes, typedExpr.ResolvedType()),
	}
	args := &NewColOperatorArgs{
		Spec:                spec,
		Inputs:              []colexecbase.Operator{input},
		StreamingMemAccount: testMemAcc,
	}
	if canFallbackToRowexec {
		args.ProcessorConstructor = rowexec.NewProcessor
	}
	result, err := TestNewColOperator(ctx, flowCtx, args)
	if err != nil {
		return nil, err
	}
	return result.Op, nil
}
