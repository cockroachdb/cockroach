// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexectestutils

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// MockTypeContext is a mock typing context for the typechecker.
type MockTypeContext struct {
	Typs []*types.T
}

var _ tree.IndexedVarContainer = &MockTypeContext{}

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (p *MockTypeContext) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return tree.DNull.Eval(ctx)
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (p *MockTypeContext) IndexedVarResolvedType(idx int) *types.T {
	return p.Typs[idx]
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (p *MockTypeContext) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("$%d", idx))
	return &n
}

// CreateTestProjectingOperator creates a projecting operator that performs
// projectingExpr on input that has inputTypes as its output columns. It does
// so by making a noop processor core with post-processing step that passes
// through all input columns and renders an additional column using
// projectingExpr to create the render; then, the processor core is used to
// plan all necessary infrastructure using NewColOperator call.
// - canFallbackToRowexec determines whether NewColOperator will be able to use
// rowexec.NewProcessor to instantiate a wrapped rowexec processor. This should
// be false unless we expect that for some unit tests we will not be able to
// plan the "pure" vectorized operators.
//
// Note: colexecargs.TestNewColOperator must have been injected into the package
// in which the tests are running.
func CreateTestProjectingOperator(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input colexecop.Operator,
	inputTypes []*types.T,
	projectingExpr string,
	canFallbackToRowexec bool,
	testMemAcc *mon.BoundAccount,
) (colexecop.Operator, error) {
	expr, err := parser.ParseExpr(projectingExpr)
	if err != nil {
		return nil, err
	}
	p := &MockTypeContext{Typs: inputTypes}
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
	args := &colexecargs.NewColOperatorArgs{
		Spec:                spec,
		Inputs:              []colexecargs.OpWithMetaInfo{{Root: input}},
		StreamingMemAccount: testMemAcc,
	}
	if canFallbackToRowexec {
		args.ProcessorConstructor = rowexec.NewProcessor
	}
	result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
	if err != nil {
		return nil, err
	}
	return result.Root, nil
}
