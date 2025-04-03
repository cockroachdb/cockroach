// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexectestutils

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// MockTypeContext is a mock typing context for the typechecker.
type MockTypeContext struct {
	Typs []*types.T
}

var _ eval.IndexedVarContainer = &MockTypeContext{}

// IndexedVarEval implements the eval.IndexedVarContainer interface.
func (p *MockTypeContext) IndexedVarEval(idx int) (tree.Datum, error) {
	return tree.DNull, nil
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (p *MockTypeContext) IndexedVarResolvedType(idx int) *types.T {
	return p.Typs[idx]
}

// CreateTestProjectingOperator creates a projecting operator that performs
// projectingExpr on input that has inputTypes as its output columns. It does
// so by making a noop processor core with post-processing step that passes
// through all input columns and renders an additional column using
// projectingExpr to create the render; then, the processor core is used to
// plan all necessary infrastructure using NewColOperator call.
//
// Note: colexecargs.TestNewColOperator must have been injected into the package
// in which the tests are running.
func CreateTestProjectingOperator(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input colexecop.Operator,
	inputTypes []*types.T,
	projectingExpr string,
	testMemAcc *mon.BoundAccount,
) (colexecop.Operator, error) {
	expr, err := parser.ParseExpr(projectingExpr)
	if err != nil {
		return nil, err
	}
	p := &MockTypeContext{Typs: inputTypes}
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	semaCtx.IVarContainer = p
	typedExpr, err := tree.TypeCheck(ctx, expr, &semaCtx, types.AnyElement)
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
	result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
	if err != nil {
		return nil, err
	}
	return result.Root, nil
}
