// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// analyzeExpr performs semantic analysis of an expression, including:
// - type checking (with optional type enforcement);
// - normalization.
// The parameters sources and IndexedVars, if both are non-nil, indicate
// name resolution should be performed. The IndexedVars map will be filled
// as a result.
func (p *planner) analyzeExpr(
	ctx context.Context,
	expr tree.Expr,
	iVarHelper tree.IndexedVarHelper,
	expectedType *types.T,
	requireType bool,
	typingContext string,
) (tree.TypedExpr, error) {
	// Type check.
	var typedExpr tree.TypedExpr
	var err error
	p.semaCtx.IVarContainer = iVarHelper.Container()
	if requireType {
		typedExpr, err = tree.TypeCheckAndRequire(ctx, expr, &p.semaCtx, expectedType, typingContext)
	} else {
		typedExpr, err = tree.TypeCheck(ctx, expr, &p.semaCtx, expectedType)
	}
	p.semaCtx.IVarContainer = nil
	if err != nil {
		return nil, err
	}

	// Normalize.
	return p.txCtx.NormalizeExpr(ctx, p.EvalContext(), typedExpr)
}
