// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// analyzeExpr performs semantic analysis of an expression, including:
// - replacing sub-queries by a sql.subquery node;
// - resolving names (optional);
// - type checking (with optional type enforcement);
// - normalization.
// The parameters sources and IndexedVars, if both are non-nil, indicate
// name resolution should be performed. The IndexedVars map will be filled
// as a result.
func (p *planner) analyzeExpr(
	ctx context.Context,
	raw tree.Expr,
	sources sqlbase.MultiSourceInfo,
	iVarHelper tree.IndexedVarHelper,
	expectedType *types.T,
	requireType bool,
	typingContext string,
) (tree.TypedExpr, error) {
	// Replace the sub-queries.
	// In all contexts that analyze a single expression, a single value
	// is expected. Tell this to replaceSubqueries.  (See UPDATE for a
	// counter-example; cases where a subquery is an operand of a
	// comparison are handled specially in the subqueryVisitor already.)
	err := p.analyzeSubqueries(ctx, raw, 1 /* one value expected */)
	if err != nil {
		return nil, err
	}

	// Perform optional name resolution.
	resolved := raw
	if sources != nil {
		var hasStar bool
		resolved, _, hasStar, err = p.resolveNames(raw, sources, iVarHelper)
		if err != nil {
			return nil, err
		}
		p.curPlan.hasStar = p.curPlan.hasStar || hasStar
	}

	// Type check.
	var typedExpr tree.TypedExpr
	p.semaCtx.IVarContainer = iVarHelper.Container()
	if requireType {
		typedExpr, err = tree.TypeCheckAndRequire(resolved, &p.semaCtx,
			expectedType, typingContext)
	} else {
		typedExpr, err = tree.TypeCheck(resolved, &p.semaCtx, expectedType)
	}
	p.semaCtx.IVarContainer = nil
	if err != nil {
		return nil, err
	}

	// Normalize.
	return p.txCtx.NormalizeExpr(p.EvalContext(), typedExpr)
}
