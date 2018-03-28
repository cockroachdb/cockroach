// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	expectedType types.T,
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
