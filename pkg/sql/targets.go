// Copyright 2016 The Cockroach Authors.
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

const autoGenerateRenderOutputName = ""

// computeRender expands a target expression into a result column.
func (p *planner) computeRender(
	ctx context.Context,
	target tree.SelectExpr,
	desiredType *types.T,
	info sqlbase.MultiSourceInfo,
	ivarHelper tree.IndexedVarHelper,
	outputName string,
) (column sqlbase.ResultColumn, expr tree.TypedExpr, err error) {
	// When generating an output column name it should exactly match the original
	// expression, so if our caller has requested that we generate the output
	// column name, we determine the name before we perform any manipulations to
	// the expression.
	if outputName == autoGenerateRenderOutputName {
		if outputName, err = tree.GetRenderColName(p.SessionData().SearchPath, target); err != nil {
			return sqlbase.ResultColumn{}, nil, err
		}
	}

	normalized, err := p.analyzeExpr(ctx, target.Expr, info, ivarHelper, desiredType, false, "")
	if err != nil {
		return sqlbase.ResultColumn{}, nil, err
	}

	return sqlbase.ResultColumn{Name: outputName, Typ: normalized.ResolvedType()}, normalized, nil
}

// computeRenderAllowingStars expands a target expression into a result column (or more
// than one, in case there is a star). Whether star expansion occurred
// is indicated by the third return value.
func (p *planner) computeRenderAllowingStars(
	ctx context.Context,
	target tree.SelectExpr,
	desiredType *types.T,
	info sqlbase.MultiSourceInfo,
	ivarHelper tree.IndexedVarHelper,
	outputName string,
) (columns sqlbase.ResultColumns, exprs []tree.TypedExpr, hasStar bool, err error) {
	// Pre-normalize any VarName so the work is not done twice below.
	if err := target.NormalizeTopLevelVarName(); err != nil {
		return nil, nil, false, err
	}

	if hasStar, cols, typedExprs, err := sqlbase.CheckRenderStar(
		ctx, p.analyzeExpr, target, info, ivarHelper,
	); err != nil {
		return nil, nil, false, err
	} else if hasStar {
		return cols, typedExprs, hasStar, nil
	}

	col, expr, err := p.computeRender(ctx, target, desiredType, info, ivarHelper, outputName)
	if err != nil {
		return nil, nil, false, err
	}

	return sqlbase.ResultColumns{col}, []tree.TypedExpr{expr}, false, nil
}

// equivalentRenders returns true if and only if the two render expressions
// are equivalent.
func (s *renderNode) equivalentRenders(i, j int) bool {
	return s.isRenderEquivalent(symbolicExprStr(s.render[i]), j)
}

// isRenderEquivalent is a helper function for equivalentRenders() and
// addOrMergeRenders(). Do not use directly.
func (s *renderNode) isRenderEquivalent(exprStr string, j int) bool {
	otherExprStr := s.renderStrings[j]
	// otherExprStr may be the empty string if the render columns were reset.
	// In that case, just recompute on demand.
	if otherExprStr == "" {
		otherExprStr = symbolicExprStr(s.render[j])
		s.renderStrings[j] = otherExprStr
	}
	return otherExprStr == exprStr
}

// addOrReuseRender adds the given result column to the select render list and
// returns its column index. If the expression is already rendered and the
// reuse flag is true, no new render is added and the index of the existing
// column is returned instead.
func (s *renderNode) addOrReuseRender(
	col sqlbase.ResultColumn, expr tree.TypedExpr, reuseExistingRender bool,
) (colIdx int) {
	exprStr := symbolicExprStr(expr)
	if reuseExistingRender {
		for j := 0; j < len(s.render); j++ {
			// Now, try to find an equivalent render starting from idx. We use
			// the syntax representation as approximation of equivalence. At this point
			// the expressions must have undergone name resolution already so that
			// comparison occurs after replacing column names to IndexedVars.
			if s.isRenderEquivalent(exprStr, j) && s.render[j].ResolvedType().Equivalent(col.Typ) {
				return j
			}
		}
	}

	s.addRenderColumn(expr, exprStr, col)
	return len(s.render) - 1
}

// addOrReuseRenders adds the given result columns to the select render list
// and returns their column indices. If an expression is already rendered, and
// the reuse flag is true, no new render is added and the index of the existing
// column is returned instead.
func (s *renderNode) addOrReuseRenders(
	cols sqlbase.ResultColumns, exprs []tree.TypedExpr, reuseExistingRender bool,
) (colIdxs []int) {
	colIdxs = make([]int, len(cols))
	for i := range cols {
		colIdxs[i] = s.addOrReuseRender(cols[i], exprs[i], reuseExistingRender)
	}
	return colIdxs
}

// symbolicExprStr returns a string representation of the expression using
// symbolic notation. Because the symbolic notation disambiguate columns, this
// string can be used to determine if two expressions are equivalent.
func symbolicExprStr(expr tree.Expr) string {
	return tree.AsStringWithFlags(expr, tree.FmtCheckEquivalence)
}
