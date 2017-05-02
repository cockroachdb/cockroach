// Copyright 2016 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

const autoGenerateRenderOutputName = ""

// computeRender expands a target expression into a result column.
func (p *planner) computeRender(
	ctx context.Context,
	target parser.SelectExpr,
	desiredType parser.Type,
	info multiSourceInfo,
	ivarHelper parser.IndexedVarHelper,
	outputName string,
) (column sqlbase.ResultColumn, expr parser.TypedExpr, err error) {
	// When generating an output column name it should exactly match the original
	// expression, so if our caller has requested that we generate the output
	// column name, we determine the name before we perform any manipulations to
	// the expression.
	if outputName == autoGenerateRenderOutputName {
		if outputName, err = getRenderColName(p.session.SearchPath, target); err != nil {
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
	target parser.SelectExpr,
	desiredType parser.Type,
	info multiSourceInfo,
	ivarHelper parser.IndexedVarHelper,
	outputName string,
) (columns sqlbase.ResultColumns, exprs []parser.TypedExpr, hasStar bool, err error) {
	// Pre-normalize any VarName so the work is not done twice below.
	if err := target.NormalizeTopLevelVarName(); err != nil {
		return nil, nil, false, err
	}

	if hasStar, cols, typedExprs, err := checkRenderStar(target, info, ivarHelper); err != nil {
		return nil, nil, false, err
	} else if hasStar {
		return cols, typedExprs, hasStar, nil
	}

	col, expr, err := p.computeRender(ctx, target, desiredType, info, ivarHelper, outputName)
	if err != nil {
		return nil, nil, false, err
	}

	return sqlbase.ResultColumns{col}, []parser.TypedExpr{expr}, false, nil
}

// equivalentRenders returns true if and only if the two render expressions
// are equivalent.
func (s *renderNode) equivalentRenders(i, j int) bool {
	return s.isRenderEquivalent(symbolicExprStr(s.render[i]), j)
}

// isRenderEquivalent is a helper function for equivalentRenders() and
// addOrMergeRenders(). Do not use directly.
func (s *renderNode) isRenderEquivalent(exprStr string, j int) bool {
	return symbolicExprStr(s.render[j]) == exprStr
}

// addOrMergeRenders adds the given result columns to the select
// render list and returns their column indices. If an expression is
// already rendered, and the reuse flag is true, no new render is
// added and the index of the existing column is returned instead.
func (s *renderNode) addOrMergeRender(
	col sqlbase.ResultColumn, expr parser.TypedExpr, reuseExistingRender bool,
) (colIdx int) {
	if reuseExistingRender {
		// Now, try to find an equivalent render. We use the syntax
		// representation as approximation of equivalence.  At this
		// point the expressions must have underwent name resolution
		// already so that comparison occurs after replacing column names
		// to IndexedVars.
		exprStr := symbolicExprStr(expr)
		for j := range s.render {
			if s.isRenderEquivalent(exprStr, j) {
				return j
			}
		}
	}

	s.addRenderColumn(expr, col)

	return len(s.render) - 1
}

func (s *renderNode) addOrMergeRenders(
	cols sqlbase.ResultColumns, exprs []parser.TypedExpr, reuseExistingRender bool,
) (colIdxs []int) {
	colIdxs = make([]int, len(cols))
	for i := range cols {
		colIdxs[i] = s.addOrMergeRender(cols[i], exprs[i], reuseExistingRender)
	}
	return colIdxs
}

// symbolicExprStr returns a string representation of the expression using
// symbolic notation. Because the symbolic notation disambiguate columns, this
// string can be used to determine if two expressions are equivalent.
func symbolicExprStr(expr parser.Expr) string {
	return parser.AsStringWithFlags(expr, parser.FmtSymbolicVars)
}

// checkRenderStar handles the case where the target specification contains a
// SQL star (UnqualifiedStar or AllColumnsSelector). We match the prefix of the
// name to one of the tables in the query and then expand the "*" into a list
// of columns. A sqlbase.ResultColumns and Expr pair is returned for each column.
func checkRenderStar(
	target parser.SelectExpr, info multiSourceInfo, ivarHelper parser.IndexedVarHelper,
) (isStar bool, columns sqlbase.ResultColumns, exprs []parser.TypedExpr, err error) {
	v, ok := target.Expr.(parser.VarName)
	if !ok {
		return false, nil, nil, nil
	}

	switch v.(type) {
	case parser.UnqualifiedStar, *parser.AllColumnsSelector:
		if target.As != "" {
			return false, nil, nil, errors.Errorf("\"%s\" cannot be aliased", v)
		}

		columns, exprs, err = info[0].expandStar(v, ivarHelper)
		return true, columns, exprs, err
	default:
		return false, nil, nil, nil
	}
}
