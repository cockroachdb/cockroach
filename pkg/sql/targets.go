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
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/pkg/errors"
)

// computeRender expands a target expression into a result column (or more
// than one, in case there is a star). Whether star expansion occurred
// is indicated by the third return value.
func (p *planner) computeRender(
	target parser.SelectExpr,
	desiredType parser.Type,
	info *dataSourceInfo,
	ivarHelper parser.IndexedVarHelper,
	allowStars bool,
) (columns ResultColumns, exprs []parser.TypedExpr, hasStar bool, err error) {
	// Pre-normalize any VarName so the work is not done twice below.
	if err := target.NormalizeTopLevelVarName(); err != nil {
		return nil, nil, false, err
	}

	if hasStar, cols, typedExprs, err := checkRenderStar(
		target, info, ivarHelper, allowStars); err != nil {
		return nil, nil, false, err
	} else if hasStar {
		return cols, typedExprs, hasStar, nil
	}

	// When generating an output column name it should exactly match the original
	// expression, so determine the output column name before we perform any
	// manipulations to the expression.
	outputName := getRenderColName(target)

	normalized, err := p.analyzeExpr(
		target.Expr, multiSourceInfo{info}, ivarHelper, desiredType, false, "")
	if err != nil {
		return nil, nil, false, err
	}

	return ResultColumns{
		ResultColumn{Name: outputName, Typ: normalized.ResolvedType()},
	}, []parser.TypedExpr{normalized}, false, nil
}

// equivalentRenders returns true if and only if the two render expressions
// are equivalent.
func (s *renderNode) equivalentRenders(i, j int) bool {
	firstExprStr := parser.AsStringWithFlags(s.render[i], parser.FmtSymbolicVars)
	return s.isRenderEquivalent(firstExprStr, j)
}

// isRenderEquivalent is a helper function for equivalentRenders() and
// addOrMergeRenders(). Do not use directly.
func (s *renderNode) isRenderEquivalent(exprStr string, j int) bool {
	return parser.AsStringWithFlags(s.render[j], parser.FmtSymbolicVars) == exprStr
}

// addOrMergeRenders adds the given result columns to the select
// render list and returns their column indices. If an expression is
// already rendered, and the reuse flag is true, no new render is
// added and the index of the existing column is returned instead.
func (s *renderNode) addOrMergeRenders(
	cols ResultColumns, exprs []parser.TypedExpr, reuseExistingRender bool,
) (colIdxs []int) {
	colIdxs = make([]int, len(cols))
	for i := range cols {
		index := -1

		if reuseExistingRender {
			// Now, try to find an equivalent render. We use the syntax
			// representation as approximation of equivalence.  At this
			// point the expressions must have underwent name resolution
			// already so that comparison occurs after replacing column names
			// to IndexedVars.
			exprStr := parser.AsStringWithFlags(exprs[i], parser.FmtSymbolicVars)
			for j := range s.render {
				if s.isRenderEquivalent(exprStr, j) {
					index = j
					break
				}
			}
		}

		if index == -1 {
			index = len(s.render)
			s.addRenderColumn(exprs[i], cols[i])
		}
		colIdxs[i] = index
	}

	return colIdxs
}

// checkRenderStar handles the case where the target specification
// contains a SQL star (UnqualifiedStar or AllColumnsSelector).  In
// the case where the context disallows stars (allowStars false), an
// error is reported. If star expansion is allowed, we match the
// prefix of the name to one of the tables in the query and then
// expand the "*" into a list of columns. A ResultColumns and Expr
// pair is returned for each column.
func checkRenderStar(
	target parser.SelectExpr,
	src *dataSourceInfo,
	ivarHelper parser.IndexedVarHelper,
	allowStars bool,
) (isStar bool, columns ResultColumns, exprs []parser.TypedExpr, err error) {
	v, ok := target.Expr.(parser.VarName)
	if !ok {
		return false, nil, nil, nil
	}
	switch v.(type) {
	case parser.UnqualifiedStar, *parser.AllColumnsSelector:
		if !allowStars {
			return false, nil, nil, errors.Errorf("\"%s\" is not allowed in this position", v)
		}
	default:
		return false, nil, nil, nil
	}

	if target.As != "" {
		return false, nil, nil, errors.Errorf("\"%s\" cannot be aliased", v)
	}

	columns, exprs, err = src.expandStar(v, ivarHelper)
	return true, columns, exprs, err
}
