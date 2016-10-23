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
// Author: Radu Berinde (radu@cockroachlabs.com)
//
// This file implements the select code that deals with column references
// and resolving column names in expressions.

package sql

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// invalidSrcIdx is the srcIdx value returned by findColumn() when there is no match.
const invalidSrcIdx = -1

// invalidColIdx is the colIdx value returned by findColumn() when there is no match.
// We reuse the value from parser.InvalidColIdx because its meaning is the same.
const invalidColIdx = parser.InvalidColIdx

// nameResolutionVisitor is a parser.Visitor implementation used to
// resolve the column names in an expression.
type nameResolutionVisitor struct {
	err        error
	sources    multiSourceInfo
	colOffsets []int
	ivarHelper parser.IndexedVarHelper
}

var _ parser.Visitor = &nameResolutionVisitor{}

func (v *nameResolutionVisitor) VisitPre(expr parser.Expr) (recurse bool, newNode parser.Expr) {
	if v.err != nil {
		return false, expr
	}

	switch t := expr.(type) {
	case *parser.IndexedVar:
		// We allow resolving IndexedVars on expressions that have already been resolved by this
		// resolver. This is used in some cases when adding render targets for grouping or sorting.
		v.ivarHelper.AssertSameContainer(t)
		return true, expr

	case parser.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			v.err = err
			return false, expr
		}
		return v.VisitPre(vn)

	case *parser.ColumnItem:
		srcIdx, colIdx, err := v.sources.findColumn(t)
		if err != nil {
			v.err = err
			return false, expr
		}
		ivar := v.ivarHelper.IndexedVar(v.colOffsets[srcIdx] + colIdx)
		return true, ivar

	case *parser.FuncExpr:
		// Check for invalid use of *, which, if it is an argument, is the only argument.
		if len(t.Exprs) != 1 {
			break
		}
		vn, ok := t.Exprs[0].(parser.VarName)
		if !ok {
			break
		}
		vn, v.err = vn.NormalizeVarName()
		if v.err != nil {
			return false, expr
		}
		// Save back to avoid re-doing the work later.
		t.Exprs[0] = vn

		fn, err := t.Name.Normalize()
		if err != nil {
			v.err = err
			return false, expr
		}

		if strings.EqualFold(fn.Function(), "count") {
			// Special case handling for COUNT(*). This is a special construct to
			// count the number of rows; in this case * does NOT refer to a set of
			// columns. A * is invalid elsewhere (and will be caught by TypeCheck()).
			// Replace the function argument with a special non-NULL VariableExpr.
			switch arg := vn.(type) {
			case parser.UnqualifiedStar:
				// Replace, see below.
			case *parser.AllColumnsSelector:
				// Replace, see below.
				// However we must first properly reject SELECT COUNT(foo.*) FROM bar.
				if _, err := v.sources.checkDatabaseName(arg.TableName); err != nil {
					v.err = err
					return false, expr
				}
			default:
				// Nothing to do, recurse.
				return true, expr
			}

			t = t.CopyNode()
			t.Exprs[0] = parser.StarDatumInstance
			return true, t
		}
		return true, t

	case *parser.Subquery:
		// Do not recurse into subqueries.
		return false, expr
	}

	return true, expr
}

func (*nameResolutionVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

func (s *selectNode) resolveNames(expr parser.Expr) (parser.Expr, error) {
	return s.planner.resolveNames(expr, s.sourceInfo, s.ivarHelper)
}

// resolveNames walks the provided expression and resolves all names
// using the tableInfo and ivarHelper.
func (p *planner) resolveNames(
	expr parser.Expr, sources multiSourceInfo, ivarHelper parser.IndexedVarHelper,
) (parser.Expr, error) {
	if expr == nil {
		return nil, nil
	}
	v := &p.nameResolutionVisitor
	*v = nameResolutionVisitor{
		err:        nil,
		sources:    sources,
		colOffsets: make([]int, len(sources)),
		ivarHelper: ivarHelper,
	}
	colOffset := 0
	for i, s := range sources {
		v.colOffsets[i] = colOffset
		colOffset += len(s.sourceColumns)
	}

	expr, _ = parser.WalkExpr(v, expr)
	return expr, v.err
}
