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
// This file implements the select code that deals with column references
// and resolving column names in expressions.

package sql

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// nameResolutionVisitor is a tree.Visitor implementation used to
// resolve the column names in an expression.
type nameResolutionVisitor struct {
	err        error
	sources    sqlbase.MultiSourceInfo
	iVarHelper tree.IndexedVarHelper
	searchPath sessiondata.SearchPath
	resolver   sqlbase.ColumnResolver

	// foundDependentVars is set to true during the analysis if an
	// expression was found which can change values between rows of the
	// same data source, for example IndexedVars and calls to the
	// random() function.
	foundDependentVars bool
	// foundStars is set to true if a star is expanded during name
	// resolution, e.g. in SELECT (kv.*) FROM kv.
	foundStars bool
}

var _ tree.Visitor = &nameResolutionVisitor{}

func makeUntypedTuple(texprs []tree.TypedExpr) *tree.Tuple {
	exprs := make(tree.Exprs, len(texprs))
	for i, e := range texprs {
		exprs[i] = e
	}
	return &tree.Tuple{Exprs: exprs}
}

func (v *nameResolutionVisitor) VisitPre(expr tree.Expr) (recurse bool, newNode tree.Expr) {
	if v.err != nil {
		return false, expr
	}

	switch t := expr.(type) {
	case tree.UnqualifiedStar:
		v.foundDependentVars = true

	case *tree.AllColumnsSelector:
		v.foundStars = true
		// AllColumnsSelector at the top level of a SELECT clause are
		// replaced when the select's renders are prepared. If we
		// encounter one here during expression analysis, it's being used
		// as an argument to an inner expression/function. In that case,
		// treat it as a tuple of the expanded columns.
		//
		// Hence:
		//    SELECT kv.* FROM kv                 -> SELECT k, v FROM kv
		//    SELECT (kv.*) FROM kv               -> SELECT (k, v) FROM kv
		//    SELECT COUNT(DISTINCT kv.*) FROM kv -> SELECT COUNT(DISTINCT (k, v)) FROM kv
		//
		_, exprs, err := expandStar(context.TODO(), v.sources, t, v.iVarHelper)
		if err != nil {
			v.err = err
			return false, expr
		}
		if len(exprs) > 0 {
			// If the star expanded to more than one column, then there are
			// dependent vars.
			v.foundDependentVars = true
		}
		// We return an untyped tuple because name resolution occurs
		// before type checking, and type checking will resolve the
		// tuple's type.
		return false, makeUntypedTuple(exprs)

	case *tree.IndexedVar:
		// If the indexed var is a standalone ordinal reference, ensure it
		// becomes a fully bound indexed var.
		t, v.err = v.iVarHelper.BindIfUnbound(t)
		if v.err != nil {
			return false, expr
		}

		v.foundDependentVars = true
		return false, t

	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			v.err = err
			return false, expr
		}
		return v.VisitPre(vn)

	case *tree.ColumnItem:
		v.resolver.ResolverState.ForUpdateOrDelete = t.ForUpdateOrDelete
		_, err := t.Resolve(context.TODO(), &v.resolver)
		if err != nil {
			v.err = err
			return false, expr
		}

		srcIdx := v.resolver.ResolverState.SrcIdx
		colIdx := v.resolver.ResolverState.ColIdx
		ivar := v.iVarHelper.IndexedVar(v.sources[srcIdx].ColOffset + colIdx)
		v.foundDependentVars = true
		return true, ivar

	case *tree.FuncExpr:
		fd, err := t.Func.Resolve(v.searchPath)
		if err != nil {
			v.err = err
			return false, expr
		}

		if fd.HasOverloadsNeedingRepeatedEvaluation {
			// TODO(knz): this property should really be an attribute of the
			// individual overloads. By looking at the name-level property
			// indicator, we are marking a function as row-dependent as
			// soon as one overload is, even if the particular overload
			// that would be selected by type checking for this FuncExpr
			// is constant. This could be more fine-grained.
			v.foundDependentVars = true
		}

		// Check for invalid use of *, which, if it is an argument, is the only argument.
		if len(t.Exprs) != 1 {
			break
		}
		vn, ok := t.Exprs[0].(tree.VarName)
		if !ok {
			break
		}
		vn, v.err = vn.NormalizeVarName()
		if v.err != nil {
			return false, expr
		}
		// Save back to avoid re-doing the work later.
		t.Exprs[0] = vn

		if strings.EqualFold(fd.Name, "count") && t.Type == 0 {
			if _, ok := vn.(tree.UnqualifiedStar); ok {
				// Special case handling for COUNT(*). This is a special construct to
				// count the number of rows; in this case * does NOT refer to a set of
				// columns. A * is invalid elsewhere (and will be caught by TypeCheck()).
				// Replace the function with COUNT_ROWS (which doesn't take any
				// arguments).
				e := &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: &tree.UnresolvedName{
							NumParts: 1, Parts: tree.NameParts{"count_rows"},
						},
					},
				}
				// We call TypeCheck to fill in FuncExpr internals. This is a fixed
				// expression; we should not hit an error here.
				if _, err := e.TypeCheck(&tree.SemaContext{}, types.Any); err != nil {
					panic(err)
				}
				e.Filter = t.Filter
				e.WindowDef = t.WindowDef
				return true, e
			}
		}
		// TODO(#15750): support additional forms:
		//
		//   COUNT(t.*): looks like this behaves the same as COUNT(*) in PG (perhaps
		//               t.* becomes a tuple and the tuple itself is never NULL?)
		//
		//   COUNT(DISTINCT t.*): this deduplicates between the tuples. Note that
		//                        this can be part of a join:
		//                          SELECT COUNT(DISTINCT t.*) FROM t, u
		return true, t

	case *tree.Subquery:
		// Do not recurse into subqueries.
		return false, expr
	}

	return true, expr
}

func (*nameResolutionVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// resolveNamesForRender resolves the names in expr using the naming
// context of the given renderNode (FROM clause).
func (p *planner) resolveNamesForRender(
	expr tree.Expr, s *renderNode,
) (tree.Expr, bool, bool, error) {
	return p.resolveNames(expr, s.sourceInfo, s.ivarHelper)
}

// resolveNames walks the provided expression and resolves all names
// using the tableInfo and iVarHelper.
// If anything that looks like a column reference (indexed vars, star,
// etc) is encountered, or a function that may change value for every
// row in a table, the 2nd return value is true.
// If any star is expanded, the 3rd return value is true.
func (p *planner) resolveNames(
	expr tree.Expr, sources sqlbase.MultiSourceInfo, ivarHelper tree.IndexedVarHelper,
) (tree.Expr, bool, bool, error) {
	if expr == nil {
		return nil, false, false, nil
	}
	v := &p.nameResolutionVisitor
	*v = nameResolutionVisitor{
		err:                nil,
		sources:            sources,
		iVarHelper:         ivarHelper,
		searchPath:         p.SessionData().SearchPath,
		foundDependentVars: false,
		resolver: sqlbase.ColumnResolver{
			Sources: sources,
		},
	}
	return resolveNamesUsingVisitor(expr, v)
}

func resolveNames(
	expr tree.Expr,
	sources sqlbase.MultiSourceInfo,
	ivarHelper tree.IndexedVarHelper,
	searchPath sessiondata.SearchPath,
) (tree.Expr, bool, bool, error) {
	v := &nameResolutionVisitor{
		err:                nil,
		sources:            sources,
		iVarHelper:         ivarHelper,
		searchPath:         searchPath,
		foundDependentVars: false,
		resolver: sqlbase.ColumnResolver{
			Sources: sources,
		},
	}
	return resolveNamesUsingVisitor(expr, v)
}

func resolveNamesUsingVisitor(
	expr tree.Expr, v *nameResolutionVisitor,
) (tree.Expr, bool, bool, error) {
	colOffset := 0
	for _, s := range v.sources {
		s.ColOffset = colOffset
		colOffset += len(s.SourceColumns)
	}

	expr, _ = tree.WalkExpr(v, expr)
	return expr, v.foundDependentVars, v.foundStars, v.err
}
