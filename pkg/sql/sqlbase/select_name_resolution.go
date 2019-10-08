// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// This file implements the select code that deals with column references
// and resolving column names in expressions.

package sqlbase

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NameResolutionVisitor is a tree.Visitor implementation used to
// resolve the column names in an expression.
type NameResolutionVisitor struct {
	err        error
	sources    MultiSourceInfo
	iVarHelper tree.IndexedVarHelper
	searchPath sessiondata.SearchPath
	resolver   ColumnResolver

	// foundDependentVars is set to true during the analysis if an
	// expression was found which can change values between rows of the
	// same data source, for example IndexedVars and calls to the
	// random() function.
	foundDependentVars bool
	// foundStars is set to true if a star is expanded during name
	// resolution, e.g. in SELECT (kv.*) FROM kv.
	foundStars bool
}

var _ tree.Visitor = &NameResolutionVisitor{}

func makeUntypedTuple(labels []string, texprs []tree.TypedExpr) *tree.Tuple {
	exprs := make(tree.Exprs, len(texprs))
	for i, e := range texprs {
		exprs[i] = e
	}
	return &tree.Tuple{Exprs: exprs, Labels: labels}
}

// VisitPre implements tree.Visitor.
func (v *NameResolutionVisitor) VisitPre(expr tree.Expr) (recurse bool, newNode tree.Expr) {
	if v.err != nil {
		return false, expr
	}

	switch t := expr.(type) {
	case tree.UnqualifiedStar:
		v.foundDependentVars = true

	case *tree.TupleStar:
		// TupleStars at the top level of a SELECT clause are replaced
		// when the select's renders are prepared. If we encounter one
		// here during expression analysis, it's being used as an argument
		// to an inner expression. In that case, we just report its tuple
		// operand unchanged.
		v.foundStars = true
		return v.VisitPre(t.Expr)

	case *tree.AllColumnsSelector:
		v.foundStars = true
		// AllColumnsSelectors at the top level of a SELECT clause are
		// replaced when the select's renders are prepared. If we
		// encounter one here during expression analysis, it's being used
		// as an argument to an inner expression/function. In that case,
		// treat it as a tuple of the expanded columns.
		//
		// Hence:
		//    SELECT kv.* FROM kv                 -> SELECT k, v FROM kv
		//    SELECT (kv.*) FROM kv               -> SELECT (k, v) FROM kv
		//    SELECT count(DISTINCT kv.*) FROM kv -> SELECT count(DISTINCT (k, v)) FROM kv
		//
		cols, exprs, err := expandStar(context.TODO(), v.sources, t, v.iVarHelper)
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
		// tuple's type. However we need to preserve the labels in
		// case of e.g. `SELECT (kv.*).v`.
		labels := make([]string, len(exprs))
		for i := range exprs {
			labels[i] = cols[i].Name
		}
		return false, makeUntypedTuple(labels, exprs)

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

		if fd.NeedsRepeatedEvaluation {
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

// VisitPost implements tree.Visitor.
func (*NameResolutionVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// ResolveNames is a wrapper around ResolveNamesUsingVisitor.
func ResolveNames(
	expr tree.Expr,
	sources MultiSourceInfo,
	ivarHelper tree.IndexedVarHelper,
	searchPath sessiondata.SearchPath,
) (tree.Expr, bool, bool, error) {
	var v NameResolutionVisitor
	return ResolveNamesUsingVisitor(&v, expr, sources, ivarHelper, searchPath)
}

// ResolveNamesUsingVisitor resolves the names in the given expression. It
// returns the resolved expression, whether it found dependent vars, and
// whether it found stars.
func ResolveNamesUsingVisitor(
	v *NameResolutionVisitor,
	expr tree.Expr,
	sources MultiSourceInfo,
	ivarHelper tree.IndexedVarHelper,
	searchPath sessiondata.SearchPath,
) (tree.Expr, bool, bool, error) {
	*v = NameResolutionVisitor{
		sources:    sources,
		iVarHelper: ivarHelper,
		searchPath: searchPath,
		resolver: ColumnResolver{
			Sources: sources,
		},
	}
	colOffset := 0
	for _, s := range v.sources {
		s.ColOffset = colOffset
		colOffset += len(s.SourceColumns)
	}

	expr, _ = tree.WalkExpr(v, expr)
	return expr, v.foundDependentVars, v.foundStars, v.err
}

// expandStar returns the array of column metadata and name
// expressions that correspond to the expansion of a star.
func expandStar(
	ctx context.Context, src MultiSourceInfo, v tree.VarName, ivarHelper tree.IndexedVarHelper,
) (columns ResultColumns, exprs []tree.TypedExpr, err error) {
	if len(src) == 0 || len(src[0].SourceColumns) == 0 {
		return nil, nil, pgerror.Newf(pgcode.InvalidName,
			"cannot use %q without a FROM clause", tree.ErrString(v))
	}

	colSel := func(src *DataSourceInfo, idx int) {
		col := src.SourceColumns[idx]
		if !col.Hidden {
			ivar := ivarHelper.IndexedVar(idx + src.ColOffset)
			columns = append(columns, ResultColumn{Name: col.Name, Typ: ivar.ResolvedType()})
			exprs = append(exprs, ivar)
		}
	}

	switch sel := v.(type) {
	case tree.UnqualifiedStar:
		// Simple case: a straight '*'. Take all columns.
		for _, ds := range src {
			for i := 0; i < len(ds.SourceColumns); i++ {
				colSel(ds, i)
			}
		}
	case *tree.AllColumnsSelector:
		resolver := ColumnResolver{Sources: src}
		_, _, err := sel.Resolve(ctx, &resolver)
		if err != nil {
			return nil, nil, err
		}
		ds := src[resolver.ResolverState.SrcIdx]
		colSet := ds.SourceAliases[resolver.ResolverState.ColSetIdx].ColumnSet
		for i, ok := colSet.Next(0); ok; i, ok = colSet.Next(i + 1) {
			colSel(ds, i)
		}
	}

	return columns, exprs, nil
}
