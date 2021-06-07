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

package schemaexpr

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// NameResolutionVisitor is a tree.Visitor implementation used to
// resolve the column names in an expression.
type NameResolutionVisitor struct {
	err        error
	iVarHelper tree.IndexedVarHelper
	searchPath sessiondata.SearchPath
	resolver   colinfo.ColumnResolver
}

var _ tree.Visitor = &NameResolutionVisitor{}

// VisitPre implements tree.Visitor.
func (v *NameResolutionVisitor) VisitPre(expr tree.Expr) (recurse bool, newNode tree.Expr) {
	if v.err != nil {
		return false, expr
	}

	switch t := expr.(type) {
	case *tree.IndexedVar:
		// If the indexed var is a standalone ordinal reference, ensure it
		// becomes a fully bound indexed var.
		t, v.err = v.iVarHelper.BindIfUnbound(t)
		if v.err != nil {
			return false, expr
		}

		return false, t

	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			v.err = err
			return false, expr
		}
		return v.VisitPre(vn)

	case *tree.ColumnItem:
		_, err := colinfo.ResolveColumnItem(context.TODO(), &v.resolver, t)
		if err != nil {
			v.err = err
			return false, expr
		}

		colIdx := v.resolver.ResolverState.ColIdx
		ivar := v.iVarHelper.IndexedVar(colIdx)
		return true, ivar

	case *tree.FuncExpr:
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
		return true, t
	}

	return true, expr
}

// VisitPost implements tree.Visitor.
func (*NameResolutionVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// ResolveNamesUsingVisitor resolves the names in the given expression.
func ResolveNamesUsingVisitor(
	v *NameResolutionVisitor,
	expr tree.Expr,
	source *colinfo.DataSourceInfo,
	ivarHelper tree.IndexedVarHelper,
	searchPath sessiondata.SearchPath,
) (tree.Expr, error) {
	*v = NameResolutionVisitor{
		iVarHelper: ivarHelper,
		searchPath: searchPath,
		resolver: colinfo.ColumnResolver{
			Source: source,
		},
	}

	expr, _ = tree.WalkExpr(v, expr)
	return expr, v.err
}
