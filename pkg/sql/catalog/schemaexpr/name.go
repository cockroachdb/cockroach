// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// This file implements the select code that deals with column references
// and resolving column names in expressions.

package schemaexpr

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// nameResolutionVisitor is a tree.Visitor implementation used to resolve column
// names in an expression as indexed vars.
type nameResolutionVisitor struct {
	err        error
	iVarHelper tree.IndexedVarHelper
	resolver   colinfo.ColumnResolver
}

var _ tree.Visitor = &nameResolutionVisitor{}

// VisitPre implements tree.Visitor.
func (v *nameResolutionVisitor) VisitPre(expr tree.Expr) (recurse bool, newNode tree.Expr) {
	if v.err != nil {
		return false, expr
	}

	switch t := expr.(type) {
	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			v.err = err
			return false, expr
		}
		return v.VisitPre(vn)

	case *tree.ColumnItem:
		_, err := colinfo.ResolveColumnItem(context.Background(), &v.resolver, t)
		if err != nil {
			v.err = err
			return false, expr
		}

		colIdx := v.resolver.ResolverState.ColIdx
		ivar := v.iVarHelper.IndexedVar(colIdx)
		return true, ivar
	}
	return true, expr
}

// VisitPost implements tree.Visitor.
func (*nameResolutionVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }
