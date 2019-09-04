// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// RunFilter runs a filter expression and returns whether the filter passes.
func RunFilter(filter tree.TypedExpr, evalCtx *tree.EvalContext) (bool, error) {
	if filter == nil {
		return true, nil
	}

	d, err := filter.Eval(evalCtx)
	if err != nil {
		return false, err
	}

	return d == tree.DBoolTrue, nil
}

type ivarRemapper struct {
	indexVarMap []int
}

var _ tree.Visitor = &ivarRemapper{}

func (v *ivarRemapper) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if ivar, ok := expr.(*tree.IndexedVar); ok {
		newIvar := *ivar
		newIvar.Idx = v.indexVarMap[ivar.Idx]
		return false, &newIvar
	}
	return true, expr
}

func (*ivarRemapper) VisitPost(expr tree.Expr) tree.Expr { return expr }

// RemapIVarsInTypedExpr remaps tree.IndexedVars in expr using indexVarMap.
// Note that a new expression is returned.
func RemapIVarsInTypedExpr(expr tree.TypedExpr, indexVarMap []int) tree.TypedExpr {
	v := &ivarRemapper{indexVarMap: indexVarMap}
	newExpr, _ := tree.WalkExpr(v, expr)
	return newExpr.(tree.TypedExpr)
}
