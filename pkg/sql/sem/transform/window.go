// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package transform

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

var _ tree.Visitor = &ContainsWindowVisitor{}

// ContainsWindowVisitor checks if walked expressions contain window functions.
type ContainsWindowVisitor struct {
	sawWindow bool
}

// VisitPre satisfies the Visitor interface.
func (v *ContainsWindowVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *tree.FuncExpr:
		if t.IsWindowFunctionApplication() {
			v.sawWindow = true
			return false, expr
		}
	case *tree.Subquery:
		return false, expr
	}
	return true, expr
}

// VisitPost satisfies the Visitor interface.
func (*ContainsWindowVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// ContainsWindowFunc determines if an Expr contains a window function.
func (v *ContainsWindowVisitor) ContainsWindowFunc(expr tree.Expr) bool {
	if expr != nil {
		tree.WalkExprConst(v, expr)
		ret := v.sawWindow
		v.sawWindow = false
		return ret
	}
	return false
}
