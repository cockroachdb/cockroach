// Copyright 2017 The Cockroach Authors.
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

package transform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

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

var _ tree.Visitor = &WindowAggCheckVisitor{}

// WindowAggCheckVisitor checks if walked expressions contain valid aggregations.
type WindowAggCheckVisitor struct {
	err error
}

// VisitPre satisfies the Visitor interface.
func (v *WindowAggCheckVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}
	switch t := expr.(type) {
	case *tree.FuncExpr:
		if t.GetAggregateConstructor() != nil && len(t.Exprs) > 1 {
			v.err = pgerror.UnimplementedWithIssueError(10495,
				"aggregate functions with multiple arguments are not supported yet")
			return false, expr
		}
	case *tree.Subquery:
		return false, expr
	}
	return true, expr
}

// VisitPost satisfies the Visitor interface.
func (*WindowAggCheckVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// CheckAggregates determines if aggregates in an Expr are valid.
func (v *WindowAggCheckVisitor) CheckAggregates(expr tree.Expr) error {
	if expr != nil {
		v.err = nil
		tree.WalkExprConst(v, expr)
		return v.err
	}
	return nil
}
