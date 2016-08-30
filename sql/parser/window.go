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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package parser

var _ Visitor = &ContainsWindowVisitor{}

// ContainsWindowVisitor checks if walked expressions contain window functions.
type ContainsWindowVisitor struct {
	SawWindow bool
}

// VisitPre satisfies the Visitor interface.
func (v *ContainsWindowVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	case *FuncExpr:
		if t.IsWindowFunction() {
			v.SawWindow = true
			return false, expr
		}
	case *Subquery:
		return false, expr
	}
	return true, expr
}

// VisitPost satisfies the Visitor interface.
func (*ContainsWindowVisitor) VisitPost(expr Expr) Expr { return expr }

// Reset clear the ContainsWindowVisitor's internal state.
func (v *ContainsWindowVisitor) Reset() {
	v.SawWindow = false
}

// ContainsWindowFunc determines if an Expr contains a window function.
func (v *ContainsWindowVisitor) ContainsWindowFunc(expr Expr) bool {
	if expr != nil {
		defer v.Reset()
		WalkExprConst(v, expr)
		return v.SawWindow
	}
	return false
}

// WindowFuncInExpr determines if an Expr contains a window function, using
// the Parser's embedded visitor.
func (p *Parser) WindowFuncInExpr(expr Expr) bool {
	return p.containsWindowVisitor.ContainsWindowFunc(expr)
}

// WindowFuncInExprs determines if any of the provided TypedExpr contains a
// window function, using the Parser's embedded visitor.
func (p *Parser) WindowFuncInExprs(exprs []TypedExpr) bool {
	defer p.containsWindowVisitor.Reset()
	for _, expr := range exprs {
		WalkExprConst(&p.containsWindowVisitor, expr)
		if p.containsWindowVisitor.SawWindow {
			return true
		}
	}
	return false
}
