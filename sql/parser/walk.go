// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import "fmt"

// The Visitor Visit method is invoked for each Expr node encountered by
// WalkExpr. The returned Expr replaces the pointer to the visited expression
// in the parent node and can be used for rewriting expressions. The pre
// argument indicates whether the visit is a pre-order or post-order visit. On
// a pre-order visit, if the result Visitor is nil children nodes are skipped
// from the traversal.
type Visitor interface {
	Visit(expr Expr, pre bool) (Visitor, Expr)
}

// Walk implements the Expr interface.
func (expr *AndExpr) Walk(v Visitor) {
	expr.Left = WalkExpr(v, expr.Left)
	expr.Right = WalkExpr(v, expr.Right)
}

// Walk implements the Expr interface.
func (expr *BinaryExpr) Walk(v Visitor) {
	expr.Left = WalkExpr(v, expr.Left)
	expr.Right = WalkExpr(v, expr.Right)
}

// Walk implements the Expr interface.
func (expr *CaseExpr) Walk(v Visitor) {
	if expr.Expr != nil {
		expr.Expr = WalkExpr(v, expr.Expr)
	}
	for _, w := range expr.Whens {
		w.Cond = WalkExpr(v, w.Cond)
		w.Val = WalkExpr(v, w.Val)
	}
	if expr.Else != nil {
		expr.Else = WalkExpr(v, expr.Else)
	}
}

// Walk implements the Expr interface.
func (expr *CastExpr) Walk(v Visitor) {
	expr.Expr = WalkExpr(v, expr.Expr)
}

// Walk implements the Expr interface.
func (expr *CoalesceExpr) Walk(v Visitor) {
	for i := range expr.Exprs {
		expr.Exprs[i] = WalkExpr(v, expr.Exprs[i])
	}
}

// Walk implements the Expr interface.
func (expr *ComparisonExpr) Walk(v Visitor) {
	expr.Left = WalkExpr(v, expr.Left)
	expr.Right = WalkExpr(v, expr.Right)
}

// Walk implements the Expr interface.
func (expr *ExistsExpr) Walk(v Visitor) {
	expr.Subquery = WalkExpr(v, expr.Subquery)
}

// Walk implements the Expr interface.
func (expr *FuncExpr) Walk(v Visitor) {
	for i := range expr.Exprs {
		expr.Exprs[i] = WalkExpr(v, expr.Exprs[i])
	}
}

// Walk implements the Expr interface.
func (expr *IfExpr) Walk(v Visitor) {
	expr.Cond = WalkExpr(v, expr.Cond)
	expr.True = WalkExpr(v, expr.True)
	expr.Else = WalkExpr(v, expr.Else)
}

// Walk implements the Expr interface.
func (*IsOfTypeExpr) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (expr *NotExpr) Walk(v Visitor) {
	expr.Expr = WalkExpr(v, expr.Expr)
}

// Walk implements the Expr interface.
func (expr *NullIfExpr) Walk(v Visitor) {
	expr.Expr1 = WalkExpr(v, expr.Expr1)
	expr.Expr2 = WalkExpr(v, expr.Expr2)
}

// Walk implements the Expr interface.
func (expr *OrExpr) Walk(v Visitor) {
	expr.Left = WalkExpr(v, expr.Left)
	expr.Right = WalkExpr(v, expr.Right)
}

// Walk implements the Expr interface.
func (expr *ParenExpr) Walk(v Visitor) {
	expr.Expr = WalkExpr(v, expr.Expr)
}

// Walk implements the Expr interface.
func (*QualifiedName) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (expr *RangeCond) Walk(v Visitor) {
	expr.Left = WalkExpr(v, expr.Left)
	expr.From = WalkExpr(v, expr.From)
	expr.To = WalkExpr(v, expr.To)
}

// Walk implements the Expr interface.
func (expr *Subquery) Walk(v Visitor) {
	WalkStmt(v, expr.Select)
}

// Walk implements the Expr interface.
func (expr *UnaryExpr) Walk(v Visitor) {
	expr.Expr = WalkExpr(v, expr.Expr)
}

// Walk implements the Expr interface.
func (expr Array) Walk(v Visitor) {
	for i := range expr {
		expr[i] = WalkExpr(v, expr[i])
	}
}

// Walk implements the Expr interface.
func (DefaultVal) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (IntVal) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (NumVal) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (expr Row) Walk(v Visitor) {
	for i := range expr {
		expr[i] = WalkExpr(v, expr[i])
	}
}

// Walk implements the Expr interface.
func (expr Tuple) Walk(v Visitor) {
	for i := range expr {
		expr[i] = WalkExpr(v, expr[i])
	}
}

// Walk implements the Expr interface.
func (ValArg) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (DBool) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (DBytes) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (DDate) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (DFloat) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (DInt) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (DInterval) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (dNull) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (DString) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (DTimestamp) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (DTuple) Walk(_ Visitor) {}

// Walk implements the Expr interface.
func (DValArg) Walk(_ Visitor) {}

// WalkExpr traverses the nodes in an expression.
func WalkExpr(v Visitor, expr Expr) Expr {
	v, expr = v.Visit(expr, true)
	if v == nil {
		return expr
	}

	expr.Walk(v)

	_, expr = v.Visit(expr, false)
	return expr
}

// Args defines the interface for retrieving arguments. Return false for the
// second return value if the argument cannot be found.
type Args interface {
	Arg(name string) (Datum, bool)
}

type MapArgs map[string]Datum

func (m MapArgs) Arg(name string) (Datum, bool) {
	d, ok := m[name]
	return d, ok
}

func (m MapArgs) SetValArg(d, typ Datum) (set bool, err error) {
	v, ok := d.(DValArg)
	if !ok {
		return false, nil
	}
	if t, ok := m[string(v)]; ok && typ != t {
		return false, fmt.Errorf("duplicate parameters of differing types: %s, %s", typ.Type(), t.Type())
	}
	m[string(v)] = typ
	return true,  nil
}

type argVisitor struct {
	args     Args
	optional bool
	err      error
}

var _ Visitor = &argVisitor{}

func (v *argVisitor) Visit(expr Expr, pre bool) (Visitor, Expr) {
	if !pre || v.err != nil {
		return nil, expr
	}
	placeholder, ok := expr.(ValArg)
	if !ok {
		return v, expr
	}
	d, found := v.args.Arg(placeholder.name)
	if !found {
		if v.optional {
			return v, expr
		}
		v.err = fmt.Errorf("arg %s not found", placeholder)
		return nil, expr
	}
	return v, d
}

// FillArgs replaces any placeholder nodes in the expression with arguments
// supplied with the query.
func FillArgs(stmt Statement, args Args) error {
	v := argVisitor{args: args}
	WalkStmt(&v, stmt)
	return v.err
}

// FillArgsOptional replaces any placeholder nodes in the expression with
// arguments supplied with the query. Missing args are ignored.
func FillArgsOptional(stmt Statement, args Args) error {
	v := argVisitor{
		args:     args,
		optional: true,
	}
	WalkStmt(&v, stmt)
	return v.err
}

// WalkStmt walks the entire parsed stmt calling WalkExpr on each
// expression, and replacing each expression with the one returned
// by WalkExpr.
func WalkStmt(v Visitor, stmt Statement) {
	switch stmt := stmt.(type) {
	case *Delete:
		if stmt.Where != nil {
			stmt.Where.Expr = WalkExpr(v, stmt.Where.Expr)
		}
	case *Explain:
		WalkStmt(v, stmt.Statement)
	case *Insert:
		WalkStmt(v, stmt.Rows)
	case *ParenSelect:
		WalkStmt(v, stmt.Select)
	case *Select:
		for i := range stmt.Exprs {
			stmt.Exprs[i].Expr = WalkExpr(v, stmt.Exprs[i].Expr)
		}
		if stmt.Where != nil {
			stmt.Where.Expr = WalkExpr(v, stmt.Where.Expr)
		}
		for i, expr := range stmt.GroupBy {
			stmt.GroupBy[i] = WalkExpr(v, expr)
		}
		if stmt.Having != nil {
			stmt.Having.Expr = WalkExpr(v, stmt.Having.Expr)
		}
		for i, expr := range stmt.OrderBy {
			stmt.OrderBy[i].Expr = WalkExpr(v, expr.Expr)
		}
		if stmt.Limit != nil {
			if stmt.Limit.Offset != nil {
				stmt.Limit.Offset = WalkExpr(v, stmt.Limit.Offset)
			}
			if stmt.Limit.Count != nil {
				stmt.Limit.Count = WalkExpr(v, stmt.Limit.Count)
			}
		}
	case *Set:
		for i, expr := range stmt.Values {
			stmt.Values[i] = WalkExpr(v, expr)
		}
	case *Update:
		for i, expr := range stmt.Exprs {
			stmt.Exprs[i].Expr = WalkExpr(v, expr.Expr)
		}
		if stmt.Where != nil {
			stmt.Where.Expr = WalkExpr(v, stmt.Where.Expr)
		}
	case Values:
		for i, tuple := range stmt {
			stmt[i] = WalkExpr(v, tuple).(Tuple)
		}
	}
}

type containsSubqueryVisitor struct {
	containsSubquery bool
}

var _ Visitor = &containsSubqueryVisitor{}

func (v *containsSubqueryVisitor) Visit(expr Expr, pre bool) (Visitor, Expr) {
	if pre && !v.containsSubquery {
		switch expr.(type) {
		case *Subquery:
			v.containsSubquery = true
			return nil, expr
		}
	}
	return v, expr
}

// containsSubquery returns true if the expression contains a subquery.
func containsSubquery(expr Expr) bool {
	v := containsSubqueryVisitor{containsSubquery: false}
	_ = WalkExpr(&v, expr)
	return v.containsSubquery
}
