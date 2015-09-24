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

// WalkExpr traverses the nodes in an expression. It starts by calling
// v.Visit(expr, true). If the visitor returned by v.Visit(expr, true) is not
// nil it recursively calls WalkExpr on the children of the node returned by
// v.Visit(expr, true) and finishes with a call to v.Visit(expr, false).
func WalkExpr(v Visitor, expr Expr) Expr {
	v, expr = v.Visit(expr, true)
	if v == nil {
		return expr
	}

	switch t := expr.(type) {
	case *AndExpr:
		t.Left = WalkExpr(v, t.Left)
		t.Right = WalkExpr(v, t.Right)

	case *OrExpr:
		t.Left = WalkExpr(v, t.Left)
		t.Right = WalkExpr(v, t.Right)

	case *NotExpr:
		t.Expr = WalkExpr(v, t.Expr)

	case *ParenExpr:
		t.Expr = WalkExpr(v, t.Expr)

	case *ComparisonExpr:
		t.Left = WalkExpr(v, t.Left)
		t.Right = WalkExpr(v, t.Right)

	case *RangeCond:
		t.Left = WalkExpr(v, t.Left)
		t.From = WalkExpr(v, t.From)
		t.To = WalkExpr(v, t.To)

	case *IsOfTypeExpr:
		t.Expr = WalkExpr(v, t.Expr)

	case *ExistsExpr:
		WalkStmt(v, t.Subquery.Select)

	case *IfExpr:
		t.Cond = WalkExpr(v, t.Cond)
		t.True = WalkExpr(v, t.True)
		t.Else = WalkExpr(v, t.Else)

	case *NullIfExpr:
		t.Expr1 = WalkExpr(v, t.Expr1)
		t.Expr2 = WalkExpr(v, t.Expr2)

	case *CoalesceExpr:
		for i := range t.Exprs {
			t.Exprs[i] = WalkExpr(v, t.Exprs[i])
		}

	case IntVal:
		// Terminal node: nothing to do.

	case NumVal:
		// Terminal node: nothing to do.

	case DefaultVal:
		// Terminal node: nothing to do.

	case ValArg:
		// Terminal node: nothing to do.

	case *QualifiedName:
		// Terminal node: nothing to do.

	case Row:
		for i := range t {
			t[i] = WalkExpr(v, t[i])
		}

	case Tuple:
		for i := range t {
			t[i] = WalkExpr(v, t[i])
		}

	case Array:
		for i := range t {
			t[i] = WalkExpr(v, t[i])
		}

	case DReference:
		// Terminal node: nothing to do.

	case Datum:
		// Terminal node: nothing to do.

	case *Subquery:
		WalkStmt(v, t.Select)

	case *BinaryExpr:
		t.Left = WalkExpr(v, t.Left)
		t.Right = WalkExpr(v, t.Right)

	case *UnaryExpr:
		t.Expr = WalkExpr(v, t.Expr)

	case *FuncExpr:
		for i := range t.Exprs {
			t.Exprs[i] = WalkExpr(v, t.Exprs[i])
		}

	case *CaseExpr:
		if t.Expr != nil {
			t.Expr = WalkExpr(v, t.Expr)
		}
		for _, w := range t.Whens {
			w.Cond = WalkExpr(v, w.Cond)
			w.Val = WalkExpr(v, w.Val)
		}
		if t.Else != nil {
			t.Else = WalkExpr(v, t.Else)
		}

	case *CastExpr:
		t.Expr = WalkExpr(v, t.Expr)

	default:
		panic(fmt.Sprintf("walk: unsupported expression type: %T", expr))
	}

	_, expr = v.Visit(expr, false)
	return expr
}

// Args defines the interface for retrieving arguments. Return false for the
// second return value if the argument cannot be found.
type Args interface {
	Arg(name string) (Datum, bool)
}

type argVisitor struct {
	args Args
	err  error
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
	d, found := v.args.Arg(string(placeholder))
	if !found {
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
	expr = WalkExpr(&v, expr)
	return v.containsSubquery
}
