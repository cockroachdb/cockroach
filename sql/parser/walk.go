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
// in the parent node and can be used for rewriting expressions.
type Visitor interface {
	Visit(expr Expr) Expr
}

// WalkExpr traverses the nodes in an expression. It starts by calling
// v.Visit(expr). It then recursively traverses the children nodes of the
// expression returned by v.Visit().
func WalkExpr(v Visitor, expr Expr) Expr {
	expr = v.Visit(expr)

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

	case *NullCheck:
		t.Expr = WalkExpr(v, t.Expr)

	case *ExistsExpr:
		// TODO(pmattis): Should we recurse into the Subquery?

	case BytesVal:
		// Terminal node: nothing to do.

	case StrVal:
		// Terminal node: nothing to do.

	case IntVal:
		// Terminal node: nothing to do.

	case NumVal:
		// Terminal node: nothing to do.

	case BoolVal:
		// Terminal node: nothing to do.

	case ValArg:
		// Terminal node: nothing to do.

	case NullVal:
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

	case Datum:
		// Terminal node: nothing to do.

	case *StarExpr:
		// Contains only a terminal node.

	case *Subquery:
		// TODO(pmattis): Should we recurse into the Subquery?

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
		panic(fmt.Sprintf("unsupported expression type: %T", expr))
	}

	return expr
}

// Args defines the interface for retrieving arguments. Return false for the
// second return value if the argument cannot be found.
type Args interface {
	Arg(i int) (Datum, bool)
}

type argVisitor struct {
	args Args
	err  error
}

var _ Visitor = &argVisitor{}

func (v *argVisitor) Visit(expr Expr) Expr {
	if v.err != nil {
		return expr
	}
	placeholder, ok := expr.(ValArg)
	if !ok {
		return expr
	}
	d, found := v.args.Arg(int(placeholder))
	if !found {
		v.err = fmt.Errorf("arg %s not found", placeholder)
		return expr
	}
	return d
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
	case *Insert:
		WalkStmt(v, stmt.Rows)
	case *ParenSelect:
		WalkStmt(v, stmt.Select)
	case *Select:
		for _, expr := range stmt.Exprs {
			switch expr := expr.(type) {
			case *NonStarExpr:
				expr.Expr = WalkExpr(v, expr.Expr)
			}
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
