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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import "reflect"

// Visitor defines methods that are called for nodes during an expression or statement walk.
type Visitor interface {
	// VisitPre is called for each node before recursing into that subtree. Upon return, if recurse
	// is false, the visit will not recurse into the subtree (and VisitPost will not be called for
	// this node).
	//
	// The returned Expr replaces the visited expression and can be used for rewriting expressions.
	// The function should NOT modify nodes in-place; it should make copies of nodes. The Walk
	// infrastructure will automatically make copies of parents as needed.
	VisitPre(expr Expr) (recurse bool, newExpr Expr)

	// VisitPost is called for each node after recursing into the subtree. The returned Expr
	// replaces the visited expression and can be used for rewriting expressions.
	//
	// The returned Expr replaces the visited expression and can be used for rewriting expressions.
	// The function should NOT modify nodes in-place; it should make and return copies of nodes. The
	// Walk infrastructure will automatically make copies of parents as needed.
	VisitPost(expr Expr) (newNode Expr)
}

func copyQualifiedNames(q QualifiedNames) QualifiedNames {
	if q == nil {
		return nil
	}
	copy := QualifiedNames(make([]*QualifiedName, len(q)))
	for i, n := range q {
		qCopy := *n
		copy[i] = &qCopy
	}
	return copy
}

// Walk implements the Expr interface.
func (expr *AndExpr) Walk(v Visitor) Expr {
	left, changedL := WalkExpr(v, expr.Left)
	right, changedR := WalkExpr(v, expr.Right)
	if changedL || changedR {
		exprCopy := *expr
		exprCopy.Left = left
		exprCopy.Right = right
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *BinaryExpr) Walk(v Visitor) Expr {
	left, changedL := WalkExpr(v, expr.Left)
	right, changedR := WalkExpr(v, expr.Right)
	if changedL || changedR {
		exprCopy := *expr
		exprCopy.Left = left
		exprCopy.Right = right
		return &exprCopy
	}
	return expr
}

// CopyNode makes a copy of this Expr without recursing in any child Exprs.
func (expr *CaseExpr) CopyNode() *CaseExpr {
	exprCopy := *expr
	// Copy the Whens slice.
	exprCopy.Whens = make([]*When, len(expr.Whens))
	for i, w := range expr.Whens {
		wCopy := *w
		exprCopy.Whens[i] = &wCopy
	}
	return &exprCopy
}

// Walk implements the Expr interface.
func (expr *CaseExpr) Walk(v Visitor) Expr {
	ret := expr

	if expr.Expr != nil {
		e, changed := WalkExpr(v, expr.Expr)
		if changed {
			ret = expr.CopyNode()
			ret.Expr = e
		}
	}
	for i, w := range expr.Whens {
		cond, changedC := WalkExpr(v, w.Cond)
		val, changedV := WalkExpr(v, w.Val)
		if changedC || changedV {
			if ret == expr {
				ret = expr.CopyNode()
			}
			ret.Whens[i].Cond = cond
			ret.Whens[i].Val = val
		}
	}
	if expr.Else != nil {
		e, changed := WalkExpr(v, expr.Else)
		if changed {
			if ret == expr {
				ret = expr.CopyNode()
			}
			ret.Else = e
		}
	}
	return ret
}

// Walk implements the Expr interface.
func (expr *CastExpr) Walk(v Visitor) Expr {
	e, changed := WalkExpr(v, expr.Expr)
	if changed {
		exprCopy := *expr
		exprCopy.Expr = e
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *AnnotateTypeExpr) Walk(v Visitor) Expr {
	e, changed := WalkExpr(v, expr.Expr)
	if changed {
		exprCopy := *expr
		exprCopy.Expr = e
		return &exprCopy
	}
	return expr
}

// CopyNode makes a copy of this Expr without recursing in any child Exprs.
func (expr *CoalesceExpr) CopyNode() *CoalesceExpr {
	exprCopy := *expr
	exprCopy.Exprs = Exprs(append([]Expr(nil), exprCopy.Exprs...))
	return &exprCopy
}

// Walk implements the Expr interface.
func (expr *CoalesceExpr) Walk(v Visitor) Expr {
	ret := expr
	for i := range expr.Exprs {
		e, changed := WalkExpr(v, expr.Exprs[i])
		if changed {
			if ret == expr {
				ret = expr.CopyNode()
			}
			ret.Exprs[i] = e
		}
	}
	return ret
}

// Walk implements the Expr interface.
func (expr *ComparisonExpr) Walk(v Visitor) Expr {
	left, changedL := WalkExpr(v, expr.Left)
	right, changedR := WalkExpr(v, expr.Right)
	if changedL || changedR {
		exprCopy := *expr
		exprCopy.Left = left
		exprCopy.Right = right
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *ExistsExpr) Walk(v Visitor) Expr {
	e, changed := WalkExpr(v, expr.Subquery)
	if changed {
		exprCopy := *expr
		exprCopy.Subquery = e
		return &exprCopy
	}
	return expr
}

// CopyNode makes a copy of this Expr without recursing in any child Exprs.
func (expr *FuncExpr) CopyNode() *FuncExpr {
	exprCopy := *expr
	exprCopy.Exprs = Exprs(append([]Expr(nil), exprCopy.Exprs...))
	return &exprCopy
}

// Walk implements the Expr interface.
func (expr *FuncExpr) Walk(v Visitor) Expr {
	ret := expr
	for i := range expr.Exprs {
		e, changed := WalkExpr(v, expr.Exprs[i])
		if changed {
			if ret == expr {
				ret = expr.CopyNode()
			}
			ret.Exprs[i] = e
		}
	}
	return ret
}

// Walk implements the Expr interface.
func (expr *IfExpr) Walk(v Visitor) Expr {
	c, changedC := WalkExpr(v, expr.Cond)
	t, changedT := WalkExpr(v, expr.True)
	e, changedE := WalkExpr(v, expr.Else)
	if changedC || changedT || changedE {
		exprCopy := *expr
		exprCopy.Cond = c
		exprCopy.True = t
		exprCopy.Else = e
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *IsOfTypeExpr) Walk(v Visitor) Expr {
	e, changed := WalkExpr(v, expr.Expr)
	if changed {
		exprCopy := *expr
		exprCopy.Expr = e
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *NotExpr) Walk(v Visitor) Expr {
	e, changed := WalkExpr(v, expr.Expr)
	if changed {
		exprCopy := *expr
		exprCopy.Expr = e
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *NullIfExpr) Walk(v Visitor) Expr {
	e1, changed1 := WalkExpr(v, expr.Expr1)
	e2, changed2 := WalkExpr(v, expr.Expr2)
	if changed1 || changed2 {
		exprCopy := *expr
		exprCopy.Expr1 = e1
		exprCopy.Expr2 = e2
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *OrExpr) Walk(v Visitor) Expr {
	left, changedL := WalkExpr(v, expr.Left)
	right, changedR := WalkExpr(v, expr.Right)
	if changedL || changedR {
		exprCopy := *expr
		exprCopy.Left = left
		exprCopy.Right = right
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *ParenExpr) Walk(v Visitor) Expr {
	e, changed := WalkExpr(v, expr.Expr)
	if changed {
		exprCopy := *expr
		exprCopy.Expr = e
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *RangeCond) Walk(v Visitor) Expr {
	l, changedL := WalkExpr(v, expr.Left)
	f, changedF := WalkExpr(v, expr.From)
	t, changedT := WalkExpr(v, expr.To)
	if changedL || changedF || changedT {
		exprCopy := *expr
		exprCopy.Left = l
		exprCopy.From = f
		exprCopy.To = t
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *Subquery) Walk(v Visitor) Expr {
	sel, changed := WalkStmt(v, expr.Select)
	if changed {
		return &Subquery{sel.(SelectStatement)}
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *UnaryExpr) Walk(v Visitor) Expr {
	e, changed := WalkExpr(v, expr.Expr)
	if changed {
		exprCopy := *expr
		exprCopy.Expr = e
		return &exprCopy
	}
	return expr
}

func walkExprSlice(v Visitor, slice []Expr) ([]Expr, bool) {
	copied := false
	for i := range slice {
		e, changed := WalkExpr(v, slice[i])
		if changed {
			if !copied {
				slice = append([]Expr(nil), slice...)
				copied = true
			}
			slice[i] = e
		}
	}
	return slice, copied
}

// Walk implements the Expr interface.
func (expr *Array) Walk(v Visitor) Expr {
	exprs, changed := walkExprSlice(v, expr.Exprs)
	if changed {
		return &Array{exprs}
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *Tuple) Walk(v Visitor) Expr {
	exprs, changed := walkExprSlice(v, expr.Exprs)
	if changed {
		exprCopy := *expr
		exprCopy.Exprs = exprs
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *QualifiedName) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr DefaultVal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *NumVal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *StrVal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr Placeholder) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DBool) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DBytes) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DDate) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DFloat) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DDecimal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DInt) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DInterval) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr dNull) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DString) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DTimestamp) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DTimestampTZ) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DTuple) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DPlaceholder) Walk(_ Visitor) Expr { return expr }

// WalkExpr traverses the nodes in an expression.
func WalkExpr(v Visitor, expr Expr) (newExpr Expr, changed bool) {
	recurse, newExpr := v.VisitPre(expr)

	if recurse {
		newExpr = newExpr.Walk(v)
		newExpr = v.VisitPost(newExpr)
	}

	// We cannot use == because some Expr implementations are not comparable (e.g. DTuple)
	return newExpr, (reflect.ValueOf(expr) != reflect.ValueOf(newExpr))
}

// WalkExprConst is a variant of WalkExpr for visitors that do not modify the expression.
func WalkExprConst(v Visitor, expr Expr) {
	WalkExpr(v, expr)
	// TODO(radu): we should verify that WalkExpr returns changed == false. Unfortunately that
	// is not the case today because walking through non-pointer implementations of Expr (like
	// DBool, DTuple) causes new nodes to be created. We should make all Expr implementations be
	// pointers (which will also remove the need for using reflect.ValueOf above).
}

// WalkableStmt is implemented by statements that can appear inside an expression (selects) or
// we want to start a walk from (using WalkStmt).
type WalkableStmt interface {
	Statement
	WalkStmt(Visitor) Statement
}

// CopyNode makes a copy of this Expr without recursing in any child Exprs.
func (stmt *Delete) CopyNode() *Delete {
	stmtCopy := *stmt
	if stmt.Where != nil {
		wCopy := *stmt.Where
		stmtCopy.Where = &wCopy
	}
	stmtCopy.Returning = ReturningExprs(append([]SelectExpr(nil), stmt.Returning...))
	return &stmtCopy
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *Delete) WalkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.Where != nil {
		e, changed := WalkExpr(v, stmt.Where.Expr)
		if changed {
			ret = stmt.CopyNode()
			ret.Where.Expr = e
		}
	}
	for i, expr := range stmt.Returning {
		e, changed := WalkExpr(v, expr.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.Returning[i].Expr = e
		}
	}
	return ret
}

// CopyNode makes a copy of this Expr without recursing in any child Exprs.
func (stmt *Explain) CopyNode() *Explain {
	stmtCopy := *stmt
	stmtCopy.Options = append([]string(nil), stmt.Options...)
	return &stmtCopy
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *Explain) WalkStmt(v Visitor) Statement {
	s, changed := WalkStmt(v, stmt.Statement)
	if changed {
		stmt = stmt.CopyNode()
		stmt.Statement = s
	}
	return stmt
}

// CopyNode makes a copy of this Expr without recursing in any child Exprs.
func (stmt *Insert) CopyNode() *Insert {
	stmtCopy := *stmt
	stmtCopy.Columns = copyQualifiedNames(stmt.Columns)
	stmtCopy.Returning = ReturningExprs(append([]SelectExpr(nil), stmt.Returning...))
	return &stmtCopy
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *Insert) WalkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.Rows != nil {
		rows, changed := WalkStmt(v, stmt.Rows)
		if changed {
			ret = stmt.CopyNode()
			ret.Rows = rows.(*Select)
		}
	}
	for i, expr := range stmt.Returning {
		e, changed := WalkExpr(v, expr.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.Returning[i].Expr = e
		}
	}
	// TODO(dan): Walk OnConflict once the ON CONFLICT DO UPDATE form of upsert is
	// implemented.
	return ret
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *ParenSelect) WalkStmt(v Visitor) Statement {
	sel, changed := WalkStmt(v, stmt.Select)
	if changed {
		return &ParenSelect{sel.(*Select)}
	}
	return stmt
}

// CopyNode makes a copy of this Expr without recursing in any child Exprs.
func (stmt *Select) CopyNode() *Select {
	stmtCopy := *stmt
	stmtCopy.OrderBy = make([]*Order, len(stmt.OrderBy))
	for i, o := range stmt.OrderBy {
		oCopy := *o
		stmtCopy.OrderBy[i] = &oCopy
	}
	if stmt.Limit != nil {
		lCopy := *stmt.Limit
		stmtCopy.Limit = &lCopy
	}
	return &stmtCopy
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *Select) WalkStmt(v Visitor) Statement {
	ret := stmt
	sel, changed := WalkStmt(v, stmt.Select)
	if changed {
		ret = stmt.CopyNode()
		ret.Select = sel.(SelectStatement)
	}
	for i, expr := range stmt.OrderBy {
		e, changed := WalkExpr(v, expr.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.OrderBy[i].Expr = e
		}
	}
	if stmt.Limit != nil {
		if stmt.Limit.Offset != nil {
			e, changed := WalkExpr(v, stmt.Limit.Offset)
			if changed {
				if ret == stmt {
					ret = stmt.CopyNode()
				}
				ret.Limit.Offset = e
			}
		}
		if stmt.Limit.Count != nil {
			e, changed := WalkExpr(v, stmt.Limit.Count)
			if changed {
				if ret == stmt {
					ret = stmt.CopyNode()
				}
				ret.Limit.Count = e
			}
		}
	}
	return ret
}

// CopyNode makes a copy of this Expr without recursing in any child Exprs.
func (stmt *SelectClause) CopyNode() *SelectClause {
	stmtCopy := *stmt
	stmtCopy.Exprs = SelectExprs(append([]SelectExpr(nil), stmt.Exprs...))
	stmtCopy.From = &From{
		Tables: TableExprs(append([]TableExpr(nil), stmt.From.Tables...)),
		AsOf:   stmt.From.AsOf,
	}
	if stmt.Where != nil {
		wCopy := *stmt.Where
		stmtCopy.Where = &wCopy
	}
	stmtCopy.GroupBy = GroupBy(append([]Expr(nil), stmt.GroupBy...))
	if stmt.Having != nil {
		hCopy := *stmt.Having
		stmtCopy.Having = &hCopy
	}
	return &stmtCopy
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *SelectClause) WalkStmt(v Visitor) Statement {
	ret := stmt

	for i, expr := range stmt.Exprs {
		e, changed := WalkExpr(v, expr.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.Exprs[i].Expr = e
		}
	}

	if stmt.From != nil && stmt.From.AsOf.Expr != nil {
		e, changed := WalkExpr(v, stmt.From.AsOf.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.From.AsOf.Expr = e
		}
	}

	if stmt.Where != nil {
		e, changed := WalkExpr(v, stmt.Where.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.Where.Expr = e
		}
	}

	for i, expr := range stmt.GroupBy {
		e, changed := WalkExpr(v, expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.GroupBy[i] = e
		}
	}

	if stmt.Having != nil {
		e, changed := WalkExpr(v, stmt.Having.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.Having.Expr = e
		}
	}
	return ret
}

// CopyNode makes a copy of this Expr without recursing in any child Exprs.
func (stmt *Set) CopyNode() *Set {
	stmtCopy := *stmt
	stmtCopy.Values = Exprs(append([]Expr(nil), stmt.Values...))
	return &stmtCopy
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *Set) WalkStmt(v Visitor) Statement {
	ret := stmt
	for i, expr := range stmt.Values {
		e, changed := WalkExpr(v, expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.Values[i] = e
		}
	}
	return ret
}

// CopyNode makes a copy of this Expr without recursing in any child Exprs.
func (stmt *Update) CopyNode() *Update {
	stmtCopy := *stmt
	stmtCopy.Exprs = UpdateExprs(make([]*UpdateExpr, len(stmt.Exprs)))
	for i, e := range stmt.Exprs {
		eCopy := *e
		eCopy.Names = copyQualifiedNames(e.Names)
		stmtCopy.Exprs[i] = &eCopy
	}
	if stmt.Where != nil {
		wCopy := *stmt.Where
		stmtCopy.Where = &wCopy
	}
	stmtCopy.Returning = ReturningExprs(append([]SelectExpr(nil), stmt.Returning...))
	return &stmtCopy
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *Update) WalkStmt(v Visitor) Statement {
	ret := stmt
	for i, expr := range stmt.Exprs {
		e, changed := WalkExpr(v, expr.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.Exprs[i].Expr = e
		}
	}

	if stmt.Where != nil {
		e, changed := WalkExpr(v, stmt.Where.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.Where.Expr = e
		}
	}

	for i, expr := range stmt.Returning {
		e, changed := WalkExpr(v, expr.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.Returning[i].Expr = e
		}
	}
	return ret
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *ValuesClause) WalkStmt(v Visitor) Statement {
	ret := stmt
	for i, tuple := range stmt.Tuples {
		t, changed := WalkExpr(v, tuple)
		if changed {
			if ret == stmt {
				ret = &ValuesClause{append([]*Tuple(nil), stmt.Tuples...)}
			}
			ret.Tuples[i] = t.(*Tuple)
		}
	}
	return ret
}

var _ WalkableStmt = &Delete{}
var _ WalkableStmt = &Explain{}
var _ WalkableStmt = &Insert{}
var _ WalkableStmt = &ParenSelect{}
var _ WalkableStmt = &Select{}
var _ WalkableStmt = &SelectClause{}
var _ WalkableStmt = &Set{}
var _ WalkableStmt = &Update{}
var _ WalkableStmt = &ValuesClause{}

// WalkStmt walks the entire parsed stmt calling WalkExpr on each
// expression, and replacing each expression with the one returned
// by WalkExpr.
func WalkStmt(v Visitor, stmt Statement) (newStmt Statement, changed bool) {
	walkable, ok := stmt.(WalkableStmt)
	if !ok {
		return stmt, false
	}
	newStmt = walkable.WalkStmt(v)
	return newStmt, (stmt != newStmt)
}

type simpleVisitor struct {
	fn  SimpleVisitFn
	err error
}

var _ Visitor = &simpleVisitor{}

func (v *simpleVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if v.err != nil {
		return false, expr
	}
	v.err, recurse, newExpr = v.fn(expr)
	if v.err != nil {
		return false, expr
	}
	return recurse, newExpr
}

func (*simpleVisitor) VisitPost(expr Expr) Expr { return expr }

// SimpleVisitFn is a function that is run for every node in the VisitPre stage;
// see SimpleVisit.
type SimpleVisitFn func(expr Expr) (err error, recurse bool, newExpr Expr)

// SimpleVisit is a convenience wrapper for visitors that only have VisitPre
// code and don't return any results except an error. The given function is
// called in VisitPre for every node. The visitor stops as soon as an error is
// returned.
func SimpleVisit(expr Expr, preFn SimpleVisitFn) (Expr, error) {
	v := simpleVisitor{fn: preFn}
	newExpr, _ := WalkExpr(&v, expr)
	if v.err != nil {
		return nil, v.err
	}
	return newExpr, nil
}
