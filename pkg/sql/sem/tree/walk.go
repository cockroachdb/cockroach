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

package tree

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

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
func (expr *AnnotateTypeExpr) Walk(v Visitor) Expr {
	e, changed := WalkExpr(v, expr.Expr)
	if changed {
		exprCopy := *expr
		exprCopy.Expr = e
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
func (expr *CollateExpr) Walk(v Visitor) Expr {
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
	exprCopy.Exprs = append(Exprs(nil), exprCopy.Exprs...)
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

// CopyNode makes a copy of this Expr without recursing in any child Exprs.
func (expr *FuncExpr) CopyNode() *FuncExpr {
	exprCopy := *expr
	if expr.WindowDef != nil {
		windowDefCopy := *expr.WindowDef
		exprCopy.WindowDef = &windowDefCopy
	}
	exprCopy.Exprs = append(Exprs(nil), exprCopy.Exprs...)
	if windowDef := exprCopy.WindowDef; windowDef != nil {
		windowDef.Partitions = append(Exprs(nil), windowDef.Partitions...)
		if len(windowDef.OrderBy) > 0 {
			newOrderBy := make(OrderBy, len(windowDef.OrderBy))
			for i, o := range windowDef.OrderBy {
				newOrderBy[i] = &Order{OrderType: o.OrderType, Expr: o.Expr, Direction: o.Direction}
			}
			windowDef.OrderBy = newOrderBy
		}
	}
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
	if expr.WindowDef != nil {
		for i := range expr.WindowDef.Partitions {
			e, changed := WalkExpr(v, expr.WindowDef.Partitions[i])
			if changed {
				if ret == expr {
					ret = expr.CopyNode()
				}
				ret.WindowDef.Partitions[i] = e
			}
		}
		for i := range expr.WindowDef.OrderBy {
			if expr.WindowDef.OrderBy[i].OrderType != OrderByColumn {
				continue
			}
			e, changed := WalkExpr(v, expr.WindowDef.OrderBy[i].Expr)
			if changed {
				if ret == expr {
					ret = expr.CopyNode()
				}
				ret.WindowDef.OrderBy[i].Expr = e
			}
		}
	}
	if expr.Filter != nil {
		e, changed := WalkExpr(v, expr.Filter)
		if changed {
			if ret == expr {
				ret = expr.CopyNode()
			}
			ret.Filter = e
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

// CopyNode makes a copy of this Expr without recursing in any child Exprs.
func (expr *IndirectionExpr) CopyNode() *IndirectionExpr {
	exprCopy := *expr
	exprCopy.Indirection = append(ArraySubscripts(nil), exprCopy.Indirection...)
	for i, t := range exprCopy.Indirection {
		subscriptCopy := *t
		exprCopy.Indirection[i] = &subscriptCopy
	}
	return &exprCopy
}

// Walk implements the Expr interface.
func (expr *IndirectionExpr) Walk(v Visitor) Expr {
	ret := expr

	e, changed := WalkExpr(v, expr.Expr)
	if changed {
		if ret == expr {
			ret = expr.CopyNode()
		}
		ret.Expr = e
	}

	for i, t := range expr.Indirection {
		if t.Begin != nil {
			e, changed := WalkExpr(v, t.Begin)
			if changed {
				if ret == expr {
					ret = expr.CopyNode()
				}
				ret.Indirection[i].Begin = e
			}
		}
		if t.End != nil {
			e, changed := WalkExpr(v, t.End)
			if changed {
				if ret == expr {
					ret = expr.CopyNode()
				}
				ret.Indirection[i].End = e
			}
		}
	}

	return ret
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
		exprCopy := *expr
		exprCopy.Select = sel.(SelectStatement)
		return &exprCopy
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

func walkKVOptions(v Visitor, opts KVOptions) (KVOptions, bool) {
	copied := false
	for i := range opts {
		if opts[i].Value == nil {
			continue
		}
		e, changed := WalkExpr(v, opts[i].Value)
		if changed {
			if !copied {
				opts = append(KVOptions(nil), opts...)
				copied = true
			}
			opts[i].Value = e
		}
	}
	return opts, copied
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
func (expr *Array) Walk(v Visitor) Expr {
	if exprs, changed := walkExprSlice(v, expr.Exprs); changed {
		exprCopy := *expr
		exprCopy.Exprs = exprs
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *ArrayFlatten) Walk(v Visitor) Expr {
	if sq, changed := WalkExpr(v, expr.Subquery); changed {
		exprCopy := *expr
		exprCopy.Subquery = sq
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr UnqualifiedStar) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *UnresolvedName) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *AllColumnsSelector) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *ColumnItem) Walk(_ Visitor) Expr {
	// TODO(knz): When ARRAY is supported, this must be extended
	// to recurse into the index expressions of the ColumnItems' Selector.
	return expr
}

// Walk implements the Expr interface.
func (expr DefaultVal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr MaxVal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr MinVal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *NumVal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *StrVal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *Placeholder) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DBool) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DBytes) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DDate) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DTime) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DFloat) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DDecimal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DInt) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DInterval) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DJSON) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DUuid) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DIPAddr) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr dNull) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DString) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DCollatedString) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DTimestamp) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DTimestampTZ) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DTuple) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DArray) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DTable) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DOid) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DOidWrapper) Walk(_ Visitor) Expr { return expr }

// WalkExpr traverses the nodes in an expression.
//
// NOTE: Do not count on the WalkStmt/WalkExpr machinery to visit all
// expressions contained in a query. Only a sub-set of all expressions are
// found by WalkStmt and subsequently traversed. See the comment below on
// WalkStmt for details.
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

func walkReturningClause(v Visitor, clause ReturningClause) (ReturningClause, bool) {
	switch t := clause.(type) {
	case *ReturningExprs:
		ret := t
		for i, expr := range *t {
			e, changed := WalkExpr(v, expr.Expr)
			if changed {
				if ret == t {
					ret = t.CopyNode()
				}
				(*ret)[i].Expr = e
			}
		}
		return ret, (ret != t)
	case *ReturningNothing, *NoReturningClause:
		return t, false
	default:
		panic(pgerror.NewErrorf(pgerror.CodeInternalError, "unexpected ReturningClause type: %T", t))
	}
}

// CopyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Backup) CopyNode() *Backup {
	stmtCopy := *stmt
	stmtCopy.IncrementalFrom = append(Exprs(nil), stmt.IncrementalFrom...)
	stmtCopy.Options = append(KVOptions(nil), stmt.Options...)
	return &stmtCopy
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *Backup) WalkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.AsOf.Expr != nil {
		e, changed := WalkExpr(v, stmt.AsOf.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.AsOf.Expr = e
		}
	}
	{
		e, changed := WalkExpr(v, stmt.To)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.To = e
		}
	}
	for i, expr := range stmt.IncrementalFrom {
		e, changed := WalkExpr(v, expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.IncrementalFrom[i] = e
		}
	}
	{
		opts, changed := walkKVOptions(v, stmt.Options)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.Options = opts
		}
	}
	return ret
}

// CopyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Delete) CopyNode() *Delete {
	stmtCopy := *stmt
	if stmt.Where != nil {
		wCopy := *stmt.Where
		stmtCopy.Where = &wCopy
	}
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
	returning, changed := walkReturningClause(v, stmt.Returning)
	if changed {
		if ret == stmt {
			ret = stmt.CopyNode()
		}
		ret.Returning = returning
	}
	return ret
}

// CopyNode makes a copy of this Statement without recursing in any child Statements.
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

// CopyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Insert) CopyNode() *Insert {
	stmtCopy := *stmt
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
	returning, changed := walkReturningClause(v, stmt.Returning)
	if changed {
		if ret == stmt {
			ret = stmt.CopyNode()
		}
		ret.Returning = returning
	}
	// TODO(dan): Walk OnConflict once the ON CONFLICT DO UPDATE form of upsert is
	// implemented.
	return ret
}

// CopyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *CreateTable) CopyNode() *CreateTable {
	stmtCopy := *stmt
	return &stmtCopy
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *CreateTable) WalkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.AsSource != nil {
		rows, changed := WalkStmt(v, stmt.AsSource)
		if changed {
			ret = stmt.CopyNode()
			ret.AsSource = rows.(*Select)
		}
	}
	return ret
}

// CopyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Import) CopyNode() *Import {
	stmtCopy := *stmt
	stmtCopy.Files = append(Exprs(nil), stmt.Files...)
	stmtCopy.Options = append(KVOptions(nil), stmt.Options...)
	return &stmtCopy
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *Import) WalkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.CreateFile != nil {
		e, changed := WalkExpr(v, stmt.CreateFile)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.CreateFile = e
		}
	}
	for i, expr := range stmt.Files {
		e, changed := WalkExpr(v, expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.Files[i] = e
		}
	}
	{
		opts, changed := walkKVOptions(v, stmt.Options)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.Options = opts
		}
	}
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

// CopyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Restore) CopyNode() *Restore {
	stmtCopy := *stmt
	stmtCopy.From = append(Exprs(nil), stmt.From...)
	stmtCopy.Options = append(KVOptions(nil), stmt.Options...)
	return &stmtCopy
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *Restore) WalkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.AsOf.Expr != nil {
		e, changed := WalkExpr(v, stmt.AsOf.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.AsOf.Expr = e
		}
	}
	for i, expr := range stmt.From {
		e, changed := WalkExpr(v, expr)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.From[i] = e
		}
	}
	{
		opts, changed := walkKVOptions(v, stmt.Options)
		if changed {
			if ret == stmt {
				ret = stmt.CopyNode()
			}
			ret.Options = opts
		}
	}
	return ret
}

// CopyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *ReturningExprs) CopyNode() *ReturningExprs {
	stmtCopy := append(ReturningExprs(nil), *stmt...)
	return &stmtCopy
}

func walkOrderBy(v Visitor, order OrderBy) (OrderBy, bool) {
	copied := false
	for i := range order {
		if order[i].OrderType != OrderByColumn {
			continue
		}
		e, changed := WalkExpr(v, order[i].Expr)
		if changed {
			if !copied {
				order = append(OrderBy(nil), order...)
				copied = true
			}
			order[i].Expr = e
		}
	}
	return order, copied
}

// CopyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Select) CopyNode() *Select {
	stmtCopy := *stmt
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
	order, changed := walkOrderBy(v, stmt.OrderBy)
	if changed {
		if ret == stmt {
			ret = stmt.CopyNode()
		}
		ret.OrderBy = order
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

// CopyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *SelectClause) CopyNode() *SelectClause {
	stmtCopy := *stmt
	stmtCopy.Exprs = append(SelectExprs(nil), stmt.Exprs...)
	stmtCopy.From = &From{
		Tables: append(TableExprs(nil), stmt.From.Tables...),
		AsOf:   stmt.From.AsOf,
	}
	if stmt.Where != nil {
		wCopy := *stmt.Where
		stmtCopy.Where = &wCopy
	}
	stmtCopy.GroupBy = append(GroupBy(nil), stmt.GroupBy...)
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

	for i, windowDef := range stmt.Window {
		if windowDef.Partitions != nil {
			exprs, changed := walkExprSlice(v, windowDef.Partitions)
			if changed {
				if ret == stmt {
					ret = stmt.CopyNode()
				}
				ret.Window[i].Partitions = exprs
			}
		}
		if windowDef.OrderBy != nil {
			order, changed := walkOrderBy(v, windowDef.OrderBy)
			if changed {
				if ret == stmt {
					ret = stmt.CopyNode()
				}
				ret.Window[i].OrderBy = order
			}
		}
	}
	return ret
}

// CopyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *SetVar) CopyNode() *SetVar {
	stmtCopy := *stmt
	stmtCopy.Values = append(Exprs(nil), stmt.Values...)
	return &stmtCopy
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *SetVar) WalkStmt(v Visitor) Statement {
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

// CopyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *SetClusterSetting) CopyNode() *SetClusterSetting {
	stmtCopy := *stmt
	return &stmtCopy
}

// WalkStmt is part of the WalkableStmt interface.
func (stmt *SetClusterSetting) WalkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.Value != nil {
		e, changed := WalkExpr(v, stmt.Value)
		if changed {
			ret = stmt.CopyNode()
			ret.Value = e
		}
	}
	return ret
}

// CopyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Update) CopyNode() *Update {
	stmtCopy := *stmt
	stmtCopy.Exprs = make(UpdateExprs, len(stmt.Exprs))
	for i, e := range stmt.Exprs {
		eCopy := *e
		stmtCopy.Exprs[i] = &eCopy
	}
	if stmt.Where != nil {
		wCopy := *stmt.Where
		stmtCopy.Where = &wCopy
	}
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

	returning, changed := walkReturningClause(v, stmt.Returning)
	if changed {
		if ret == stmt {
			ret = stmt.CopyNode()
		}
		ret.Returning = returning
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

var _ WalkableStmt = &CreateTable{}
var _ WalkableStmt = &Backup{}
var _ WalkableStmt = &Delete{}
var _ WalkableStmt = &Explain{}
var _ WalkableStmt = &Insert{}
var _ WalkableStmt = &Import{}
var _ WalkableStmt = &ParenSelect{}
var _ WalkableStmt = &Restore{}
var _ WalkableStmt = &Select{}
var _ WalkableStmt = &SelectClause{}
var _ WalkableStmt = &SetClusterSetting{}
var _ WalkableStmt = &SetVar{}
var _ WalkableStmt = &Update{}
var _ WalkableStmt = &ValuesClause{}

// WalkStmt walks the entire parsed stmt calling WalkExpr on each
// expression, and replacing each expression with the one returned
// by WalkExpr.
//
// NOTE: Beware that WalkStmt does not necessarily traverse all parts of a
// statement by itself. For example, it will not walk into Subquery nodes
// within a FROM clause or into a JoinCond. Walk's logic is pretty
// interdependent with the logic for constructing a query plan.
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

type debugVisitor struct {
	buf   bytes.Buffer
	level int
}

var _ Visitor = &debugVisitor{}

func (v *debugVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	v.level++
	fmt.Fprintf(&v.buf, "%*s", 2*v.level, " ")
	str := fmt.Sprintf("%#v\n", expr)
	// Remove "parser." to make the string more compact.
	str = strings.Replace(str, "parser.", "", -1)
	v.buf.WriteString(str)
	return true, expr
}

func (v *debugVisitor) VisitPost(expr Expr) Expr {
	v.level--
	return expr
}

// ExprDebugString generates a multi-line debug string with one node per line in
// Go format.
func ExprDebugString(expr Expr) string {
	v := debugVisitor{}
	WalkExprConst(&v, expr)
	return v.buf.String()
}

// StmtDebugString generates multi-line debug strings in Go format for the
// expressions that are part of the given statement.
func StmtDebugString(stmt Statement) string {
	v := debugVisitor{}
	WalkStmt(&v, stmt)
	return v.buf.String()
}

// Silence any warnings if these functions are not used.
var _ = ExprDebugString
var _ = StmtDebugString
