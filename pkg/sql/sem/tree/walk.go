// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/errors"
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

// copyNode makes a copy of this Expr without recursing in any child Exprs.
func (expr *CaseExpr) copyNode() *CaseExpr {
	exprCopy := *expr
	// Copy the Whens slice. Amortize into 1 allocation.
	whens := make([]When, len(expr.Whens))
	exprCopy.Whens = make([]*When, len(expr.Whens))
	for i, w := range expr.Whens {
		whens[i] = *w
		exprCopy.Whens[i] = &whens[i]
	}
	return &exprCopy
}

// Walk implements the Expr interface.
func (expr *CaseExpr) Walk(v Visitor) Expr {
	ret := expr

	if expr.Expr != nil {
		e, changed := WalkExpr(v, expr.Expr)
		if changed {
			ret = expr.copyNode()
			ret.Expr = e
		}
	}
	for i, w := range expr.Whens {
		cond, changedC := WalkExpr(v, w.Cond)
		val, changedV := WalkExpr(v, w.Val)
		if changedC || changedV {
			if ret == expr {
				ret = expr.copyNode()
			}
			ret.Whens[i].Cond = cond
			ret.Whens[i].Val = val
		}
	}
	if expr.Else != nil {
		e, changed := WalkExpr(v, expr.Else)
		if changed {
			if ret == expr {
				ret = expr.copyNode()
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

// Walk implements the Expr interface.
func (expr *ColumnAccessExpr) Walk(v Visitor) Expr {
	e, changed := WalkExpr(v, expr.Expr)
	if changed {
		exprCopy := *expr
		exprCopy.Expr = e
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *TupleStar) Walk(v Visitor) Expr {
	e, changed := WalkExpr(v, expr.Expr)
	if changed {
		exprCopy := *expr
		exprCopy.Expr = e
		return &exprCopy
	}
	return expr
}

// copyNode makes a copy of this Expr without recursing in any child Exprs.
func (expr *CoalesceExpr) copyNode() *CoalesceExpr {
	exprCopy := *expr
	return &exprCopy
}

// Walk implements the Expr interface.
func (expr *CoalesceExpr) Walk(v Visitor) Expr {
	ret := expr
	exprs, changed := walkExprSlice(v, expr.Exprs)
	if changed {
		if ret == expr {
			ret = expr.copyNode()
		}
		ret.Exprs = exprs
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
		// TODO(ajwerner): Should this be updating the memoized op? It seems like
		// it could need to change in some cases, but it also seems painful to know
		// which ones.
		return &exprCopy
	}
	return expr
}

// copyNode makes a copy of this Expr without recursing in any child Exprs.
func (expr *FuncExpr) copyNode() *FuncExpr {
	exprCopy := *expr
	exprCopy.Exprs = append(Exprs(nil), exprCopy.Exprs...)
	return &exprCopy
}

// copyNode makes a copy of this WindowFrame without recursing.
func (node *WindowFrame) copyNode() *WindowFrame {
	nodeCopy := *node
	return &nodeCopy
}

func walkWindowFrame(v Visitor, frame *WindowFrame) (*WindowFrame, bool) {
	ret := frame
	if frame.Bounds.StartBound != nil {
		b, changed := walkWindowFrameBound(v, frame.Bounds.StartBound)
		if changed {
			if ret == frame {
				ret = frame.copyNode()
			}
			ret.Bounds.StartBound = b
		}
	}
	if frame.Bounds.EndBound != nil {
		b, changed := walkWindowFrameBound(v, frame.Bounds.EndBound)
		if changed {
			if ret == frame {
				ret = frame.copyNode()
			}
			ret.Bounds.EndBound = b
		}
	}
	return ret, ret != frame
}

// copyNode makes a copy of this WindowFrameBound without recursing.
func (node *WindowFrameBound) copyNode() *WindowFrameBound {
	nodeCopy := *node
	return &nodeCopy
}

func walkWindowFrameBound(v Visitor, bound *WindowFrameBound) (*WindowFrameBound, bool) {
	ret := bound
	if bound.HasOffset() {
		e, changed := WalkExpr(v, bound.OffsetExpr)
		if changed {
			if ret == bound {
				ret = bound.copyNode()
			}
			ret.OffsetExpr = e
		}
	}
	return ret, ret != bound
}

// copyNode makes a copy of this WindowDef without recursing.
func (node *WindowDef) copyNode() *WindowDef {
	nodeCopy := *node
	return &nodeCopy
}

func walkWindowDef(v Visitor, windowDef *WindowDef) (*WindowDef, bool) {
	ret := windowDef
	if len(windowDef.Partitions) > 0 {
		exprs, changed := walkExprSlice(v, windowDef.Partitions)
		if changed {
			if ret == windowDef {
				ret = windowDef.copyNode()
			}
			ret.Partitions = exprs
		}
	}
	if len(windowDef.OrderBy) > 0 {
		order, changed := walkOrderBy(v, windowDef.OrderBy)
		if changed {
			if ret == windowDef {
				ret = windowDef.copyNode()
			}
			ret.OrderBy = order
		}
	}
	if windowDef.Frame != nil {
		frame, changed := walkWindowFrame(v, windowDef.Frame)
		if changed {
			if ret == windowDef {
				ret = windowDef.copyNode()
			}
			ret.Frame = frame
		}
	}

	return ret, ret != windowDef
}

// Walk implements the Expr interface.
func (expr *FuncExpr) Walk(v Visitor) Expr {
	ret := expr
	exprs, changed := walkExprSlice(v, expr.Exprs)
	if changed {
		if ret == expr {
			ret = expr.copyNode()
		}
		ret.Exprs = exprs
	}
	if expr.Filter != nil {
		e, changed := WalkExpr(v, expr.Filter)
		if changed {
			if ret == expr {
				ret = expr.copyNode()
			}
			ret.Filter = e
		}
	}

	if expr.OrderBy != nil {
		order, changed := walkOrderBy(v, expr.OrderBy)
		if changed {
			if ret == expr {
				ret = expr.copyNode()
			}
			ret.OrderBy = order
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
func (expr *IfErrExpr) Walk(v Visitor) Expr {
	c, changedC := WalkExpr(v, expr.Cond)
	t := expr.ErrCode
	changedEC := false
	if t != nil {
		t, changedEC = WalkExpr(v, expr.ErrCode)
	}
	e := expr.Else
	changedE := false
	if e != nil {
		e, changedE = WalkExpr(v, expr.Else)
	}
	if changedC || changedEC || changedE {
		exprCopy := *expr
		exprCopy.Cond = c
		exprCopy.ErrCode = t
		exprCopy.Else = e
		return &exprCopy
	}
	return expr
}

// copyNode makes a copy of this Expr without recursing in any child Exprs.
func (expr *IndirectionExpr) copyNode() *IndirectionExpr {
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
			ret = expr.copyNode()
		}
		ret.Expr = e
	}

	for i, t := range expr.Indirection {
		if t.Begin != nil {
			e, changed := WalkExpr(v, t.Begin)
			if changed {
				if ret == expr {
					ret = expr.copyNode()
				}
				ret.Indirection[i].Begin = e
			}
		}
		if t.End != nil {
			e, changed := WalkExpr(v, t.End)
			if changed {
				if ret == expr {
					ret = expr.copyNode()
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
func (expr *IsNullExpr) Walk(v Visitor) Expr {
	e, changed := WalkExpr(v, expr.Expr)
	if changed {
		exprCopy := *expr
		exprCopy.Expr = e
		return &exprCopy
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *IsNotNullExpr) Walk(v Visitor) Expr {
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

// WalkTableExpr implements the TableExpr interface.
func (expr *Subquery) WalkTableExpr(v Visitor) TableExpr {
	sel, changed := WalkStmt(v, expr.Select)
	if changed {
		exprCopy := *expr
		exprCopy.Select = sel.(SelectStatement)
		return &exprCopy
	}
	return expr
}

// WalkTableExpr implements the TableExpr interface.
func (expr *AliasedTableExpr) WalkTableExpr(v Visitor) TableExpr {
	newExpr, changed := walkTableExpr(v, expr.Expr)
	if changed {
		exprCopy := *expr
		exprCopy.Expr = newExpr
		return &exprCopy
	}
	return expr
}

// WalkTableExpr implements the TableExpr interface.
func (expr *ParenTableExpr) WalkTableExpr(v Visitor) TableExpr {
	newExpr, changed := walkTableExpr(v, expr.Expr)
	if changed {
		exprCopy := *expr
		exprCopy.Expr = newExpr
		return &exprCopy
	}
	return expr
}

// WalkTableExpr implements the TableExpr interface.
func (expr *JoinTableExpr) WalkTableExpr(v Visitor) TableExpr {
	left, changedL := walkTableExpr(v, expr.Left)
	right, changedR := walkTableExpr(v, expr.Right)
	if changedL || changedR {
		exprCopy := *expr
		exprCopy.Left = left
		exprCopy.Right = right
		return &exprCopy
	}
	return expr
}

func (expr *RowsFromExpr) copyNode() *RowsFromExpr {
	exprCopy := *expr
	exprCopy.Items = append(Exprs(nil), exprCopy.Items...)
	return &exprCopy
}

// WalkTableExpr implements the TableExpr interface.
func (expr *RowsFromExpr) WalkTableExpr(v Visitor) TableExpr {
	ret := expr
	for i := range expr.Items {
		e, changed := WalkExpr(v, expr.Items[i])
		if changed {
			if ret == expr {
				ret = expr.copyNode()
			}
			ret.Items[i] = e
		}
	}
	return ret
}

// WalkTableExpr implements the TableExpr interface.
func (expr *StatementSource) WalkTableExpr(v Visitor) TableExpr {
	s, changed := WalkStmt(v, expr.Statement)
	if changed {
		exprCopy := *expr
		exprCopy.Statement = s
		return &exprCopy
	}
	return expr
}

// WalkTableExpr implements the TableExpr interface.
func (expr *TableName) WalkTableExpr(_ Visitor) TableExpr { return expr }

// WalkTableExpr implements the TableExpr interface.
func (expr *TableRef) WalkTableExpr(_ Visitor) TableExpr { return expr }

// WalkTableExpr implements the TableExpr interface.
func (expr *UnresolvedObjectName) WalkTableExpr(_ Visitor) TableExpr { return expr }

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
func (expr *DVoid) Walk(_ Visitor) Expr { return expr }

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
func (expr PartitionMaxVal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr PartitionMinVal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *NumVal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *StrVal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *Placeholder) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DBitArray) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DBool) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DBytes) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DEncodedKey) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DDate) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DTime) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DTimeTZ) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DFloat) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DEnum) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DDecimal) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DInt) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DInterval) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DBox2D) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DPGLSN) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DPGVector) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DGeography) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DGeometry) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DJSON) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DJsonpath) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DTSQuery) Walk(_ Visitor) Expr { return expr }

// Walk implements the Expr interface.
func (expr *DTSVector) Walk(_ Visitor) Expr { return expr }

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
func (expr *DTuple) Walk(v Visitor) Expr {
	for _, d := range expr.D {
		// Datums never get changed by visitors, so we don't need to
		// look for changed elements.
		d.Walk(v)
	}
	return expr
}

// Walk implements the Expr interface.
func (expr *DArray) Walk(v Visitor) Expr {
	for _, d := range expr.Array {
		// Datums never get changed by visitors, so we don't need to
		// look for changed elements.
		d.Walk(v)
	}
	return expr
}

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

func walkTableExpr(v Visitor, expr TableExpr) (newExpr TableExpr, changed bool) {
	newExpr = expr.WalkTableExpr(v)
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

// walkableStmt is implemented by statements that can appear inside an expression (selects) or
// we want to start a walk from (using WalkStmt).
type walkableStmt interface {
	Statement
	walkStmt(Visitor) Statement
}

func walkReturningClause(v Visitor, clause ReturningClause) (ReturningClause, bool) {
	switch t := clause.(type) {
	case *ReturningExprs:
		ret := t
		for i, expr := range *t {
			e, changed := WalkExpr(v, expr.Expr)
			if changed {
				if ret == t {
					ret = t.copyNode()
				}
				(*ret)[i].Expr = e
			}
		}
		return ret, (ret != t)
	case *ReturningNothing, *NoReturningClause:
		return t, false
	default:
		panic(errors.AssertionFailedf("unexpected ReturningClause type: %T", t))
	}
}

// copyNode makes a copy of this clause without recursing in any child clause.
func (ts *TenantSpec) copyNode() *TenantSpec {
	tsCopy := *ts
	return &tsCopy
}

func walkTenantSpec(v Visitor, ts *TenantSpec) (*TenantSpec, bool) {
	if ts.Expr == nil {
		return ts, false
	}
	e, changed := WalkExpr(v, ts.Expr)
	if changed {
		ret := ts.copyNode()
		ret.Expr = e
		return ret, true
	}
	return ts, false
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (n *ShowTenantClusterSetting) copyNode() *ShowTenantClusterSetting {
	stmtCopy := *n
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (n *ShowTenantClusterSetting) walkStmt(v Visitor) Statement {
	ret := n
	sc, changed := WalkStmt(v, n.ShowClusterSetting)
	if changed {
		ret = n.copyNode()
		ret.ShowClusterSetting = sc.(*ShowClusterSetting)
	}
	ts, changed := walkTenantSpec(v, n.TenantSpec)
	if changed {
		if ret == n {
			ret = n.copyNode()
		}
		ret.TenantSpec = ts
	}
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (n *ShowTenantClusterSettingList) copyNode() *ShowTenantClusterSettingList {
	stmtCopy := *n
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (n *ShowTenantClusterSettingList) walkStmt(v Visitor) Statement {
	ret := n
	sc, changed := WalkStmt(v, n.ShowClusterSettingList)
	if changed {
		ret = n.copyNode()
		ret.ShowClusterSettingList = sc.(*ShowClusterSettingList)
	}
	ts, changed := walkTenantSpec(v, n.TenantSpec)
	if changed {
		if ret == n {
			ret = n.copyNode()
		}
		ret.TenantSpec = ts
	}
	return ret
}

func (n *AlterTenantCapability) walkStmt(v Visitor) Statement {
	ret := n
	copyNodeOnce := func() {
		if ret == n {
			stmtCopy := *n
			ret = &stmtCopy
		}
	}
	for i, capability := range n.Capabilities {
		value := capability.Value
		if value != nil {
			e, changed := WalkExpr(v, value)
			if changed {
				copyNodeOnce()
				ret.Capabilities[i].Value = e
			}
		}
	}
	ts, changed := walkTenantSpec(v, n.TenantSpec)
	if changed {
		copyNodeOnce()
		ret.TenantSpec = ts
	}
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (n *AlterTenantSetClusterSetting) copyNode() *AlterTenantSetClusterSetting {
	stmtCopy := *n
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (n *AlterTenantSetClusterSetting) walkStmt(v Visitor) Statement {
	ret := n
	if n.Value != nil {
		e, changed := WalkExpr(v, n.Value)
		if changed {
			ret = n.copyNode()
			ret.Value = e
		}
	}
	ts, changed := walkTenantSpec(v, n.TenantSpec)
	if changed {
		if ret == n {
			ret = n.copyNode()
		}
		ret.TenantSpec = ts
	}
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (n *AlterTenantReplication) copyNode() *AlterTenantReplication {
	stmtCopy := *n
	if stmtCopy.Cutover != nil {
		cutoverCopy := *stmtCopy.Cutover
		stmtCopy.Cutover = &cutoverCopy
	}
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (n *AlterTenantReplication) walkStmt(v Visitor) Statement {
	ret := n
	ts, changed := walkTenantSpec(v, n.TenantSpec)
	if changed {
		if ret == n {
			ret = n.copyNode()
		}
		ret.TenantSpec = ts
	}
	if n.Cutover != nil && n.Cutover.Timestamp != nil {
		e, changed := WalkExpr(v, n.Cutover.Timestamp)
		if changed {
			if ret == n {
				ret = n.copyNode()
			}
			ret.Cutover.Timestamp = e
		}
	}
	if n.ReplicationSourceConnUri != nil {
		e, changed := WalkExpr(v, n.ReplicationSourceConnUri)
		if changed {
			if ret == n {
				ret = n.copyNode()
			}
			ret.ReplicationSourceConnUri = e
		}
	}
	if n.ReplicationSourceTenantName != nil {
		ts, changed := walkTenantSpec(v, n.ReplicationSourceTenantName)
		if changed {
			if ret == n {
				ret = n.copyNode()
			}
			ret.TenantSpec = ts
		}
	}
	if n.Options.Retention != nil {
		e, changed := WalkExpr(v, n.Options.Retention)
		if changed {
			if ret == n {
				ret = n.copyNode()
			}
			ret.Options.Retention = e
		}
	}
	if n.Options.ExpirationWindow != nil {
		e, changed := WalkExpr(v, n.Options.ExpirationWindow)
		if changed {
			if ret == n {
				ret = n.copyNode()
			}
			ret.Options.ExpirationWindow = e
		}
	}
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (n *CreateTenant) copyNode() *CreateTenant {
	stmtCopy := *n
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (n *CreateTenant) walkStmt(v Visitor) Statement {
	ret := n
	ts, changed := walkTenantSpec(v, n.TenantSpec)
	if changed {
		if ret == n {
			ret = n.copyNode()
		}
		ret.TenantSpec = ts
	}
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (n *CreateTenantFromReplication) copyNode() *CreateTenantFromReplication {
	stmtCopy := *n
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (n *CreateTenantFromReplication) walkStmt(v Visitor) Statement {
	ret := n
	ts, changed := walkTenantSpec(v, n.TenantSpec)
	if changed {
		if ret == n {
			ret = n.copyNode()
		}
		ret.TenantSpec = ts
	}
	e, changed := WalkExpr(v, n.ReplicationSourceTenantName.Expr)
	if changed {
		if ret == n {
			ret = n.copyNode()
		}
		ret.ReplicationSourceTenantName = &TenantSpec{IsName: true, Expr: e}
	}
	e, changed = WalkExpr(v, n.ReplicationSourceConnUri)
	if changed {
		if ret == n {
			ret = n.copyNode()
		}
		ret.ReplicationSourceConnUri = e
	}
	if n.Options.Retention != nil {
		e, changed := WalkExpr(v, n.Options.Retention)
		if changed {
			if ret == n {
				ret = n.copyNode()
			}
			ret.Options.Retention = e
		}
	}
	if n.Options.ExpirationWindow != nil {
		e, changed := WalkExpr(v, n.Options.ExpirationWindow)
		if changed {
			if ret == n {
				ret = n.copyNode()
			}
			ret.Options.ExpirationWindow = e
		}
	}
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (n *ShowTenant) copyNode() *ShowTenant {
	stmtCopy := *n
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (n *ShowTenant) walkStmt(v Visitor) Statement {
	ret := n
	ts, changed := walkTenantSpec(v, n.TenantSpec)
	if changed {
		if ret == n {
			ret = n.copyNode()
		}
		ret.TenantSpec = ts
	}
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (n *ShowFingerprints) copyNode() *ShowFingerprints {
	stmtCopy := *n
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (n *ShowFingerprints) walkStmt(v Visitor) Statement {
	ret := n
	if n.TenantSpec != nil {
		ts, changed := walkTenantSpec(v, n.TenantSpec)
		if changed {
			if ret == n {
				ret = n.copyNode()
			}
			ret.TenantSpec = ts
		}
	}
	if n.Options.StartTimestamp != nil {
		e, changed := WalkExpr(v, n.Options.StartTimestamp)
		if changed {
			if ret == n {
				ret = n.copyNode()
			}
			ret.Options.StartTimestamp = e
		}
	}

	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (n *AlterTenantRename) copyNode() *AlterTenantRename {
	stmtCopy := *n
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (n *AlterTenantRename) walkStmt(v Visitor) Statement {
	ret := n
	ts, changed := walkTenantSpec(v, n.TenantSpec)
	if changed {
		if ret == n {
			ret = n.copyNode()
		}
		ret.TenantSpec = ts
	}
	ts, changed = walkTenantSpec(v, n.NewName)
	if changed {
		if ret == n {
			ret = n.copyNode()
		}
		ret.NewName = ts
	}
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (n *AlterTenantReset) copyNode() *AlterTenantReset {
	stmtCopy := *n
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (n *AlterTenantReset) walkStmt(v Visitor) Statement {
	ret := n
	ts, changed := walkTenantSpec(v, n.TenantSpec)
	if changed {
		if ret == n {
			ret = n.copyNode()
		}
		ret.TenantSpec = ts
	}
	if n.Timestamp != nil {
		e, changed := WalkExpr(v, n.Timestamp)
		if changed {
			if ret == n {
				ret = n.copyNode()
			}
			ret.Timestamp = e
		}
	}

	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (n *AlterTenantService) copyNode() *AlterTenantService {
	stmtCopy := *n
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (n *AlterTenantService) walkStmt(v Visitor) Statement {
	ret := n
	ts, changed := walkTenantSpec(v, n.TenantSpec)
	if changed {
		if ret == n {
			ret = n.copyNode()
		}
		ret.TenantSpec = ts
	}
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (n *DropTenant) copyNode() *DropTenant {
	stmtCopy := *n
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (n *DropTenant) walkStmt(v Visitor) Statement {
	ret := n
	ts, changed := walkTenantSpec(v, n.TenantSpec)
	if changed {
		if ret == n {
			ret = n.copyNode()
		}
		ret.TenantSpec = ts
	}
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Backup) copyNode() *Backup {
	stmtCopy := *stmt
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *Backup) walkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.AsOf.Expr != nil {
		e, changed := WalkExpr(v, stmt.AsOf.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.AsOf.Expr = e
		}
	}
	for i, expr := range stmt.To {
		e, changed := WalkExpr(v, expr)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.To[i] = e
		}
	}

	if stmt.Options.EncryptionPassphrase != nil {
		pw, changed := WalkExpr(v, stmt.Options.EncryptionPassphrase)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Options.EncryptionPassphrase = pw
		}
	}
	if stmt.Options.CaptureRevisionHistory != nil {
		rh, changed := WalkExpr(v, stmt.Options.CaptureRevisionHistory)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Options.CaptureRevisionHistory = rh
		}
	}

	if stmt.Options.IncludeAllSecondaryTenants != nil {
		include, changed := WalkExpr(v, stmt.Options.IncludeAllSecondaryTenants)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Options.IncludeAllSecondaryTenants = include
		}
	}

	if stmt.Options.ExecutionLocality != nil {
		rh, changed := WalkExpr(v, stmt.Options.ExecutionLocality)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Options.ExecutionLocality = rh
		}
	}

	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Delete) copyNode() *Delete {
	stmtCopy := *stmt
	if stmt.Where != nil {
		wCopy := *stmt.Where
		stmtCopy.Where = &wCopy
	}
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *Delete) walkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.Where != nil {
		e, changed := WalkExpr(v, stmt.Where.Expr)
		if changed {
			ret = stmt.copyNode()
			ret.Where.Expr = e
		}
	}
	returning, changed := walkReturningClause(v, stmt.Returning)
	if changed {
		if ret == stmt {
			ret = stmt.copyNode()
		}
		ret.Returning = returning
	}
	return ret
}

// walkStmt is part of the walkableStmt interface.
func (stmt *DoBlock) walkStmt(v Visitor) Statement {
	body := stmt.Code.VisitBody(v)
	if body != stmt.Code {
		stmtCopy := *stmt
		stmtCopy.Code = body
		return &stmtCopy
	}
	return stmt
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Explain) copyNode() *Explain {
	stmtCopy := *stmt
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *Explain) walkStmt(v Visitor) Statement {
	s, changed := WalkStmt(v, stmt.Statement)
	if changed {
		stmt = stmt.copyNode()
		stmt.Statement = s
	}
	return stmt
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *ExplainAnalyze) copyNode() *ExplainAnalyze {
	stmtCopy := *stmt
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *ExplainAnalyze) walkStmt(v Visitor) Statement {
	s, changed := WalkStmt(v, stmt.Statement)
	if changed {
		stmt = stmt.copyNode()
		stmt.Statement = s
	}
	return stmt
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Insert) copyNode() *Insert {
	stmtCopy := *stmt
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *Insert) walkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.Rows != nil {
		rows, changed := WalkStmt(v, stmt.Rows)
		if changed {
			ret = stmt.copyNode()
			ret.Rows = rows.(*Select)
		}
	}
	returning, changed := walkReturningClause(v, stmt.Returning)
	if changed {
		if ret == stmt {
			ret = stmt.copyNode()
		}
		ret.Returning = returning
	}
	// TODO(dan): Walk OnConflict once the ON CONFLICT DO UPDATE form of upsert is
	// implemented.
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *CreateTable) copyNode() *CreateTable {
	stmtCopy := *stmt
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *CreateTable) walkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.AsSource != nil {
		rows, changed := WalkStmt(v, stmt.AsSource)
		if changed {
			ret = stmt.copyNode()
			ret.AsSource = rows.(*Select)
		}
	}
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *CancelQueries) copyNode() *CancelQueries {
	stmtCopy := *stmt
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *CancelQueries) walkStmt(v Visitor) Statement {
	sel, changed := WalkStmt(v, stmt.Queries)
	if changed {
		stmt = stmt.copyNode()
		stmt.Queries = sel.(*Select)
	}
	return stmt
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *CancelSessions) copyNode() *CancelSessions {
	stmtCopy := *stmt
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *CancelSessions) walkStmt(v Visitor) Statement {
	sel, changed := WalkStmt(v, stmt.Sessions)
	if changed {
		stmt = stmt.copyNode()
		stmt.Sessions = sel.(*Select)
	}
	return stmt
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *ControlJobs) copyNode() *ControlJobs {
	stmtCopy := *stmt
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *ControlJobs) walkStmt(v Visitor) Statement {
	sel, changed := WalkStmt(v, stmt.Jobs)
	if changed {
		stmt = stmt.copyNode()
		stmt.Jobs = sel.(*Select)
	}
	return stmt
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (n *ControlSchedules) copyNode() *ControlSchedules {
	stmtCopy := *n
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (n *ControlSchedules) walkStmt(v Visitor) Statement {
	sel, changed := WalkStmt(v, n.Schedules)
	if changed {
		n = n.copyNode()
		n.Schedules = sel.(*Select)
	}
	return n
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Import) copyNode() *Import {
	stmtCopy := *stmt
	stmtCopy.Files = append(Exprs(nil), stmt.Files...)
	stmtCopy.Options = append(KVOptions(nil), stmt.Options...)
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *Import) walkStmt(v Visitor) Statement {
	ret := stmt
	for i, expr := range stmt.Files {
		e, changed := WalkExpr(v, expr)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Files[i] = e
		}
	}
	{
		opts, changed := walkKVOptions(v, stmt.Options)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Options = opts
		}
	}
	return ret
}

// walkStmt is part of the walkableStmt interface.
func (stmt *ParenSelect) walkStmt(v Visitor) Statement {
	sel, changed := WalkStmt(v, stmt.Select)
	if changed {
		return &ParenSelect{sel.(*Select)}
	}
	return stmt
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Restore) copyNode() *Restore {
	stmtCopy := *stmt
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *Restore) walkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.AsOf.Expr != nil {
		e, changed := WalkExpr(v, stmt.AsOf.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.AsOf.Expr = e
		}
	}
	for i, expr := range stmt.From {
		e, changed := WalkExpr(v, expr)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.From[i] = e
		}
	}

	if stmt.Options.EncryptionPassphrase != nil {
		pw, changed := WalkExpr(v, stmt.Options.EncryptionPassphrase)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Options.EncryptionPassphrase = pw
		}
	}

	if stmt.Options.IntoDB != nil {
		intoDB, changed := WalkExpr(v, stmt.Options.IntoDB)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Options.IntoDB = intoDB
		}
	}

	if stmt.Options.ExecutionLocality != nil {
		include, changed := WalkExpr(v, stmt.Options.ExecutionLocality)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Options.ExecutionLocality = include
		}
	}

	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *ReturningExprs) copyNode() *ReturningExprs {
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
			orderByCopy := *order[i]
			orderByCopy.Expr = e
			order[i] = &orderByCopy
		}
	}
	return order, copied
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Select) copyNode() *Select {
	stmtCopy := *stmt
	if stmt.Limit != nil {
		lCopy := *stmt.Limit
		stmtCopy.Limit = &lCopy
	}
	if stmt.With != nil {
		withCopy := *stmt.With
		stmtCopy.With = &withCopy
		cteList := make([]CTE, len(stmt.With.CTEList))
		stmtCopy.With.CTEList = make([]*CTE, len(stmt.With.CTEList))
		for i, cte := range stmt.With.CTEList {
			cteList[i] = *cte
			stmtCopy.With.CTEList[i] = &cteList[i]
		}
	}
	return &stmtCopy
}

func (n *CopyTo) walkStmt(v Visitor) Statement {
	if newStmt, changed := WalkStmt(v, n.Statement); changed {
		// Make a copy of the CopyTo statement.
		stmtCopy := *n
		ret := &(stmtCopy)
		ret.Statement = newStmt
	}
	return n
}

// walkStmt is part of the walkableStmt interface.
func (stmt *Select) walkStmt(v Visitor) Statement {
	ret := stmt
	sel, changed := WalkStmt(v, stmt.Select)
	if changed {
		ret = stmt.copyNode()
		ret.Select = sel.(SelectStatement)
	}
	order, changed := walkOrderBy(v, stmt.OrderBy)
	if changed {
		if ret == stmt {
			ret = stmt.copyNode()
		}
		ret.OrderBy = order
	}
	if stmt.Limit != nil {
		if stmt.Limit.Offset != nil {
			e, changed := WalkExpr(v, stmt.Limit.Offset)
			if changed {
				if ret == stmt {
					ret = stmt.copyNode()
				}
				ret.Limit.Offset = e
			}
		}
		if stmt.Limit.Count != nil {
			e, changed := WalkExpr(v, stmt.Limit.Count)
			if changed {
				if ret == stmt {
					ret = stmt.copyNode()
				}
				ret.Limit.Count = e
			}
		}
	}
	if stmt.With != nil {
		for i := range stmt.With.CTEList {
			if stmt.With.CTEList[i] != nil {
				withStmt, changed := WalkStmt(v, stmt.With.CTEList[i].Stmt)
				if changed {
					if ret == stmt {
						ret = stmt.copyNode()
					}
					ret.With.CTEList[i].Stmt = withStmt
				}
			}
		}
	}

	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *SelectClause) copyNode() *SelectClause {
	stmtCopy := *stmt
	stmtCopy.Exprs = append(SelectExprs(nil), stmt.Exprs...)
	stmtCopy.From = From{
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
	stmtCopy.Window = append(Window(nil), stmt.Window...)
	stmtCopy.DistinctOn = append(DistinctOn(nil), stmt.DistinctOn...)
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *SelectClause) walkStmt(v Visitor) Statement {
	ret := stmt

	for i, expr := range stmt.Exprs {
		e, changed := WalkExpr(v, expr.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Exprs[i].Expr = e
		}
	}

	if stmt.From.AsOf.Expr != nil {
		e, changed := WalkExpr(v, stmt.From.AsOf.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.From.AsOf.Expr = e
		}
	}

	if stmt.Where != nil {
		e, changed := WalkExpr(v, stmt.Where.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Where.Expr = e
		}
	}

	for i, expr := range stmt.GroupBy {
		e, changed := WalkExpr(v, expr)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.GroupBy[i] = e
		}
	}

	if stmt.Having != nil {
		e, changed := WalkExpr(v, stmt.Having.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Having.Expr = e
		}
	}

	for i := range stmt.Window {
		w, changed := walkWindowDef(v, stmt.Window[i])
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Window[i] = w
		}
	}

	for i := range stmt.From.Tables {
		t, changed := walkTableExpr(v, stmt.From.Tables[i])
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.From.Tables[i] = t
		}
	}

	for i := range stmt.DistinctOn {
		e, changed := WalkExpr(v, stmt.DistinctOn[i])
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.DistinctOn[i] = e
		}
	}

	return ret
}

func (stmt *UnionClause) walkStmt(v Visitor) Statement {
	left, changedL := WalkStmt(v, stmt.Left)
	right, changedR := WalkStmt(v, stmt.Right)
	if changedL || changedR {
		stmtCopy := *stmt
		stmtCopy.Left = left.(*Select)
		stmtCopy.Right = right.(*Select)
		return &stmtCopy
	}
	return stmt
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *SetVar) copyNode() *SetVar {
	stmtCopy := *stmt
	stmtCopy.Values = append(Exprs(nil), stmt.Values...)
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *SetVar) walkStmt(v Visitor) Statement {
	ret := stmt
	for i, expr := range stmt.Values {
		e, changed := WalkExpr(v, expr)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Values[i] = e
		}
	}
	return ret
}

// walkStmt is part of the walkableStmt interface.
func (stmt *SetZoneConfig) walkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.YAMLConfig != nil {
		e, changed := WalkExpr(v, stmt.YAMLConfig)
		if changed {
			newStmt := *stmt
			ret = &newStmt
			ret.YAMLConfig = e
		}
	}
	if stmt.Options != nil {
		newOpts, changed := walkKVOptions(v, stmt.Options)
		if changed {
			if ret == stmt {
				newStmt := *stmt
				ret = &newStmt
			}
			ret.Options = newOpts
		}
	}
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *SetTracing) copyNode() *SetTracing {
	stmtCopy := *stmt
	stmtCopy.Values = append(Exprs(nil), stmt.Values...)
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *SetTracing) walkStmt(v Visitor) Statement {
	ret := stmt
	for i, expr := range stmt.Values {
		e, changed := WalkExpr(v, expr)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Values[i] = e
		}
	}
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *SetClusterSetting) copyNode() *SetClusterSetting {
	stmtCopy := *stmt
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *SetClusterSetting) walkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.Value != nil {
		e, changed := WalkExpr(v, stmt.Value)
		if changed {
			ret = stmt.copyNode()
			ret.Value = e
		}
	}
	return ret
}

// copyNode makes a copy of this Statement without recursing in any child Statements.
func (stmt *Update) copyNode() *Update {
	stmtCopy := *stmt
	exprs := make([]UpdateExpr, len(stmt.Exprs))
	stmtCopy.Exprs = make(UpdateExprs, len(stmt.Exprs))
	for i, e := range stmt.Exprs {
		exprs[i] = *e
		stmtCopy.Exprs[i] = &exprs[i]
	}
	if stmt.Where != nil {
		wCopy := *stmt.Where
		stmtCopy.Where = &wCopy
	}
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *Update) walkStmt(v Visitor) Statement {
	ret := stmt
	for i, expr := range stmt.Exprs {
		e, changed := WalkExpr(v, expr.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Exprs[i].Expr = e
		}
	}

	if stmt.Where != nil {
		e, changed := WalkExpr(v, stmt.Where.Expr)
		if changed {
			if ret == stmt {
				ret = stmt.copyNode()
			}
			ret.Where.Expr = e
		}
	}

	returning, changed := walkReturningClause(v, stmt.Returning)
	if changed {
		if ret == stmt {
			ret = stmt.copyNode()
		}
		ret.Returning = returning
	}
	return ret
}

// walkStmt is part of the walkableStmt interface.
func (stmt *ValuesClause) walkStmt(v Visitor) Statement {
	ret := stmt
	for i, tuple := range stmt.Rows {
		exprs, changed := walkExprSlice(v, tuple)
		if changed {
			if ret == stmt {
				ret = &ValuesClause{append([]Exprs(nil), stmt.Rows...)}
			}
			ret.Rows[i] = exprs
		}
	}
	return ret
}

// copyNode makes a copy of this Statement.
func (stmt *BeginTransaction) copyNode() *BeginTransaction {
	stmtCopy := *stmt
	return &stmtCopy
}

// walkStmt is part of the walkableStmt interface.
func (stmt *BeginTransaction) walkStmt(v Visitor) Statement {
	ret := stmt
	if stmt.Modes.AsOf.Expr != nil {
		e, changed := WalkExpr(v, stmt.Modes.AsOf.Expr)
		if changed {
			ret = stmt.copyNode()
			ret.Modes.AsOf.Expr = e
		}
	}
	return ret
}

var _ walkableStmt = &AlterTenantCapability{}
var _ walkableStmt = &AlterTenantRename{}
var _ walkableStmt = &AlterTenantReplication{}
var _ walkableStmt = &AlterTenantService{}
var _ walkableStmt = &AlterTenantSetClusterSetting{}
var _ walkableStmt = &Backup{}
var _ walkableStmt = &BeginTransaction{}
var _ walkableStmt = &CancelQueries{}
var _ walkableStmt = &CancelSessions{}
var _ walkableStmt = &ControlJobs{}
var _ walkableStmt = &ControlSchedules{}
var _ walkableStmt = &CreateTable{}
var _ walkableStmt = &CreateTenant{}
var _ walkableStmt = &CreateTenantFromReplication{}
var _ walkableStmt = &Delete{}
var _ walkableStmt = &DoBlock{}
var _ walkableStmt = &DropTenant{}
var _ walkableStmt = &Explain{}
var _ walkableStmt = &Import{}
var _ walkableStmt = &Insert{}
var _ walkableStmt = &ParenSelect{}
var _ walkableStmt = &Restore{}
var _ walkableStmt = &SelectClause{}
var _ walkableStmt = &Select{}
var _ walkableStmt = &SetClusterSetting{}
var _ walkableStmt = &SetTransaction{}
var _ walkableStmt = &SetVar{}
var _ walkableStmt = &ShowFingerprints{}
var _ walkableStmt = &ShowTenantClusterSetting{}
var _ walkableStmt = &ShowTenant{}
var _ walkableStmt = &UnionClause{}
var _ walkableStmt = &Update{}
var _ walkableStmt = &ValuesClause{}

// WalkStmt walks the entire parsed stmt calling WalkExpr on each
// expression, and replacing each expression with the one returned
// by WalkExpr.
//
// NOTE: Beware that WalkStmt does not necessarily traverse all parts of a
// statement by itself. For example, it will not walk into Subquery nodes
// within a FROM clause or into a JoinCond. Walk's logic is pretty
// interdependent with the logic for constructing a query plan.
func WalkStmt(v Visitor, stmt Statement) (newStmt Statement, changed bool) {
	walkable, ok := stmt.(walkableStmt)
	if !ok {
		return stmt, false
	}
	newStmt = walkable.walkStmt(v)
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
	recurse, newExpr, v.err = v.fn(expr)
	if v.err != nil {
		return false, expr
	}
	return recurse, newExpr
}

func (*simpleVisitor) VisitPost(expr Expr) Expr { return expr }

// SimpleVisitFn is a function that is run for every node in the VisitPre stage;
// see SimpleVisit.
type SimpleVisitFn func(expr Expr) (recurse bool, newExpr Expr, err error)

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

// SimpleStmtVisit is a convenience wrapper for visitors that want to visit
// all part of a statement, only have VisitPre code and don't return
// any results except an error. The given function is called in VisitPre
// for every node. The visitor stops as soon as an error is returned.
func SimpleStmtVisit(stmt Statement, preFn SimpleVisitFn) (Statement, error) {
	v := simpleVisitor{fn: preFn}
	newStmt, changed := WalkStmt(&v, stmt)
	if v.err != nil {
		return nil, v.err
	}
	if changed {
		return newStmt, nil
	}
	return stmt, nil
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
