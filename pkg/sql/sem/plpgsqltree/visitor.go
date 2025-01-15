// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plpgsqltree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	unimp "github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// StatementVisitor defines methods that are called plpgsql statements during
// a statement walk.
type StatementVisitor interface {
	// Visit is called during a statement walk. If recurse is false for a given
	// node, the node's children (if any) will not be visited.
	Visit(stmt Statement) (newStmt Statement, recurse bool)
}

// Walk traverses the plpgsql statement.
func Walk(v StatementVisitor, stmt Statement) Statement {
	newStmt := stmt.WalkStmt(v)
	return newStmt
}

// SQLStmtVisitor applies a visitor to every SQL statement and expression found
// while walking the PL/pgSQL AST. SQLStmtVisitor supports using
// tree.v.visitExprFn for callers that don't need the full Visitor interface.
//
// *Only one of Fn or Visitor can be set.*
type SQLStmtVisitor struct {
	Fn      tree.SimpleVisitFn
	Visitor tree.Visitor
	Err     error
}

var _ StatementVisitor = &SQLStmtVisitor{}

// visitExpr calls the visitor function on the given expression. The updated
// expression could be nil, so SQLStmtVisitor must be prepared to handle that.
func (v *SQLStmtVisitor) visitExpr(expr tree.Expr) (newExpr tree.Expr, err error) {
	if expr == nil {
		return nil, nil
	}
	if v.Fn != nil {
		newExpr, err = tree.SimpleVisit(expr, v.Fn)
	} else {
		newExpr, _ = tree.WalkExpr(v.Visitor, expr)
	}
	if err != nil {
		v.Err = err
	}
	return newExpr, v.Err
}

// visitStmt calls the visitor function on the given statement. The updated
// expression could be nil, so SQLStmtVisitor must be prepared to handle that.
func (v *SQLStmtVisitor) visitStmt(stmt tree.Statement) (newStmt tree.Statement, err error) {
	if stmt == nil {
		return nil, nil
	}
	if v.Fn != nil {
		newStmt, err = tree.SimpleStmtVisit(stmt, v.Fn)
	} else {
		newStmt, _ = tree.WalkStmt(v.Visitor, stmt)
	}
	if err != nil {
		v.Err = err
	}
	return newStmt, v.Err
}

func (v *SQLStmtVisitor) Visit(stmt Statement) (newStmt Statement, recurse bool) {
	if v.Err != nil {
		return stmt, false
	}
	newStmt = stmt
	var s tree.Statement
	var e tree.Expr
	switch t := stmt.(type) {
	case *CursorDeclaration:
		s, v.Err = v.visitStmt(t.Query)
		if v.Err != nil {
			return stmt, false
		}
		if t.Query != s {
			cpy := t.CopyNode()
			cpy.Query = s
			newStmt = cpy
		}
	case *Execute:
		s, v.Err = v.visitStmt(t.SqlStmt)
		if v.Err != nil {
			return stmt, false
		}
		if t.SqlStmt != s {
			cpy := t.CopyNode()
			cpy.SqlStmt = s
			newStmt = cpy
		}
	case *Open:
		s, v.Err = v.visitStmt(t.Query)
		if v.Err != nil {
			return stmt, false
		}
		if t.Query != s {
			cpy := t.CopyNode()
			cpy.Query = s
			newStmt = cpy
		}
	case *Declaration:
		e, v.Err = v.visitExpr(t.Expr)
		if v.Err != nil {
			return stmt, false
		}
		if t.Expr != e {
			cpy := t.CopyNode()
			cpy.Expr = e
			newStmt = cpy
		}

	case *Assignment:
		e, v.Err = v.visitExpr(t.Value)
		if v.Err != nil {
			return stmt, false
		}
		if t.Value != e {
			cpy := t.CopyNode()
			cpy.Value = e
			newStmt = cpy
		}
	case *If:
		e, v.Err = v.visitExpr(t.Condition)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *ElseIf:
		e, v.Err = v.visitExpr(t.Condition)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *While:
		e, v.Err = v.visitExpr(t.Condition)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *Exit:
		e, v.Err = v.visitExpr(t.Condition)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *Continue:
		e, v.Err = v.visitExpr(t.Condition)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *Return:
		e, v.Err = v.visitExpr(t.Expr)
		if v.Err != nil {
			return stmt, false
		}
		if t.Expr != e {
			cpy := t.CopyNode()
			cpy.Expr = e
			newStmt = cpy
		}
	case *Raise:
		for i, p := range t.Params {
			e, v.Err = v.visitExpr(p)
			if v.Err != nil {
				return stmt, false
			}
			if t.Params[i] != e {
				if newStmt != stmt {
					cpy := t.CopyNode()
					newStmt = cpy
				}
				newStmt.(*Raise).Params[i] = e
			}
		}
		for i, p := range t.Options {
			e, v.Err = v.visitExpr(p.Expr)
			if v.Err != nil {
				return stmt, false
			}
			if t.Options[i].Expr != e {
				if newStmt != stmt {
					cpy := t.CopyNode()
					newStmt = cpy
				}
				newStmt.(*Raise).Options[i].Expr = e
			}
		}
	case *Assert:
		e, v.Err = v.visitExpr(t.Condition)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}

	case *DynamicExecute:
		for i, p := range t.Params {
			e, v.Err = v.visitExpr(p)
			if v.Err != nil {
				return stmt, false
			}
			if t.Params[i] != e {
				if newStmt != stmt {
					cpy := t.CopyNode()
					newStmt = cpy
				}
				newStmt.(*DynamicExecute).Params[i] = e
			}
		}
	case *Call:
		e, v.Err = v.visitExpr(t.Proc)
		if v.Err != nil {
			return stmt, false
		}
		if t.Proc != e {
			cpy := t.CopyNode()
			if e == nil {
				cpy.Proc = nil
			} else {
				cpy.Proc = e.(*tree.FuncExpr)
			}
			newStmt = cpy
		}

	case *ForLoop:
		switch c := t.Control.(type) {
		case *IntForLoopControl:
			var newLower, newUpper, newStep tree.Expr
			newLower, v.Err = v.visitExpr(c.Lower)
			if v.Err != nil {
				return stmt, false
			}
			newUpper, v.Err = v.visitExpr(c.Upper)
			if v.Err != nil {
				return stmt, false
			}
			if c.Step != nil {
				newStep, v.Err = v.visitExpr(c.Step)
				if v.Err != nil {
					return stmt, false
				}
			}
			if newLower != c.Lower || newUpper != c.Upper || newStep != c.Step {
				cpy := t.CopyNode()
				cpy.Control = &IntForLoopControl{
					Reverse: c.Reverse,
					Lower:   newLower,
					Upper:   newUpper,
					Step:    newStep,
				}
				newStmt = cpy
			}
		}

	case *ForEachArray, *ReturnNext,
		*ReturnQuery, *Perform:
		panic(unimp.New("plpgsql visitor", "Unimplemented PLpgSQL visitor"))
	}
	if v.Err != nil {
		return stmt, false
	}
	return newStmt, true
}

// TypeRefVisitor calls the given replace function on each type reference
// contained in the visited PLpgSQL statements. Note that this currently only
// includes `Declaration`. SQL statements and expressions are not visited.
type TypeRefVisitor struct {
	Fn  func(typ tree.ResolvableTypeReference) (newTyp tree.ResolvableTypeReference, err error)
	Err error
}

var _ StatementVisitor = &TypeRefVisitor{}

func (v *TypeRefVisitor) Visit(stmt Statement) (newStmt Statement, recurse bool) {
	if v.Err != nil {
		return stmt, false
	}
	newStmt = stmt
	if t, ok := stmt.(*Declaration); ok {
		var newTyp tree.ResolvableTypeReference
		newTyp, v.Err = v.Fn(t.Typ)
		if v.Err != nil {
			return stmt, false
		}
		if t.Typ != newTyp {
			if newStmt == stmt {
				newStmt = t.CopyNode()
				newStmt.(*Declaration).Typ = newTyp
			}
		}
	}
	return newStmt, true
}
