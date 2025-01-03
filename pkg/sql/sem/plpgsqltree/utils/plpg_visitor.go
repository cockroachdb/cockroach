// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utils

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	unimp "github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// PLpgSQLStmtCounter is used to accurately report telemetry for plpgsql
// statements test . We can not use the telemetry counters due to them needing
// to be reset after every statement using reporter.ReportDiagnostics.
type PLpgSQLStmtCounter map[string]int

func (p *PLpgSQLStmtCounter) String() string {
	var buf strings.Builder
	counter := *p

	// Sort the counters to avoid flakes in test
	keys := make([]string, 0)
	for k := range counter {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		buf.WriteString(fmt.Sprintf("%s: %d\n", k, counter[k]))

	}
	return buf.String()
}

type telemetryVisitor struct {
	ctx context.Context
	// StmtCnt captures telemetry for plpgsql statements that can be returned
	// during parser test.
	StmtCnt PLpgSQLStmtCounter
	Err     error
}

var _ plpgsqltree.StatementVisitor = &telemetryVisitor{}

// Visit implements the StatementVisitor interface
func (v *telemetryVisitor) Visit(
	stmt plpgsqltree.Statement,
) (newStmt plpgsqltree.Statement, recurse bool) {
	tag := stmt.PlpgSQLStatementTag()
	sqltelemetry.IncrementPlpgsqlStmtCounter(tag)

	//Capturing telemetry for tests
	_, ok := v.StmtCnt[tag]
	if !ok {
		v.StmtCnt[tag] = 1
	} else {
		v.StmtCnt[tag]++
	}
	v.Err = nil

	return stmt, true
}

// MakePLpgSQLTelemetryVisitor makes a plpgsql telemetry visitor, for capturing
// test telemetry
func MakePLpgSQLTelemetryVisitor() telemetryVisitor {
	return telemetryVisitor{ctx: context.Background(), StmtCnt: PLpgSQLStmtCounter{}}
}

// CountPLpgSQLStmt parses the function body and calls the Walk function for
// each plpgsql statement.
func CountPLpgSQLStmt(sql string) (PLpgSQLStmtCounter, error) {
	v := MakePLpgSQLTelemetryVisitor()
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	plpgsqltree.Walk(&v, stmt.AST)

	return v.StmtCnt, v.Err
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

var _ plpgsqltree.StatementVisitor = &SQLStmtVisitor{}

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

func (v *SQLStmtVisitor) Visit(
	stmt plpgsqltree.Statement,
) (newStmt plpgsqltree.Statement, recurse bool) {
	if v.Err != nil {
		return stmt, false
	}
	newStmt = stmt
	var s tree.Statement
	var e tree.Expr
	switch t := stmt.(type) {
	case *plpgsqltree.CursorDeclaration:
		s, v.Err = v.visitStmt(t.Query)
		if v.Err != nil {
			return stmt, false
		}
		if t.Query != s {
			cpy := t.CopyNode()
			cpy.Query = s
			newStmt = cpy
		}
	case *plpgsqltree.Execute:
		s, v.Err = v.visitStmt(t.SqlStmt)
		if v.Err != nil {
			return stmt, false
		}
		if t.SqlStmt != s {
			cpy := t.CopyNode()
			cpy.SqlStmt = s
			newStmt = cpy
		}
	case *plpgsqltree.Open:
		s, v.Err = v.visitStmt(t.Query)
		if v.Err != nil {
			return stmt, false
		}
		if t.Query != s {
			cpy := t.CopyNode()
			cpy.Query = s
			newStmt = cpy
		}
	case *plpgsqltree.Declaration:
		e, v.Err = v.visitExpr(t.Expr)
		if v.Err != nil {
			return stmt, false
		}
		if t.Expr != e {
			cpy := t.CopyNode()
			cpy.Expr = e
			newStmt = cpy
		}

	case *plpgsqltree.Assignment:
		e, v.Err = v.visitExpr(t.Value)
		if v.Err != nil {
			return stmt, false
		}
		if t.Value != e {
			cpy := t.CopyNode()
			cpy.Value = e
			newStmt = cpy
		}
	case *plpgsqltree.If:
		e, v.Err = v.visitExpr(t.Condition)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.ElseIf:
		e, v.Err = v.visitExpr(t.Condition)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.While:
		e, v.Err = v.visitExpr(t.Condition)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.Exit:
		e, v.Err = v.visitExpr(t.Condition)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.Continue:
		e, v.Err = v.visitExpr(t.Condition)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.Return:
		e, v.Err = v.visitExpr(t.Expr)
		if v.Err != nil {
			return stmt, false
		}
		if t.Expr != e {
			cpy := t.CopyNode()
			cpy.Expr = e
			newStmt = cpy
		}
	case *plpgsqltree.Raise:
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
				newStmt.(*plpgsqltree.Raise).Params[i] = e
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
				newStmt.(*plpgsqltree.Raise).Options[i].Expr = e
			}
		}
	case *plpgsqltree.Assert:
		e, v.Err = v.visitExpr(t.Condition)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}

	case *plpgsqltree.DynamicExecute:
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
				newStmt.(*plpgsqltree.DynamicExecute).Params[i] = e
			}
		}
	case *plpgsqltree.Call:
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

	case *plpgsqltree.ForLoop:
		switch c := t.Control.(type) {
		case *plpgsqltree.IntForLoopControl:
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
				cpy.Control = &plpgsqltree.IntForLoopControl{
					Reverse: c.Reverse,
					Lower:   newLower,
					Upper:   newUpper,
					Step:    newStep,
				}
				newStmt = cpy
			}
		}

	case *plpgsqltree.ForEachArray, *plpgsqltree.ReturnNext,
		*plpgsqltree.ReturnQuery, *plpgsqltree.Perform:
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

var _ plpgsqltree.StatementVisitor = &TypeRefVisitor{}

func (v *TypeRefVisitor) Visit(
	stmt plpgsqltree.Statement,
) (newStmt plpgsqltree.Statement, recurse bool) {
	if v.Err != nil {
		return stmt, false
	}
	newStmt = stmt
	if t, ok := stmt.(*plpgsqltree.Declaration); ok {
		var newTyp tree.ResolvableTypeReference
		newTyp, v.Err = v.Fn(t.Typ)
		if v.Err != nil {
			return stmt, false
		}
		if t.Typ != newTyp {
			if newStmt == stmt {
				newStmt = t.CopyNode()
				newStmt.(*plpgsqltree.Declaration).Typ = newTyp
			}
		}
	}
	return newStmt, true
}
