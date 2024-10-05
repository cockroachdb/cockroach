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
	"github.com/cockroachdb/errors"
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
	taggedStmt, ok := stmt.(plpgsqltree.TaggedStatement)
	if !ok {
		v.Err = errors.AssertionFailedf("no tag found for stmt %q", stmt)
	}
	tag := taggedStmt.PlpgSQLStatementTag()
	sqltelemetry.IncrementPlpgsqlStmtCounter(tag)

	//Capturing telemetry for tests
	_, ok = v.StmtCnt[tag]
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

// SQLStmtVisitor calls Fn for every SQL statement and expression found while
// walking the PLpgSQL AST. Since PLpgSQL nodes may have statement and
// expression fields that are nil, Fn should handle the nil case.
type SQLStmtVisitor struct {
	Fn  tree.SimpleVisitFn
	Err error
}

var _ plpgsqltree.StatementVisitor = &SQLStmtVisitor{}

// simpleStmtVisit wraps tree.SimpleStmtVisit and handles the case when the
// statement is nil for convenience.
func simpleStmtVisit(statement tree.Statement, fn tree.SimpleVisitFn) (tree.Statement, error) {
	if statement == nil {
		return nil, nil
	}
	return tree.SimpleStmtVisit(statement, fn)
}

// simpleVisit wraps tree.SimpleVisit and handles the case when the expression
// is nil for convenience.
func simpleVisit(expr tree.Expr, fn tree.SimpleVisitFn) (tree.Expr, error) {
	if expr == nil {
		return nil, nil
	}
	return tree.SimpleVisit(expr, fn)
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
		s, v.Err = simpleStmtVisit(t.Query, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		if t.Query != s {
			cpy := t.CopyNode()
			cpy.Query = s
			newStmt = cpy
		}
	case *plpgsqltree.Execute:
		s, v.Err = simpleStmtVisit(t.SqlStmt, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		if t.SqlStmt != s {
			cpy := t.CopyNode()
			cpy.SqlStmt = s
			newStmt = cpy
		}
	case *plpgsqltree.Open:
		s, v.Err = simpleStmtVisit(t.Query, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		if t.Query != s {
			cpy := t.CopyNode()
			cpy.Query = s
			newStmt = cpy
		}
	case *plpgsqltree.Declaration:
		e, v.Err = simpleVisit(t.Expr, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		if t.Expr != e {
			cpy := t.CopyNode()
			cpy.Expr = e
			newStmt = cpy
		}

	case *plpgsqltree.Assignment:
		e, v.Err = simpleVisit(t.Value, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		if t.Value != e {
			cpy := t.CopyNode()
			cpy.Value = e
			newStmt = cpy
		}
	case *plpgsqltree.If:
		e, v.Err = simpleVisit(t.Condition, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.ElseIf:
		e, v.Err = simpleVisit(t.Condition, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.While:
		e, v.Err = simpleVisit(t.Condition, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
		e, v.Err = simpleVisit(t.Condition, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.Exit:
		e, v.Err = simpleVisit(t.Condition, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.Continue:
		e, v.Err = simpleVisit(t.Condition, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		if t.Condition != e {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.Return:
		e, v.Err = simpleVisit(t.Expr, v.Fn)
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
			e, v.Err = simpleVisit(p, v.Fn)
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
			e, v.Err = simpleVisit(p.Expr, v.Fn)
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
		e, v.Err = simpleVisit(t.Condition, v.Fn)
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
			e, v.Err = simpleVisit(p, v.Fn)
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
		e, v.Err = simpleVisit(t.Proc, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		if t.Proc != e {
			cpy := t.CopyNode()
			cpy.Proc = e.(*tree.FuncExpr)
			newStmt = cpy
		}

	case *plpgsqltree.ForInt, *plpgsqltree.ForSelect, *plpgsqltree.ForCursor,
		*plpgsqltree.ForDynamic, *plpgsqltree.ForEachArray, *plpgsqltree.ReturnNext,
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
