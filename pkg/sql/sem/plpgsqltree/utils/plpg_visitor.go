// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
func (v *telemetryVisitor) Visit(stmt plpgsqltree.Statement) (newStmt plpgsqltree.Statement, changed bool) {
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

	return stmt, false
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

// ParseAndCollectTelemetryForPLpgSQLFunc takes a plpgsql function and parses and collects
// telemetry on the parsable statements.
func ParseAndCollectTelemetryForPLpgSQLFunc(stmt *tree.CreateRoutine) error {
	// Assert that the function language is PLPGSQL.
	var funcBodyStr string
	for _, option := range stmt.Options {
		switch opt := option.(type) {
		case tree.RoutineBodyStr:
			funcBodyStr = string(opt)
		}
	}

	if _, err := CountPLpgSQLStmt(funcBodyStr); err != nil {
		return errors.Wrap(err, "plpgsql not supported in user-defined functions")
	}
	return unimp.New("plpgsql", "plpgsql not supported in user-defined functions")
}

// SQLStmtVisitor calls Fn for every SQL statement and expression found while
// walking the PLpgSQL AST. Since PLpgSQL nodes may have statement and
// expression fields that are nil, Fn should handle the nil case.
type SQLStmtVisitor struct {
	Fn    tree.SimpleVisitFn
	TypFn func(typ tree.ResolvableTypeReference) (newTyp tree.ResolvableTypeReference, err error)
	Err   error
}

var _ plpgsqltree.StatementVisitor = &SQLStmtVisitor{}

func (v *SQLStmtVisitor) Visit(stmt plpgsqltree.Statement) (newStmt plpgsqltree.Statement, changed bool) {
	if v.Err != nil {
		return stmt, false
	}
	newStmt = stmt
	var s tree.Statement
	var e tree.Expr
	switch t := stmt.(type) {
	case *plpgsqltree.CursorDeclaration:
		s, v.Err = tree.SimpleStmtVisit(t.Query, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Query != s
		if changed {
			cpy := t.CopyNode()
			cpy.Query = s
			newStmt = cpy
		}
	case *plpgsqltree.Execute:
		s, v.Err = tree.SimpleStmtVisit(t.SqlStmt, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.SqlStmt != s
		if changed {
			cpy := t.CopyNode()
			cpy.SqlStmt = s
			newStmt = cpy
		}
	case *plpgsqltree.Open:
		s, v.Err = tree.SimpleStmtVisit(t.Query, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Query != s
		if changed {
			cpy := t.CopyNode()
			cpy.Query = s
			newStmt = cpy
		}
	case *plpgsqltree.Declaration:
		e, v.Err = tree.SimpleVisit(t.Expr, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Expr != e
		if changed {
			cpy := t.CopyNode()
			cpy.Expr = e
			newStmt = cpy
		}
		if v.TypFn != nil {
			var newTyp tree.ResolvableTypeReference
			newTyp, v.Err = v.TypFn(t.Typ)
			if v.Err != nil {
				return stmt, false
			}
			changed = changed || t.Typ != newTyp
			if changed {
				if newStmt == stmt {
					newStmt = t.CopyNode()
				}
				newStmt.(*plpgsqltree.Declaration).Typ = newTyp
			}
		}
	case *plpgsqltree.Assignment:
		e, v.Err = tree.SimpleVisit(t.Value, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Value != e
		if changed {
			cpy := t.CopyNode()
			cpy.Value = e
			newStmt = cpy
		}
	case *plpgsqltree.If:
		e, v.Err = tree.SimpleVisit(t.Condition, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Condition != e
		if changed {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.ElseIf:
		e, v.Err = tree.SimpleVisit(t.Condition, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Condition != e
		if changed {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.While:
		e, v.Err = tree.SimpleVisit(t.Condition, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Condition != e
		if changed {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.ForInt:
		el, err := tree.SimpleVisit(t.Lower, v.Fn)
		if err != nil {
			v.Err = err
			return stmt, false
		}
		changed = t.Lower != el
		eu, err := tree.SimpleVisit(t.Upper, v.Fn)
		if err != nil {
			v.Err = err
			return stmt, false
		}
		changed = changed || (t.Upper != eu)
		es, err := tree.SimpleVisit(t.Step, v.Fn)
		if err != nil {
			v.Err = err
			return stmt, false
		}
		changed = changed || (t.Step != es)
		if changed {
			cpy := t.CopyNode()
			cpy.Lower = el
			cpy.Upper = eu
			cpy.Step = es
			newStmt = cpy
		}
	case *plpgsqltree.ForSelect:
		e, v.Err = tree.SimpleVisit(t.Query, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Query != e
		if changed {
			cpy := t.CopyNode()
			cpy.Query = e
			newStmt = cpy
		}
	case *plpgsqltree.ForCursor:
		e, v.Err = tree.SimpleVisit(t.ArgQuery, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.ArgQuery != e
		if changed {
			cpy := t.CopyNode()
			cpy.ArgQuery = e
			newStmt = cpy
		}
	case *plpgsqltree.ForDynamic:
		e, v.Err = tree.SimpleVisit(t.Query, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Query != e
		if changed {
			cpy := t.CopyNode()
			cpy.Query = e
			newStmt = cpy
		}
		for i, p := range t.Params {
			e, v.Err = tree.SimpleVisit(p, v.Fn)
			if v.Err != nil {
				return stmt, false
			}
			changed = changed || (t.Params[i] != e)
			if changed {
				if newStmt != stmt {
					cpy := t.CopyNode()
					newStmt = cpy
				}
				newStmt.(*plpgsqltree.ForDynamic).Params[i] = e
			}
		}
	case *plpgsqltree.ForEachArray:
		e, v.Err = tree.SimpleVisit(t.Expr, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Expr != e
		if changed {
			cpy := t.CopyNode()
			cpy.Expr = e
			newStmt = cpy
		}
	case *plpgsqltree.Exit:
		e, v.Err = tree.SimpleVisit(t.Condition, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Condition != e
		if changed {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.Continue:
		e, v.Err = tree.SimpleVisit(t.Condition, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Condition != e
		if changed {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.Return:
		e, v.Err = tree.SimpleVisit(t.Expr, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Expr != e
		if changed {
			cpy := t.CopyNode()
			cpy.Expr = e
			newStmt = cpy
		}
	case *plpgsqltree.ReturnNext:
		e, v.Err = tree.SimpleVisit(t.Expr, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Expr != e
		if changed {
			cpy := t.CopyNode()
			cpy.Expr = e
			newStmt = cpy
		}
	case *plpgsqltree.ReturnQuery:
		e, v.Err = tree.SimpleVisit(t.Query, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Query != e
		if changed {
			cpy := t.CopyNode()
			cpy.Query = e
			newStmt = cpy
		}
		e, v.Err = tree.SimpleVisit(t.DynamicQuery, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = changed || (t.DynamicQuery != e)
		if changed {
			if newStmt == stmt {
				cpy := t.CopyNode()
				newStmt = cpy
			}
			newStmt.(*plpgsqltree.ReturnQuery).DynamicQuery = e
		}
		t.DynamicQuery = e
		for i, p := range t.Params {
			e, v.Err = tree.SimpleVisit(p, v.Fn)
			if v.Err != nil {
				return stmt, false
			}
			changed = changed || (t.Params[i] != e)
			if changed {
				if newStmt != stmt {
					cpy := t.CopyNode()
					newStmt = cpy
				}
				newStmt.(*plpgsqltree.ReturnQuery).Params[i] = e
			}
		}
	case *plpgsqltree.Raise:
		for i, p := range t.Params {
			e, v.Err = tree.SimpleVisit(p, v.Fn)
			if v.Err != nil {
				return stmt, false
			}
			changed = changed || (t.Params[i] != e)
			if changed {
				if newStmt != stmt {
					cpy := t.CopyNode()
					newStmt = cpy
				}
				newStmt.(*plpgsqltree.Raise).Params[i] = e
			}
		}
		for i, p := range t.Options {
			e, v.Err = tree.SimpleVisit(p.Expr, v.Fn)
			if v.Err != nil {
				return stmt, false
			}
			changed = changed || (t.Options[i].Expr != e)
			if changed {
				if newStmt != stmt {
					cpy := t.CopyNode()
					newStmt = cpy
				}
				newStmt.(*plpgsqltree.Raise).Options[i].Expr = e
			}
		}
	case *plpgsqltree.Assert:
		e, v.Err = tree.SimpleVisit(t.Condition, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Condition != e
		if changed {
			cpy := t.CopyNode()
			cpy.Condition = e
			newStmt = cpy
		}
	case *plpgsqltree.DynamicExecute:
		for i, p := range t.Params {
			e, v.Err = tree.SimpleVisit(p, v.Fn)
			if v.Err != nil {
				return stmt, false
			}
			changed = changed || (t.Params[i] != e)
			if changed {
				if newStmt != stmt {
					cpy := t.CopyNode()
					newStmt = cpy
				}
				newStmt.(*plpgsqltree.DynamicExecute).Params[i] = e
			}
		}
	case *plpgsqltree.Perform:
		e, v.Err = tree.SimpleVisit(t.Expr, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Expr != e
		if changed {
			cpy := t.CopyNode()
			cpy.Expr = e
			newStmt = cpy
		}
	case *plpgsqltree.Call:
		e, v.Err = tree.SimpleVisit(t.Expr, v.Fn)
		if v.Err != nil {
			return stmt, false
		}
		changed = t.Expr != e
		if changed {
			cpy := t.CopyNode()
			cpy.Expr = e
			newStmt = cpy
		}
	}
	if v.Err != nil {
		return stmt, false
	}
	return newStmt, changed
}
