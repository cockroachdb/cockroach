// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package timer defines an Analyzer that detects correct use of
// timeutil.Timer.
package timer

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Doc documents this pass.
const Doc = `check for correct use of timeutil.Timer`

// Analyzer defines this pass.
var Analyzer = &analysis.Analyzer{
	Name:     "timer",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// timerChecker assures that timeutil.Timer objects are used correctly, to
// avoid race conditions and deadlocks. These timers require callers to set
// their Read field to true when their channel has been received on. If this
// field is not set and the timer's Reset method is called, we will deadlock.
// This lint assures that the Read field is set in the most common case where
// Reset is used, within a for-loop where each iteration blocks on a select
// statement. The timers are usually used as timeouts on these select
// statements, and need to be reset after each iteration.
//
// for {
//   timer.Reset(...)
//   select {
//     case <-timer.C:
//       timer.Read = true   <--  lint verifies that this line is present
//     case ...:
//   }
// }
//
func run(pass *analysis.Pass) (interface{}, error) {
	selectorIsTimer := func(s *ast.SelectorExpr) bool {
		tv, ok := pass.TypesInfo.Types[s.X]
		if !ok {
			return false
		}
		typ := tv.Type.Underlying()
		for {
			ptr, pok := typ.(*types.Pointer)
			if !pok {
				break
			}
			typ = ptr.Elem()
		}
		named, ok := typ.(*types.Named)
		if !ok {
			return false
		}
		if named.Obj().Type().String() != "github.com/cockroachdb/cockroach/pkg/util/timeutil.Timer" {
			return false
		}
		return true
	}

	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	nodeFilter := []ast.Node{
		(*ast.ForStmt)(nil),
	}
	inspect.Preorder(nodeFilter, func(n ast.Node) {
		fr, ok := n.(*ast.ForStmt)
		if !ok {
			return
		}
		walkStmts(fr.Body.List, func(s ast.Stmt) bool {
			return walkSelectStmts(s, func(s ast.Stmt) bool {
				comm, ok := s.(*ast.CommClause)
				if !ok || comm.Comm == nil /* default: */ {
					return true
				}

				// if receiving on a timer's C chan.
				var unary ast.Expr
				switch v := comm.Comm.(type) {
				case *ast.AssignStmt:
					// case `now := <-timer.C:`
					unary = v.Rhs[0]
				case *ast.ExprStmt:
					// case `<-timer.C:`
					unary = v.X
				default:
					return true
				}
				chanRead, ok := unary.(*ast.UnaryExpr)
				if !ok || chanRead.Op != token.ARROW {
					return true
				}
				selector, ok := chanRead.X.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if !selectorIsTimer(selector) {
					return true
				}
				selectorName := fmt.Sprint(selector.X)
				if selector.Sel.String() != timerChanName {
					return true
				}

				// Verify that the case body contains `timer.Read = true`.
				noRead := walkStmts(comm.Body, func(s ast.Stmt) bool {
					assign, ok := s.(*ast.AssignStmt)
					if !ok || assign.Tok != token.ASSIGN {
						return true
					}
					for i := range assign.Lhs {
						l, r := assign.Lhs[i], assign.Rhs[i]

						// if assignment to correct field in timer.
						assignSelector, ok := l.(*ast.SelectorExpr)
						if !ok {
							return true
						}
						if !selectorIsTimer(assignSelector) {
							return true
						}
						if fmt.Sprint(assignSelector.X) != selectorName {
							return true
						}
						if assignSelector.Sel.String() != "Read" {
							return true
						}

						// if assigning `true`.
						val, ok := r.(*ast.Ident)
						if !ok {
							return true
						}
						if val.String() == "true" {
							// returning false will short-circuit walkStmts and assign
							// noRead to false instead of the default value of true.
							return false
						}
					}
					return true
				})
				if noRead {
					pass.Reportf(comm.Pos(), "must set timer.Read = true after reading from timer.C (see timeutil/timer.go)")
				}
				return true
			})
		})
	})

	return nil, nil
}

const timerChanName = "C"

func walkSelectStmts(n ast.Node, fn func(ast.Stmt) bool) bool {
	sel, ok := n.(*ast.SelectStmt)
	if !ok {
		return true
	}
	return walkStmts(sel.Body.List, fn)
}

func walkStmts(stmts []ast.Stmt, fn func(ast.Stmt) bool) bool {
	for _, stmt := range stmts {
		if !fn(stmt) {
			return false
		}
	}
	return true
}
