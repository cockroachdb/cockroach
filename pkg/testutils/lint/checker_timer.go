// Copyright 2016 The Cockroach Authors.
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

// +build lint

package lint

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"log"

	"honnef.co/go/tools/lint"
)

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
type timerChecker struct {
	timerType types.Type
}

func (*timerChecker) Name() string {
	return "timercheck"
}

func (*timerChecker) Prefix() string {
	return "T"
}

const timerChanName = "C"

func (m *timerChecker) Init(program *lint.Program) {
	timeutilPkg := program.Prog.Package("github.com/cockroachdb/cockroach/pkg/util/timeutil")
	if timeutilPkg == nil {
		log.Fatal("timeutil package not found")
	}
	timerObject := timeutilPkg.Pkg.Scope().Lookup("Timer")
	if timerObject == nil {
		log.Fatal("timeutil.Timer type not found")
	}
	m.timerType = timerObject.Type()

	func() {
		if typ, ok := m.timerType.Underlying().(*types.Struct); ok {
			for i := 0; i < typ.NumFields(); i++ {
				if typ.Field(i).Name() == timerChanName {
					return
				}
			}
		}
		log.Fatalf("no field called %q in type %s", timerChanName, m.timerType)
	}()
}

func (m *timerChecker) Funcs() map[string]lint.Func {
	return map[string]lint.Func{
		"TimeutilTimerRead": m.checkSetTimeutilTimerRead,
	}
}

func (m *timerChecker) selectorIsTimer(s *ast.SelectorExpr, info *types.Info) bool {
	selTyp := info.TypeOf(s.X)
	if ptr, ok := selTyp.(*types.Pointer); ok {
		selTyp = ptr.Elem()
	}
	return selTyp == m.timerType
}

func (m *timerChecker) checkSetTimeutilTimerRead(j *lint.Job) {
	forAllFiles(j, func(n ast.Node) bool {
		return walkForStmts(n, func(s ast.Stmt) bool {
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
				if !m.selectorIsTimer(selector, j.Program.Info) {
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
						if !m.selectorIsTimer(assignSelector, j.Program.Info) {
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
					j.Errorf(comm, "must set timer.Read = true after reading from timer.C (see timeutil/timer.go)")
				}
				return true
			})
		})
	})
}
