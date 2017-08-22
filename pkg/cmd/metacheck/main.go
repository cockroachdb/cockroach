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

package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"log"
	"os"
	"strings"

	"honnef.co/go/tools/lint"
	"honnef.co/go/tools/lint/lintutil"
	"honnef.co/go/tools/simple"
	"honnef.co/go/tools/staticcheck"
	"honnef.co/go/tools/unused"
)

type metaChecker struct {
	checkers []lint.Checker
}

func (m *metaChecker) Init(program *lint.Program) {
	for _, checker := range m.checkers {
		checker.Init(program)
	}
}

func (m *metaChecker) Funcs() map[string]lint.Func {
	funcs := map[string]lint.Func{
		"FloatToUnsigned":   checkConvertFloatToUnsigned,
		"TimeutilTimerRead": checkSetTimeutilTimerRead,
	}
	for _, checker := range m.checkers {
		for k, v := range checker.Funcs() {
			if _, ok := funcs[k]; ok {
				log.Fatalf("duplicate lint function %s", k)
			} else {
				funcs[k] = v
			}
		}
	}
	return funcs
}

func forAllFiles(j *lint.Job, fn func(node ast.Node) bool) {
	for _, f := range j.Program.Files {
		ast.Inspect(f, fn)
	}
}

// @ianlancetaylor via golang-nuts[0]:
//
// For the record, the spec says, in https://golang.org/ref/spec#Conversions:
// "In all non-constant conversions involving floating-point or complex
// values, if the result type cannot represent the value the conversion
// succeeds but the result value is implementation-dependent."  That is the
// case that applies here: you are converting a negative floating point number
// to uint64, which can not represent a negative value, so the result is
// implementation-dependent.  The conversion to int64 works, of course. And
// the conversion to int64 and then to uint64 succeeds in converting to int64,
// and when converting to uint64 follows a different rule: "When converting
// between integer types, if the value is a signed integer, it is sign
// extended to implicit infinite precision; otherwise it is zero extended. It
// is then truncated to fit in the result type's size."
//
// So, basically, don't convert a negative floating point number to an
// unsigned integer type.
//
// [0] https://groups.google.com/d/msg/golang-nuts/LH2AO1GAIZE/PyygYRwLAwAJ
//
// TODO(tamird): upstream this.
func checkConvertFloatToUnsigned(j *lint.Job) {
	forAllFiles(j, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		castType, ok := j.Program.Info.TypeOf(call.Fun).(*types.Basic)
		if !ok {
			return true
		}
		if castType.Info()&types.IsUnsigned == 0 {
			return true
		}
		for _, arg := range call.Args {
			argType, ok := j.Program.Info.TypeOf(arg).(*types.Basic)
			if !ok {
				continue
			}
			if argType.Info()&types.IsFloat == 0 {
				continue
			}
			j.Errorf(arg, "do not convert a floating point number to an unsigned integer type")
		}
		return true
	})
}

func walkForStmts(n ast.Node, fn func(s ast.Stmt) bool) bool {
	fr, ok := n.(*ast.ForStmt)
	if !ok {
		return true
	}
	return walkStmts(fr.Body.List, fn)
}

func walkSelectStmts(n ast.Node, fn func(s ast.Stmt) bool) bool {
	sel, ok := n.(*ast.SelectStmt)
	if !ok {
		return true
	}
	return walkStmts(sel.Body.List, fn)
}

func walkStmts(stmts []ast.Stmt, fn func(s ast.Stmt) bool) bool {
	for _, stmt := range stmts {
		if !fn(stmt) {
			return false
		}
	}
	return true
}

// checkSetTimeutilTimerRead assures that timeutil.Timer objects are used
// correctly, to avoid race conditions and deadlocks. These timers require
// callers to set their Read field to true when their channel has been received
// on. If this field is not set and the timer's Reset method is called, we will
// deadlock. This lint assures that the Read field is set in the most common
// case where Reset is used, within a for-loop where each iteration blocks
// on a select statement. The timers are usually used as timeouts on these
// select statements, and need to be reset after each iteration.
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
func checkSetTimeutilTimerRead(j *lint.Job) {
	selectorIsTimer := func(s *ast.SelectorExpr) bool {
		typ := j.Program.Info.TypeOf(s.X).String()
		return strings.HasSuffix(typ, "pkg/util/timeutil.Timer")
	}

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
				if !selectorIsTimer(selector) {
					return true
				}
				selectorName := fmt.Sprint(selector.X)
				if selector.Sel.String() != "C" {
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
					j.Errorf(comm, "must set timer.Read = true after reading from timer.C (see timeutil/timer.go)")
				}
				return true
			})
		})
	})
}

func main() {
	unusedChecker := unused.NewChecker(unused.CheckAll)
	unusedChecker.WholeProgram = true
	meta := metaChecker{
		checkers: []lint.Checker{
			simple.NewChecker(),
			staticcheck.NewChecker(),
			unused.NewLintChecker(unusedChecker),
		},
	}
	lintutil.ProcessArgs("metacheck", &meta, os.Args[1:])
}
