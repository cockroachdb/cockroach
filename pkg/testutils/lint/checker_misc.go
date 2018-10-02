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
	"go/ast"

	"honnef.co/go/tools/lint"
)

type miscChecker struct{}

func (*miscChecker) Name() string {
	return "misccheck"
}

func (*miscChecker) Prefix() string {
	return "CR"
}

func (*miscChecker) Init(*lint.Program) {}

func (*miscChecker) Funcs() map[string]lint.Func {
	return map[string]lint.Func{
		"FloatToUnsigned": checkConvertFloatToUnsigned,
		"Unconvert":       checkUnconvert,
	}
}

func forAllFiles(j *lint.Job, fn func(node ast.Node) bool) {
	for _, f := range j.Program.Files {
		if !lint.IsGenerated(f) {
			ast.Inspect(f, fn)
		}
	}
}

func walkForStmts(n ast.Node, fn func(ast.Stmt) bool) bool {
	fr, ok := n.(*ast.ForStmt)
	if !ok {
		return true
	}
	return walkStmts(fr.Body.List, fn)
}

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
