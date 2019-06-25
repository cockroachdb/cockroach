// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// TODO(radu): re-enable the checkers using the new staticcheck interfaces.
// +build lint_todo

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
