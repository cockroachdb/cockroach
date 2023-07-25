// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package deferunlockcheck

import (
	"go/ast"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Doc documents this pass.
const Doc = "checks that usages of mutex Unlock() are deferred."

const noLintName = "deferunlock"

// Analyzer is an analysis pass that checks for mutex unlocks which
// aren't deferred.
var Analyzer = &analysis.Analyzer{
	Name:     "deferunlockcheck",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	astInspector := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	// Note: defer ...Unlock() expressions are captured as ast.DeferStmt nodes so we don't have to worry about those results returning
	// for ast.ExprStmt nodes.
	filter := []ast.Node{
		(*ast.ExprStmt)(nil),
	}
	astInspector.Preorder(filter, func(n ast.Node) {
		stmt := n.(*ast.ExprStmt)
		expr, ok := stmt.X.(*ast.CallExpr)
		if !ok {
			return
		}
		sel, ok := expr.Fun.(*ast.SelectorExpr)
		if !ok {
			return
		}
		if sel.Sel != nil && (sel.Sel.Name == "Unlock" || sel.Sel.Name == "RUnlock") && !passesutil.HasNolintComment(pass, n, noLintName) {
			pass.Reportf(sel.Pos(), "Mutex %s not deferred", sel.Sel.Name)
		}
	})

	return nil, nil
}
