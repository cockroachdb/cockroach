// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package leaktestcall defines an Analyzer that detects correct use
// of leaktest.AfterTest(t).
package leaktestcall

import (
	"go/ast"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is an analysis pass that checks that the return value of
// leaktest.AfterFunc(t) is called in defer statements.
var Analyzer = &analysis.Analyzer{
	Name:     "leaktestcall",
	Doc:      "Check that the closure returned by leaktest.AfterFunc(t) is called",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	astInspector := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	filter := []ast.Node{
		(*ast.DeferStmt)(nil),
	}

	astInspector.Preorder(filter, func(n ast.Node) {
		def := n.(*ast.DeferStmt)
		switch funCall := def.Call.Fun.(type) {
		case *ast.SelectorExpr:
			packageIdent, ok := funCall.X.(*ast.Ident)
			if !ok {
				return
			}
			if packageIdent.Name == "leaktest" && funCall.Sel != nil && funCall.Sel.Name == "AfterTest" {
				pass.Reportf(def.Call.Pos(), "leaktest.AfterTest return not called")
			}
		}
	})

	return nil, nil
}
