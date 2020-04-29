// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package nakedreturn defines an Analyzer that detects naked returns.
package nakedreturn

import (
	"go/ast"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Doc documents this pass.
const Doc = `check for naked returns`

// Analyzer defines this pass.
var Analyzer = &analysis.Analyzer{
	Name:     "nakedreturn",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	inspect.Preorder([]ast.Node{
		(*ast.ReturnStmt)(nil),
	}, func(n ast.Node) {
		ret := n.(*ast.ReturnStmt)

		fSig := passes.FindContainingFuncSig(pass, ret)
		if fSig.Results().Len() > 0 && len(ret.Results) == 0 {
			pass.Reportf(ret.Pos(), "illegal naked return")
		}
	})
	return nil, nil
}
