// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package forbiddenmethod

import (
	"go/ast"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const nakedGoPassName = "nakedgo"

// NakedGoAnalyzer prevents use of the `go` keyword.
var NakedGoAnalyzer = &analysis.Analyzer{
	Name:     nakedGoPassName,
	Doc:      "TODO",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run: func(pass *analysis.Pass) (interface{}, error) {
		inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
		inspect.Preorder([]ast.Node{
			(*ast.GoStmt)(nil),
		}, func(n ast.Node) {
			goStmt := *n.(*ast.GoStmt)
			// TODO(tbg): passesutil.HasNoLintComment doesn't work here, see
			// the test cases.
			if false {
				if passesutil.HasNolintComment(pass, goStmt.Call, nakedGoPassName) {
					return
				}
			}
			pass.Report(analysis.Diagnostic{
				Pos:     n.Pos(),
				Message: "Use of go keyword not allowed, use a Stopper instead",
			})
		})
		return nil, nil
	},
}
