// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package deferloop

import (
	"go/ast"
	"go/token"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
)

const Doc = `check for defer inside loops

The deferloop linter finds all defers that happen inside loops. These defers run
at the end of the enclosing function, not at the end of the loop iteration; this
ia almost never what is intended.

To allow a specific instance, prepend a "//nolint:deferloop" comment.
`

// Analyzer defines this pass.
var Analyzer = &analysis.Analyzer{
	Name: "deferloop",
	Doc:  Doc,
	Run:  run,
}

type visitor struct {
	// insideLoop is true if the nodes that will be visited are inside a loop, up
	// to the function that encloses it.
	insideLoop bool
}

var pass *analysis.Pass
var results []token.Pos

// Memoize the two possible visitors to avoid allocations.
var visitorNotInsideLoop = &visitor{insideLoop: false}
var visitorInsideLoop = &visitor{insideLoop: true}

func (v *visitor) Visit(n ast.Node) ast.Visitor {
	if n == nil {
		return nil
	}
	switch n := n.(type) {
	case *ast.DeferStmt:
		if v.insideLoop && !passesutil.HasNolintComment(pass, n, "deferloop") {
			results = append(results, n.Pos())
		}

	case *ast.RangeStmt, *ast.ForStmt:
		return visitorInsideLoop

	case *ast.FuncLit:
		return visitorNotInsideLoop
	}

	return v
}

func run(p *analysis.Pass) (interface{}, error) {
	pass = p
	for _, file := range pass.Files {
		if strings.HasSuffix(pass.Fset.Position(file.Pos()).Filename, "_test.go") {
			// Skip test files.
			continue
		}
		ast.Walk(visitorNotInsideLoop, file)
	}
	for _, pos := range results {
		p.Reportf(pos, "defer inside loop")
	}
	return nil, nil
}
