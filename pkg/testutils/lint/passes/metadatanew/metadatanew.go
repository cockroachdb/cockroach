// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package metadatanew provides an Analyzer that flags direct construction
// of metric.Metadata composite literals that are not wrapped in a
// metric.InitMetadata() call. The constructor records the caller's
// source file, which is needed to resolve metric ownership via
// CODEOWNERS at generation time.
package metadatanew

import (
	"go/ast"
	"go/types"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const metricPkgPath = "github.com/cockroachdb/cockroach/pkg/util/metric"

// Analyzer flags metric.Metadata{...} composite literals that are not
// wrapped in a metric.InitMetadata() call.
var Analyzer = &analysis.Analyzer{
	Name:     "metadatanew",
	Doc:      "checks that metric.Metadata is constructed via metric.InitMetadata()",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// Build a set of CompositeLit nodes that are arguments to
	// InitMetadata calls, so we can skip them.
	wrappedLits := make(map[ast.Node]bool)
	insp.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call := n.(*ast.CallExpr)
		if !isInitMetadataCall(call, pass) {
			return
		}
		for _, arg := range call.Args {
			wrappedLits[arg] = true
		}
	})

	insp.Preorder([]ast.Node{
		(*ast.CompositeLit)(nil),
	}, func(n ast.Node) {
		lit := n.(*ast.CompositeLit)
		if lit.Type == nil {
			return
		}
		if !isMetadataType(pass.TypesInfo.TypeOf(lit)) {
			return
		}
		// Allow if this literal is an argument to InitMetadata().
		if wrappedLits[lit] {
			return
		}
		if passesutil.HasNolintComment(pass, n, "metadatanew") {
			return
		}
		pass.Report(analysis.Diagnostic{
			Pos: n.Pos(),
			Message: "direct metric.Metadata{} literal must be wrapped " +
				"in metric.InitMetadata() to record the source file for " +
				"CODEOWNERS resolution",
		})
	})
	return nil, nil
}

// isInitMetadataCall returns true if call is to metric.InitMetadata.
func isInitMetadataCall(call *ast.CallExpr, pass *analysis.Pass) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "InitMetadata" {
		return false
	}
	obj, ok := pass.TypesInfo.Uses[sel.Sel]
	if !ok {
		return false
	}
	fn, ok := obj.(*types.Func)
	if !ok {
		return false
	}
	return fn.Pkg() != nil && fn.Pkg().Path() == metricPkgPath
}

// isMetadataType returns true if t is metric.Metadata (or *metric.Metadata).
func isMetadataType(t types.Type) bool {
	if t == nil {
		return false
	}
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}
	named, ok := t.(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj.Pkg() != nil &&
		obj.Pkg().Path() == metricPkgPath &&
		obj.Name() == "Metadata"
}
