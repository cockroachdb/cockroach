// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package unconvert defines an Analyzer that detects unnecessary type
// conversions.
package unconvert

import (
	"go/ast"
	"go/token"
	"go/types"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Doc documents this pass.
const Doc = `check for unnecessary type conversions`

const name = "unconvert"

// Analyzer defines this pass.
var Analyzer = &analysis.Analyzer{
	Name:     name,
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// Adapted from https://github.com/mdempsky/unconvert/blob/beb68d938016d2dec1d1b078054f4d3db25f97be/unconvert.go#L371-L414.
func run(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	nodeFilter := []ast.Node{
		(*ast.CallExpr)(nil),
	}
	inspect.Preorder(nodeFilter, func(n ast.Node) {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return
		}
		if len(call.Args) != 1 || call.Ellipsis != token.NoPos {
			return
		}
		ft, ok := pass.TypesInfo.Types[call.Fun]
		if !ok {
			pass.Reportf(call.Pos(), "missing type")
			return
		}
		if !ft.IsType() {
			// Function call; not a conversion.
			return
		}
		at, ok := pass.TypesInfo.Types[call.Args[0]]
		if !ok {
			pass.Reportf(call.Pos(), "missing type")
			return
		}
		if !types.Identical(ft.Type, at.Type) {
			// A real conversion.
			return
		}
		if isUntypedValue(call.Args[0], pass.TypesInfo) {
			// Workaround golang.org/issue/13061.
			return
		}
		// Adapted from https://github.com/mdempsky/unconvert/blob/beb68d938016d2dec1d1b078054f4d3db25f97be/unconvert.go#L416-L430.
		//
		// cmd/cgo generates explicit type conversions that
		// are often redundant when introducing
		// _cgoCheckPointer calls (issue #16).  Users can't do
		// anything about these, so skip over them.
		if ident, ok := call.Fun.(*ast.Ident); ok {
			if ident.Name == "_cgoCheckPointer" {
				return
			}
		}
		if passesutil.HasNolintComment(pass, call, name) {
			return
		}
		pass.Reportf(call.Pos(), "unnecessary conversion")
	})

	return nil, nil
}

// Cribbed from https://github.com/mdempsky/unconvert/blob/beb68d938016d2dec1d1b078054f4d3db25f97be/unconvert.go#L557-L607.
func isUntypedValue(n ast.Expr, info *types.Info) bool {
	switch n := n.(type) {
	case *ast.BinaryExpr:
		switch n.Op {
		case token.SHL, token.SHR:
			// Shifts yield an untyped value if their LHS is untyped.
			return isUntypedValue(n.X, info)
		case token.EQL, token.NEQ, token.LSS, token.GTR, token.LEQ, token.GEQ:
			// Comparisons yield an untyped boolean value.
			return true
		case token.ADD, token.SUB, token.MUL, token.QUO, token.REM,
			token.AND, token.OR, token.XOR, token.AND_NOT,
			token.LAND, token.LOR:
			return isUntypedValue(n.X, info) && isUntypedValue(n.Y, info)
		}
	case *ast.UnaryExpr:
		switch n.Op {
		case token.ADD, token.SUB, token.NOT, token.XOR:
			return isUntypedValue(n.X, info)
		}
	case *ast.BasicLit:
		// Basic literals are always untyped.
		return true
	case *ast.ParenExpr:
		return isUntypedValue(n.X, info)
	case *ast.SelectorExpr:
		return isUntypedValue(n.Sel, info)
	case *ast.Ident:
		if obj, ok := info.Uses[n]; ok {
			if obj.Pkg() == nil && obj.Name() == "nil" {
				// The universal untyped zero value.
				return true
			}
			if b, ok := obj.Type().(*types.Basic); ok && b.Info()&types.IsUntyped != 0 {
				// Reference to an untyped constant.
				return true
			}
		}
	case *ast.CallExpr:
		if b, ok := asBuiltin(n.Fun, info); ok {
			switch b.Name() {
			case "real", "imag":
				return isUntypedValue(n.Args[0], info)
			case "complex":
				return isUntypedValue(n.Args[0], info) && isUntypedValue(n.Args[1], info)
			}
		}
	}

	return false
}

// Cribbed from https://github.com/mdempsky/unconvert/blob/beb68d938016d2dec1d1b078054f4d3db25f97be/unconvert.go#L609-L630.
func asBuiltin(n ast.Expr, info *types.Info) (*types.Builtin, bool) {
	for {
		paren, ok := n.(*ast.ParenExpr)
		if !ok {
			break
		}
		n = paren.X
	}

	ident, ok := n.(*ast.Ident)
	if !ok {
		return nil, false
	}

	obj, ok := info.Uses[ident]
	if !ok {
		return nil, false
	}

	b, ok := obj.(*types.Builtin)
	return b, ok
}
