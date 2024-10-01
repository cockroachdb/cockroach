// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package returncheck defines an Analyzer that detects unused or
// discarded kvpb.Error objects.
package returncheck

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is an analysis.Analyzer that checks for unused or discarded
// kvpb.Error objects from function calls.
var Analyzer = &analysis.Analyzer{
	Name:     "returncheck",
	Doc:      "`returncheck` : `kvpb.Error` :: `errcheck` : (stdlib)`error`",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runAnalyzer,
}

func runAnalyzer(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	inspect.Preorder([]ast.Node{
		(*ast.AssignStmt)(nil),
		(*ast.DeferStmt)(nil),
		(*ast.ExprStmt)(nil),
		(*ast.GoStmt)(nil),
	}, func(n ast.Node) {
		switch stmt := n.(type) {
		case *ast.AssignStmt:
			// Find "_" in the left-hand side of the assigments and check if the corressponding
			// right-hand side expression is a call that returns the target type.
			for i := 0; i < len(stmt.Lhs); i++ {
				if id, ok := stmt.Lhs[i].(*ast.Ident); ok && id.Name == "_" {
					var rhs ast.Expr
					if len(stmt.Rhs) == 1 {
						// ..., stmt.Lhs[i], ... := stmt.Rhs[0]
						rhs = stmt.Rhs[0]
					} else {
						// ..., stmt.Lhs[i], ... := ..., stmt.Rhs[i], ...
						rhs = stmt.Rhs[i]
					}
					if call, ok := rhs.(*ast.CallExpr); ok {
						recordUnchecked(pass, call, i)
					}
				}
			}
		case *ast.ExprStmt:
			if call, ok := stmt.X.(*ast.CallExpr); ok {
				recordUnchecked(pass, call, -1)
			}
		case *ast.GoStmt:
			recordUnchecked(pass, stmt.Call, -1)
		case *ast.DeferStmt:
			recordUnchecked(pass, stmt.Call, -1)
		}
	})
	return nil, nil
}

// recordUnchecked records an error if a given calls has an unchecked
// return. If pos is not a negative value and the call returns a
// tuple, check if the return value at the specified position is of type
// kvpb.Error.
func recordUnchecked(pass *analysis.Pass, call *ast.CallExpr, pos int) {
	isTarget := false
	switch t := pass.TypesInfo.Types[call].Type.(type) {
	case *types.Named:
		isTarget = isTargetType(t)
	case *types.Pointer:
		isTarget = isTargetType(t.Elem())
	case *types.Tuple:
		for i := 0; i < t.Len(); i++ {
			if pos >= 0 && i != pos {
				continue
			}
			switch et := t.At(i).Type().(type) {
			case *types.Named:
				isTarget = isTargetType(et)
			case *types.Pointer:
				isTarget = isTargetType(et.Elem())
			}
		}
	}

	if isTarget {
		pass.Reportf(call.Pos(), "unchecked kvpb.Error value")
	}
}

func isTargetType(t types.Type) bool {
	return t.String() == "github.com/cockroachdb/cockroach/pkg/kv/kvpb.Error"
}
