// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package iteratorerr defines an Analyzer that detects iterator loops
// where the error variable is not checked after the loop terminates.
package iteratorerr

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
const Doc = `check for unchecked errors after iterator loops

This analyzer detects patterns like:

  for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
      // process
  }
  return data, nil  // ERROR: forgot to check err!

When iterating over data with an iterator that returns (bool, error), the
error must be checked after the loop terminates. This pattern was the root
cause of SQL statistics data loss issues.`

const name = "iteratorerr"

var errorType = types.Universe.Lookup("error").Type()

// Analyzer is a linter that detects unchecked errors after iterator loops.
var Analyzer = &analysis.Analyzer{
	Name:     name,
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// Find all function declarations and literals
	inspect.Preorder([]ast.Node{(*ast.FuncDecl)(nil), (*ast.FuncLit)(nil)}, func(n ast.Node) {
		var body *ast.BlockStmt
		switch fn := n.(type) {
		case *ast.FuncDecl:
			if fn.Body == nil {
				return
			}
			body = fn.Body
		case *ast.FuncLit:
			body = fn.Body
		}

		checkFunctionBody(pass, body)
	})

	return nil, nil
}

func checkFunctionBody(pass *analysis.Pass, body *ast.BlockStmt) {
	for i, stmt := range body.List {
		forStmt, ok := stmt.(*ast.ForStmt)
		if !ok {
			continue
		}

		// Check if this looks like an iterator loop
		errObj := findIteratorErrorVar(pass, forStmt)
		if errObj == nil {
			continue
		}

		// Check statements after the for loop
		remainingStmts := body.List[i+1:]
		if !isErrorCheckedInStatements(pass, errObj, remainingStmts) {
			// Find the first return after the loop
			for _, stmt := range remainingStmts {
				if ret, ok := stmt.(*ast.ReturnStmt); ok {
					if passesutil.HasNolintComment(pass, ret, name) {
						continue
					}
					pass.Report(analysis.Diagnostic{
						Pos: ret.Pos(),
						Message: "return after iterator loop without checking error; " +
							"the loop may have terminated due to an error. " +
							"Add \"//nolint:iteratorerr\" to suppress if this is intentional.",
					})
					break
				}
			}
		}
	}
}

// findIteratorErrorVar checks if a for loop looks like an iterator pattern
// and returns the error variable if found.
func findIteratorErrorVar(pass *analysis.Pass, forStmt *ast.ForStmt) types.Object {
	// Check init statement for pattern like: ok, err = it.Next(ctx)
	// or: ok, err := it.Next(ctx)
	if forStmt.Init == nil {
		return nil
	}

	assign, ok := forStmt.Init.(*ast.AssignStmt)
	if !ok {
		return nil
	}

	// Need at least 2 LHS values (ok, err)
	if len(assign.Lhs) < 2 {
		return nil
	}

	// Check if RHS is a method call that looks like an iterator
	if len(assign.Rhs) != 1 {
		return nil
	}

	call, ok := assign.Rhs[0].(*ast.CallExpr)
	if !ok {
		return nil
	}

	// Check for .Next, .Scan, .Read, etc. method calls
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil
	}

	methodName := sel.Sel.Name
	iteratorMethods := []string{"Next", "Scan", "Read", "Step", "Advance", "Fetch"}
	isIteratorMethod := false
	for _, m := range iteratorMethods {
		if methodName == m {
			isIteratorMethod = true
			break
		}
	}
	if !isIteratorMethod {
		return nil
	}

	// Find the error variable (typically last in assignment)
	for i := len(assign.Lhs) - 1; i >= 0; i-- {
		lhs := assign.Lhs[i]
		if ident, ok := lhs.(*ast.Ident); ok && ident.Name != "_" {
			// Check if this is an error type
			if assign.Tok == token.DEFINE {
				// Short variable declaration - check type info
				if obj := pass.TypesInfo.ObjectOf(ident); obj != nil {
					if isErrorTyped(obj.Type()) {
						return obj
					}
				}
			} else {
				// Assignment - look up existing variable
				if obj := pass.TypesInfo.Uses[ident]; obj != nil {
					if isErrorTyped(obj.Type()) {
						return obj
					}
				}
			}
		}
	}

	return nil
}

func isErrorTyped(t types.Type) bool {
	return types.Identical(t, errorType)
}

// isErrorCheckedInStatements checks if the error variable is checked
// (compared to nil or passed to function) in any of the statements.
func isErrorCheckedInStatements(pass *analysis.Pass, errObj types.Object, stmts []ast.Stmt) bool {
	for _, stmt := range stmts {
		if isErrorCheckedInStmt(pass, errObj, stmt) {
			return true
		}
	}
	return false
}

func isErrorCheckedInStmt(pass *analysis.Pass, errObj types.Object, stmt ast.Stmt) bool {
	found := false
	ast.Inspect(stmt, func(n ast.Node) bool {
		if found {
			return false
		}

		switch node := n.(type) {
		case *ast.IfStmt:
			// Check if condition involves the error
			if usesObject(pass, node.Cond, errObj) {
				found = true
				return false
			}
		case *ast.BinaryExpr:
			// Check for err != nil or err == nil
			if node.Op == token.NEQ || node.Op == token.EQL {
				if usesObject(pass, node.X, errObj) || usesObject(pass, node.Y, errObj) {
					found = true
					return false
				}
			}
		case *ast.CallExpr:
			// Check if error is passed to a function
			for _, arg := range node.Args {
				if usesObject(pass, arg, errObj) {
					found = true
					return false
				}
			}
		case *ast.ReturnStmt:
			// Check if error is returned
			for _, result := range node.Results {
				if usesObject(pass, result, errObj) {
					found = true
					return false
				}
			}
		}
		return true
	})
	return found
}

// usesObject checks if an expression uses a particular object.
func usesObject(pass *analysis.Pass, expr ast.Expr, obj types.Object) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		if found {
			return false
		}
		if ident, ok := n.(*ast.Ident); ok {
			if pass.TypesInfo.Uses[ident] == obj {
				found = true
				return false
			}
		}
		return true
	})
	return found
}
