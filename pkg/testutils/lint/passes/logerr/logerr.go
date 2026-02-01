// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package logerr defines an Analyzer that detects log-and-swallow patterns
// where an error is logged but not returned or propagated.
package logerr

import (
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Doc documents this pass.
const Doc = `check for errors that are logged but not returned

This analyzer detects patterns like:

  if err != nil {
      log.Errorf(ctx, "failed: %v", err)
      // no return - function continues
  }
  return result, nil

When an error is detected and logged but the function continues execution
and returns nil for the error, data loss or corruption can occur silently.`

const name = "logerr"

var errorType = types.Universe.Lookup("error").Type()

// Analyzer is a linter that detects errors that are logged but not returned.
var Analyzer = &analysis.Analyzer{
	Name:     name,
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// Find all if statements
	inspect.Preorder([]ast.Node{(*ast.IfStmt)(nil)}, func(n ast.Node) {
		ifStmt := n.(*ast.IfStmt)

		// Check if condition is an error != nil check
		errObj, ok := isNonNilErrCheck(pass, ifStmt.Cond)
		if !ok {
			return
		}

		// Check if the body contains a logging call
		if !containsLogCall(ifStmt.Body) {
			return
		}

		// Check if the body has a return statement
		if hasReturnStatement(ifStmt.Body) {
			return
		}

		// Check if error is assigned to a variable or named return
		if assignsError(pass, ifStmt.Body, errObj) {
			return
		}

		// Check if this is in a defer (handled by defererr)
		// Skip if in defer block
		if isInDeferBlock(pass, n) {
			return
		}

		// Check for nolint comment
		if passesutil.HasNolintComment(pass, ifStmt, name) {
			return
		}

		pass.Report(analysis.Diagnostic{
			Pos: ifStmt.Pos(),
			Message: "error is logged but not returned; " +
				"function continues execution which may cause data loss. " +
				"Add \"//nolint:logerr\" to suppress if this is intentional.",
		})
	})

	return nil, nil
}

// isNonNilErrCheck checks if an expression is checking an error for non-nil.
func isNonNilErrCheck(pass *analysis.Pass, expr ast.Expr) (types.Object, bool) {
	binaryExpr, ok := expr.(*ast.BinaryExpr)
	if !ok {
		return nil, false
	}

	switch binaryExpr.Op {
	case token.NEQ:
		// Check for err != nil or nil != err
		if isErrorType(pass, binaryExpr.X) && isNilIdent(pass, binaryExpr.Y) {
			if id, ok := binaryExpr.X.(*ast.Ident); ok {
				return pass.TypesInfo.Uses[id], true
			}
		}
		if isErrorType(pass, binaryExpr.Y) && isNilIdent(pass, binaryExpr.X) {
			if id, ok := binaryExpr.Y.(*ast.Ident); ok {
				return pass.TypesInfo.Uses[id], true
			}
		}
	case token.LAND, token.LOR:
		if obj, ok := isNonNilErrCheck(pass, binaryExpr.X); ok {
			return obj, true
		}
		return isNonNilErrCheck(pass, binaryExpr.Y)
	}

	return nil, false
}

func isErrorType(pass *analysis.Pass, expr ast.Expr) bool {
	t := pass.TypesInfo.TypeOf(expr)
	return t != nil && types.Identical(t, errorType)
}

func isNilIdent(pass *analysis.Pass, expr ast.Expr) bool {
	return pass.TypesInfo.Types[expr].IsNil()
}

// containsLogCall checks if the block contains a logging function call.
func containsLogCall(block *ast.BlockStmt) bool {
	found := false
	ast.Inspect(block, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if isLogCall(call) {
			found = true
			return false
		}
		return true
	})
	return found
}

// isLogCall checks if a call expression is a logging function.
func isLogCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	methodName := strings.ToLower(sel.Sel.Name)

	logMethods := []string{
		"warning", "warningf", "warn", "warnf",
		"error", "errorf",
		"info", "infof",
		"debug", "debugf",
		"fatal", "fatalf",
		"print", "printf", "println",
		"log", "logf",
		"vwarning", "vwarningf", "verror", "verrorf", "vinfo", "vinfof",
	}

	for _, m := range logMethods {
		if methodName == m {
			return true
		}
	}

	if ident, ok := sel.X.(*ast.Ident); ok {
		pkgName := strings.ToLower(ident.Name)
		if pkgName == "log" || pkgName == "logger" || pkgName == "logging" {
			return true
		}
	}

	return false
}

// hasReturnStatement checks if a block contains any return statement.
func hasReturnStatement(block *ast.BlockStmt) bool {
	found := false
	ast.Inspect(block, func(n ast.Node) bool {
		if found {
			return false
		}
		if _, ok := n.(*ast.ReturnStmt); ok {
			found = true
			return false
		}
		// Don't look inside nested functions
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		return true
	})
	return found
}

// assignsError checks if the error is assigned to another variable.
func assignsError(pass *analysis.Pass, block *ast.BlockStmt, errObj types.Object) bool {
	found := false
	ast.Inspect(block, func(n ast.Node) bool {
		if found {
			return false
		}
		assign, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, rhs := range assign.Rhs {
			if usesObject(pass, rhs, errObj) {
				// Check if LHS is not a blank identifier
				if i < len(assign.Lhs) {
					if ident, ok := assign.Lhs[i].(*ast.Ident); ok && ident.Name != "_" {
						found = true
						return false
					}
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

// isInDeferBlock checks if a node is inside a defer statement.
func isInDeferBlock(pass *analysis.Pass, n ast.Node) bool {
	file := passesutil.FindContainingFile(pass, n)
	pos := n.Pos()

	inDefer := false
	ast.Inspect(file, func(node ast.Node) bool {
		if inDefer {
			return false
		}
		if node == nil {
			return false
		}
		if deferStmt, ok := node.(*ast.DeferStmt); ok {
			// Check if our node is inside this defer
			if pos >= deferStmt.Pos() && pos <= deferStmt.End() {
				inDefer = true
				return false
			}
		}
		return true
	})
	return inDefer
}
