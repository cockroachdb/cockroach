// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package defererr defines an Analyzer that detects errors in defer blocks
// that are logged but not propagated, which can cause silent data loss.
package defererr

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
const Doc = `check for errors in defer blocks that are logged but not propagated

This analyzer detects patterns like:

  defer func() {
      if err := sink.Flush(ctx); err != nil {
          log.Warningf(ctx, "failed: %v", err)
      }
  }()

When errors in defer blocks are only logged and not propagated via a named
return value or panic, data loss can occur silently. This pattern was the
root cause of backup compaction data loss issues.`

const name = "defererr"

var errorType = types.Universe.Lookup("error").Type()

// Analyzer is a linter that detects errors in defer blocks that are
// logged but not returned or propagated.
var Analyzer = &analysis.Analyzer{
	Name:     name,
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// Find all defer statements
	inspect.Preorder([]ast.Node{(*ast.DeferStmt)(nil)}, func(n ast.Node) {
		deferStmt := n.(*ast.DeferStmt)

		// We only care about defer func() { ... }() patterns
		callExpr, ok := deferStmt.Call.Fun.(*ast.FuncLit)
		if !ok {
			return
		}

		// Look for if statements inside the defer function body
		for _, stmt := range callExpr.Body.List {
			checkIfStmtForSwallowedError(pass, stmt, callExpr)
		}
	})

	return nil, nil
}

// checkIfStmtForSwallowedError checks if an if statement in a defer block
// swallows an error by logging but not propagating it.
func checkIfStmtForSwallowedError(pass *analysis.Pass, stmt ast.Stmt, funcLit *ast.FuncLit) {
	ifStmt, ok := stmt.(*ast.IfStmt)
	if !ok {
		return
	}

	// Check if condition is an error != nil check
	errObj, ok := isNonNilErrCheck(pass, ifStmt.Cond)
	if !ok {
		return
	}

	// Check if the body contains a logging call
	if !containsLogCall(pass, ifStmt.Body) {
		return
	}

	// Check if the error is properly handled (assigned to named return, panic, etc.)
	if hasProperErrorHandling(pass, ifStmt.Body, errObj, funcLit) {
		return
	}

	// Check for nolint comment
	if passesutil.HasNolintComment(pass, ifStmt, name) {
		return
	}

	pass.Report(analysis.Diagnostic{
		Pos: ifStmt.Pos(),
		Message: "error in defer block is logged but not propagated; " +
			"this can cause silent data loss. Consider assigning to a named " +
			"return value or calling panic. Add \"//nolint:defererr\" to suppress " +
			"if this is intentional.",
	})
}

// isNonNilErrCheck checks if an expression is checking an error for non-nil.
// Returns the error object if so.
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
		// Recurse into && and || expressions
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
func containsLogCall(pass *analysis.Pass, block *ast.BlockStmt) bool {
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
	// Handle selector expressions like log.Warningf, log.Errorf, etc.
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	// Get the method name
	methodName := strings.ToLower(sel.Sel.Name)

	// Common logging method patterns
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

	// Also check for receiver-based patterns like ctx.Log or similar
	if ident, ok := sel.X.(*ast.Ident); ok {
		pkgName := strings.ToLower(ident.Name)
		if pkgName == "log" || pkgName == "logger" || pkgName == "logging" {
			return true
		}
	}

	return false
}

// hasProperErrorHandling checks if the error is properly handled in the block.
func hasProperErrorHandling(
	pass *analysis.Pass, block *ast.BlockStmt, errObj types.Object, funcLit *ast.FuncLit,
) bool {
	// Check for panic calls
	if containsPanic(block) {
		return true
	}

	// Check for assignment to named return value
	if assignsToNamedReturn(pass, block, errObj, funcLit) {
		return true
	}

	// Check for assignment to outer error variable
	if assignsToOuterError(pass, block, errObj) {
		return true
	}

	return false
}

// containsPanic checks if the block contains a panic call.
func containsPanic(block *ast.BlockStmt) bool {
	found := false
	ast.Inspect(block, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "panic" {
			found = true
			return false
		}
		return true
	})
	return found
}

// assignsToNamedReturn checks if the error is assigned to a named return value.
func assignsToNamedReturn(
	pass *analysis.Pass, block *ast.BlockStmt, errObj types.Object, funcLit *ast.FuncLit,
) bool {
	// Get the function's named return values
	namedReturns := make(map[types.Object]bool)
	if funcLit.Type.Results != nil {
		for _, field := range funcLit.Type.Results.List {
			for _, name := range field.Names {
				if obj := pass.TypesInfo.ObjectOf(name); obj != nil {
					namedReturns[obj] = true
				}
			}
		}
	}

	if len(namedReturns) == 0 {
		return false
	}

	// Check if any assignment in the block assigns to a named return
	found := false
	ast.Inspect(block, func(n ast.Node) bool {
		if found {
			return false
		}
		assign, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for _, lhs := range assign.Lhs {
			if ident, ok := lhs.(*ast.Ident); ok {
				if obj := pass.TypesInfo.ObjectOf(ident); obj != nil && namedReturns[obj] {
					// Check if assigning the error or something derived from it
					for _, rhs := range assign.Rhs {
						if usesObject(pass, rhs, errObj) {
							found = true
							return false
						}
					}
				}
			}
		}
		return true
	})
	return found
}

// assignsToOuterError checks if the error is assigned to an outer error variable.
func assignsToOuterError(pass *analysis.Pass, block *ast.BlockStmt, errObj types.Object) bool {
	found := false
	ast.Inspect(block, func(n ast.Node) bool {
		if found {
			return false
		}
		assign, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		// Check if assigning error to something other than a blank identifier
		for i, lhs := range assign.Lhs {
			if ident, ok := lhs.(*ast.Ident); ok && ident.Name != "_" {
				// Check if RHS uses the error object
				if i < len(assign.Rhs) {
					if usesObject(pass, assign.Rhs[i], errObj) {
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
