// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package txnerr defines an Analyzer that detects swallowed errors in
// transaction closures, where returning nil allows the transaction to commit
// despite an error occurring.
package txnerr

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
const Doc = `check for swallowed errors in transaction closures

This analyzer detects patterns like:

  return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
      if err := step1(txn); err != nil {
          log.Warning(ctx, "step1 failed: %v", err)
          // BUG: should return err, not continue
      }
      return step2(txn)  // Transaction may commit with partial state
  })

When errors in transaction closures are logged but not returned, the
transaction will commit with potentially corrupted or incomplete state.`

const name = "txnerr"

var errorType = types.Universe.Lookup("error").Type()

// Analyzer is a linter that detects swallowed errors in transaction closures.
var Analyzer = &analysis.Analyzer{
	Name:     name,
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// txnMethodNames are method names that typically accept transaction closures.
var txnMethodNames = []string{
	"Txn", "RunInTxn", "ExecuteInTxn", "WithTxn",
	"InTransaction", "DoTxn", "ExecTxn",
}

func run(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// Find all call expressions
	inspect.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call := n.(*ast.CallExpr)

		// Check if this looks like a transaction method call
		if !isTxnMethodCall(call) {
			return
		}

		// Find function literal arguments (transaction closures)
		for _, arg := range call.Args {
			funcLit, ok := arg.(*ast.FuncLit)
			if !ok {
				continue
			}

			// Check if the function returns error
			if !returnsError(pass, funcLit) {
				continue
			}

			// Check for error swallowing patterns in the closure
			checkTxnClosure(pass, funcLit)
		}
	})

	return nil, nil
}

func isTxnMethodCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	methodName := sel.Sel.Name
	for _, txnMethod := range txnMethodNames {
		if methodName == txnMethod {
			return true
		}
	}
	return false
}

func returnsError(pass *analysis.Pass, funcLit *ast.FuncLit) bool {
	if funcLit.Type.Results == nil || len(funcLit.Type.Results.List) == 0 {
		return false
	}

	// Check last return type
	lastResult := funcLit.Type.Results.List[len(funcLit.Type.Results.List)-1]
	resultType := pass.TypesInfo.TypeOf(lastResult.Type)
	return types.Identical(resultType, errorType)
}

func checkTxnClosure(pass *analysis.Pass, funcLit *ast.FuncLit) {
	if funcLit.Body == nil {
		return
	}

	// Look for if statements with error checks
	ast.Inspect(funcLit.Body, func(n ast.Node) bool {
		ifStmt, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}

		// Check if condition is an error != nil check
		errObj, ok := isNonNilErrCheck(pass, ifStmt.Cond)
		if !ok {
			return true
		}

		// Check if the body logs but doesn't return
		if containsLogCall(ifStmt.Body) && !hasReturnStatement(ifStmt.Body) {
			// Check if error is assigned to something for later use
			if assignsError(pass, ifStmt.Body, errObj) {
				return true
			}

			if passesutil.HasNolintComment(pass, ifStmt, name) {
				return true
			}

			pass.Report(analysis.Diagnostic{
				Pos: ifStmt.Pos(),
				Message: "error in transaction closure is logged but not returned; " +
					"transaction may commit with incomplete state. " +
					"Add \"//nolint:txnerr\" to suppress if this is intentional.",
			})
		}

		return true
	})
}

func isNonNilErrCheck(pass *analysis.Pass, expr ast.Expr) (types.Object, bool) {
	binaryExpr, ok := expr.(*ast.BinaryExpr)
	if !ok {
		return nil, false
	}

	switch binaryExpr.Op {
	case token.NEQ:
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
	}

	for _, m := range logMethods {
		if methodName == m {
			return true
		}
	}

	if ident, ok := sel.X.(*ast.Ident); ok {
		pkgName := strings.ToLower(ident.Name)
		if pkgName == "log" || pkgName == "logger" {
			return true
		}
	}

	return false
}

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
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		return true
	})
	return found
}

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
