// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package returnerrcheck defines an suite of Analyzers that
// detects conditionals which check for a non-nil error and then
// proceed to return a nil error.
package returnerrcheck

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/ast/inspector"
)

// Doc documents this pass.
const Doc = `check for return of nil in a conditional which check for non-nil errors`

var errorType = types.Universe.Lookup("error").Type()

const name = "returnerrcheck"

// Analyzer is a linter that ensures that returns from functions which
// return an error as their last return value which have returns inside
// the body of if conditionals which check for an error to be non-nil do
// not return a nil error without a `//nolint:returnerrcheck` comment.
var Analyzer = &analysis.Analyzer{
	Name:     name,
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run: func(pass *analysis.Pass) (interface{}, error) {
		inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
		inspect.Preorder([]ast.Node{
			(*ast.IfStmt)(nil),
		}, func(n ast.Node) {
			ifStmt := n.(*ast.IfStmt)

			// Are we inside of a function which returns an error?
			containing := findContainingFunc(pass, n)
			if !funcReturnsErrorLast(containing) {
				return
			}

			// Now we want to know if the condition in this statement checks if
			// an error is non-nil.
			errObj, ok := isNonNilErrCheck(pass, ifStmt.Cond)
			if !ok {
				return
			}
			for i, n := range ifStmt.Body.List {
				returnStmt, ok := n.(*ast.ReturnStmt)
				if !ok {
					continue
				}
				if len(returnStmt.Results) == 0 {
					continue
				}
				lastRes := returnStmt.Results[len(returnStmt.Results)-1]
				if !pass.TypesInfo.Types[lastRes].IsNil() {
					continue
				}
				if hasAcceptableAction(pass, errObj, ifStmt.Body.List[:i+1]) {
					continue
				}
				if passesutil.HasNolintComment(pass, returnStmt, name) {
					continue
				}
				pass.Report(analysis.Diagnostic{
					Pos: n.Pos(),
					Message: fmt.Sprintf("unexpected nil error return after checking for a non-nil error" +
						"; if this is not a mistake, add a \"//nolint:returnerrcheck\" comment"),
				})
			}
		})
		return nil, nil
	},
}

// hasAcceptableAction determines if there is some action in statements which
// uses errObj in a way which excuses a nil error return value. Such actions
// include assigning the error to a value, calling Error() on it, type asserting
// it, calling a function with the error as an argument, or returning the error
// in a different return statement.
func hasAcceptableAction(pass *analysis.Pass, errObj types.Object, statements []ast.Stmt) bool {
	var seen bool
	isObj := func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok {
			if seen = pass.TypesInfo.Uses[id] == errObj; seen {
				return true
			}
		}
		return false
	}
	inspect := func(n ast.Node) (wantMore bool) {
		switch n := n.(type) {
		case *ast.CallExpr:
			// If we call a function with the non-nil error object, we probably know
			// what we're doing.
			for _, arg := range n.Args {
				if isObj(arg) {
					return false
				}
			}
		case *ast.AssignStmt:
			// If we assign the error to some value, we probably know what we're
			// doing.
			for _, rhs := range n.Rhs {
				if isObj(rhs) {
					return false
				}
			}
		case *ast.KeyValueExpr:
			// If we assign the error to a field or map in a literal, we probably
			// know what we're doing.
			if isObj(n.Value) {
				return false
			}

		case *ast.SelectorExpr:
			// If we're selecting something off of the error (i.e. err.Error()) then
			// we probably know what we're doing.
			if isObj(n.X) {
				return false
			}
		case *ast.ReturnStmt:
			// If we return the error, perhaps in a different if statement, we
			// might know what we're doing. This is good for cases like:
			//
			//  if cond || err != nil {
			//     if err != nil {
			//         return err
			//     }
			//     ...
			//     return nil
			//  }
			//
			// TODO(ajwerner): ensure that this is returning from the right
			// function or whether this is a good idea.
			if numRes := len(n.Results); numRes > 0 && isObj(n.Results[numRes-1]) {
				return false
			}
		}
		return true
	}
	for _, stmt := range statements {
		if ast.Inspect(stmt, inspect); seen {
			return true
		}
	}
	return false
}

func isNonNilErrCheck(pass *analysis.Pass, expr ast.Expr) (errObj types.Object, ok bool) {
	// TODO(ajwerner): Maybe make this understand !(err == nil) and its variants.
	binaryExpr, ok := expr.(*ast.BinaryExpr)
	if !ok {
		return nil, false
	}
	// We care about cases when errors are idents not when they're fields
	switch binaryExpr.Op {
	case token.NEQ:
		var id *ast.Ident
		if pass.TypesInfo.Types[binaryExpr.X].Type == errorType &&
			pass.TypesInfo.Types[binaryExpr.Y].IsNil() {
			id, ok = binaryExpr.X.(*ast.Ident)
		} else if pass.TypesInfo.Types[binaryExpr.Y].Type == errorType &&
			pass.TypesInfo.Types[binaryExpr.X].IsNil() {
			id, ok = binaryExpr.Y.(*ast.Ident)
		}
		if ok {
			errObj, ok := pass.TypesInfo.Uses[id]
			return errObj, ok
		}
	case token.LAND, token.LOR:
		if errObj, ok := isNonNilErrCheck(pass, binaryExpr.X); ok {
			return errObj, ok
		}
		return isNonNilErrCheck(pass, binaryExpr.Y)
	}
	return nil, false
}

func funcReturnsErrorLast(f *types.Signature) bool {
	results := f.Results()
	return results.Len() > 0 &&
		results.At(results.Len()-1).Type() == errorType
}

func findContainingFunc(pass *analysis.Pass, n ast.Node) *types.Signature {
	stack, _ := astutil.PathEnclosingInterval(passesutil.FindContainingFile(pass, n), n.Pos(), n.End())
	for _, n := range stack {
		if funcDecl, ok := n.(*ast.FuncDecl); ok {
			return pass.TypesInfo.ObjectOf(funcDecl.Name).(*types.Func).Type().(*types.Signature)
		}
		if funcLit, ok := n.(*ast.FuncLit); ok {
			return pass.TypesInfo.Types[funcLit].Type.(*types.Signature)
		}
	}
	return nil
}
