// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package errcmp defines an Analyzer which checks
// for usage of errors.Is instead of direct ==/!= comparisons.
package errcmp

import (
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Doc documents this pass.
const Doc = `check for comparison of error objects`

var errorType = types.Universe.Lookup("error").Type()

// Analyzer checks for usage of errors.Is instead of direct ==/!=
// comparisons.
var Analyzer = &analysis.Analyzer{
	Name:     "errcmp",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// Our analyzer just wants to see comparisons and casts.
	nodeFilter := []ast.Node{
		(*ast.BinaryExpr)(nil),
		(*ast.TypeAssertExpr)(nil),
		(*ast.SwitchStmt)(nil),
	}

	// Now traverse the ASTs.
	inspect.Preorder(nodeFilter, func(n ast.Node) {
		// Catch-all for possible bugs in the linter code.
		defer func() {
			if r := recover(); r != nil {
				if err, ok := r.(error); ok {
					pass.Reportf(n.Pos(), "internal linter error: %v", err)
					return
				}
				panic(r)
			}
		}()

		if cmp, ok := n.(*ast.BinaryExpr); ok {
			checkErrCmp(pass, cmp)
			return
		}
		if cmp, ok := n.(*ast.TypeAssertExpr); ok {
			checkErrCast(pass, cmp)
			return
		}
		if cmp, ok := n.(*ast.SwitchStmt); ok {
			checkErrSwitch(pass, cmp)
			return
		}
	})

	return nil, nil
}

func checkErrSwitch(pass *analysis.Pass, s *ast.SwitchStmt) {
	if pass.TypesInfo.Types[s.Tag].Type == errorType {
		pass.Reportf(s.Switch, escNl(`invalid direct comparison of error object
Tip:
   switch err { case errRef:...
-> switch { case errors.Is(err, errRef): ...
`))
	}
}

func checkErrCast(pass *analysis.Pass, texpr *ast.TypeAssertExpr) {
	if pass.TypesInfo.Types[texpr.X].Type == errorType {
		pass.Reportf(texpr.Lparen, escNl(`invalid direct cast on error object
Alternatives:
   if _, ok := err.(*T); ok        ->   if errors.HasType(err, (*T)(nil))
   if _, ok := err.(I); ok         ->   if errors.HasInterface(err, (*I)(nil))
   if myErr, ok := err.(*T); ok    ->   if myErr := (*T)(nil); errors.As(err, &myErr)
   if myErr, ok := err.(I); ok     ->   if myErr := (I)(nil); errors.As(err, &myErr)
   switch err.(type) { case *T:... ->   switch { case errors.HasType(err, (*T)(nil)): ...
`))
	}
}

func isEOFError(e ast.Expr) bool {
	if s, ok := e.(*ast.SelectorExpr); ok {
		if io, ok := s.X.(*ast.Ident); ok && io.Name == "io" && io.Obj == (*ast.Object)(nil) {
			if s.Sel.Name == "EOF" {
				return true
			}
		}
	}
	return false
}

func checkErrCmp(pass *analysis.Pass, binaryExpr *ast.BinaryExpr) {
	switch binaryExpr.Op {
	case token.NEQ, token.EQL:
		if pass.TypesInfo.Types[binaryExpr.X].Type == errorType &&
			!pass.TypesInfo.Types[binaryExpr.Y].IsNil() {
			// We have a special case: when the RHS is io.EOF.
			// This is nearly always used with APIs that return
			// it undecorated.
			if isEOFError(binaryExpr.Y) {
				return
			}

			pass.Reportf(binaryExpr.OpPos, escNl(`use errors.Is instead of a direct comparison
For example:
   if errors.Is(err, errMyOwnErrReference) {
     ...
   }
`))
		}
	}
}

func escNl(msg string) string {
	return strings.ReplaceAll(msg, "\n", "\\n++")
}
