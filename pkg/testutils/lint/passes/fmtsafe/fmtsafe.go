// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fmtsafe

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"github.com/cockroachdb/errors"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
	"golang.org/x/tools/go/types/typeutil"
)

// Doc documents this pass.
var Doc = `checks that log and error functions don't leak PII.

This linter checks the following:

- that the format string in Infof(), Errorf() and similar calls is a
  constant string.

  This check is essential for correctness because format strings
  are assumed to be PII-free and always safe for reporting in
  telemetry or PII-free logs.

- that the message strings in errors.New() and similar calls that
  construct error objects is a constant string.

  This check is provided to encourage hygiene: errors
  constructed using non-constant strings are better constructed using
  a formatting function instead, which makes the construction more
  readable and encourage the provision of PII-free reportable details.

It is possible for a call site *in a test file* to opt the format/message
string out of the linter using /* nolint:fmtsafe */ after the format
argument. This escape hatch is not available in non-test code.
`

// Analyzer defines this pass.
var Analyzer = &analysis.Analyzer{
	Name:     "fmtsafe",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// Our analyzer just wants to see function definitions
	// and call points.
	nodeFilter := []ast.Node{
		(*ast.FuncDecl)(nil),
		(*ast.CallExpr)(nil),
	}

	// fmtOrMsgStr, if non-nil, indicates an incoming
	// format or message string in the argument list of the
	// current function.
	//
	// The pointer is set at the beginning of every function declaration
	// for a function recognized by this linter (= any of those listed
	// in functions.go). In non-recognized function, it is set to nil to
	// indicate there is no known format or message string.
	var fmtOrMsgStr *types.Var
	var enclosingFnName string

	// Now traverse the ASTs. The preorder traversal visits each
	// function declaration node before its body, so we always get to
	// set fmtOrMsgStr before the call points inside the body are
	// visited.
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

		if fd, ok := n.(*ast.FuncDecl); ok {
			// This is the function declaration header. Obtain the formal
			// parameter and the name of the function being defined.
			// We use the name in subsequent error messages to provide
			// more context, and to facilitate the definition
			// of precise exceptions in lint_test.go.
			enclosingFnName, fmtOrMsgStr = maybeGetConstStr(pass, fd)
			return
		}
		// At a call site.
		call := n.(*ast.CallExpr)
		checkCallExpr(pass, enclosingFnName, call, fmtOrMsgStr)
	})
	return nil, nil
}

func maybeGetConstStr(
	pass *analysis.Pass, fd *ast.FuncDecl,
) (enclosingFnName string, res *types.Var) {
	if fd.Body == nil {
		// No body. Since there won't be any callee, take
		// an early return.
		return "", nil
	}

	// What's the function being defined?
	fn := pass.TypesInfo.Defs[fd.Name].(*types.Func)
	if fn == nil {
		return "", nil
	}
	fnName := stripVendor(fn.FullName())

	var wantVariadic bool
	var argIdx int

	if requireConstFmt[fnName] {
		// Expect a variadic function and the format parameter
		// next-to-last in the parameter list.
		wantVariadic = true
		argIdx = -2
	} else if requireConstMsg[fnName] {
		// Expect a non-variadic function and the format parameter last in
		// the parameter list.
		wantVariadic = false
		argIdx = -1
	} else {
		// Not a recognized function. Bail.
		return fn.Name(), nil
	}

	sig := fn.Type().(*types.Signature)
	if sig.Variadic() != wantVariadic {
		panic(errors.Newf("expected variadic %v, got %v", wantVariadic, sig.Variadic()))
	}

	params := sig.Params()
	nparams := params.Len()

	// Is message or format param a string?
	if nparams+argIdx < 0 {
		panic(errors.New("not enough arguments"))
	}
	if p := params.At(nparams + argIdx); p.Type() == types.Typ[types.String] {
		// Found it!
		return fn.Name(), p
	}
	return fn.Name(), nil
}

func checkCallExpr(pass *analysis.Pass, enclosingFnName string, call *ast.CallExpr, fv *types.Var) {
	// What's the function being called?
	cfn := typeutil.Callee(pass.TypesInfo, call)
	if cfn == nil {
		// Not a call to a statically identified function.
		// We can't lint.
		return
	}
	fn, ok := cfn.(*types.Func)
	if !ok {
		// Not a function with a name. We can't lint either.
		return
	}

	// What's the full name of the callee? This includes the package
	// path and, for methods, the type of the supporting object.
	fnName := stripVendor(fn.FullName())

	var wantVariadic bool
	var argIdx int
	var argType string

	// Do the analysis of the callee.
	if requireConstFmt[fnName] {
		// Expect a variadic function and the format parameter
		// next-to-last in the parameter list.
		wantVariadic = true
		argIdx = -2
		argType = "format"
	} else if requireConstMsg[fnName] {
		// Expect a non-variadic function and the format parameter last in
		// the parameter list.
		wantVariadic = false
		argIdx = -1
		argType = "message"
	} else {
		// Not a recognized function. Bail.
		return
	}

	typ := pass.TypesInfo.Types[call.Fun].Type
	if typ == nil {
		panic(errors.New("can't find function type"))
	}

	sig, ok := typ.(*types.Signature)
	if !ok {
		panic(errors.New("can't derive signature"))
	}
	if sig.Variadic() != wantVariadic {
		panic(errors.Newf("expected variadic %v, got %v", wantVariadic, sig.Variadic()))
	}

	idx := sig.Params().Len() + argIdx
	if idx < 0 {
		panic(errors.New("not enough parameters"))
	}

	lit := pass.TypesInfo.Types[call.Args[idx]].Value
	if lit != nil {
		// A literal or constant! All is well.
		return
	}

	// Not a constant. If it's a variable and the variable
	// refers to the incoming format/message from the arg list,
	// tolerate that.
	if fv != nil {
		if id, ok := call.Args[idx].(*ast.Ident); ok {
			if pass.TypesInfo.ObjectOf(id) == fv {
				// Same arg as incoming. All good.
				return
			}
		}
	}

	// If the argument is opting out of the linter with a special
	// comment, tolerate that.
	if hasNoLintComment(pass, call, idx) {
		return
	}

	pass.Reportf(call.Lparen, escNl("%s(): %s argument is not a constant expression"+Tip),
		enclosingFnName, argType)
}

// Tip is exported for use in tests.
var Tip = `
Tip: use YourFuncf("descriptive prefix %%s", ...) or list new formatting wrappers in pkg/testutils/lint/passes/fmtsafe/functions.go.`

func hasNoLintComment(pass *analysis.Pass, call *ast.CallExpr, idx int) bool {
	fPos, f := findContainingFile(pass, call)

	if !strings.HasSuffix(fPos.Name(), "_test.go") {
		// The nolint: escape hatch is only supported in test files.
		return false
	}

	startPos := call.Args[idx].End()
	endPos := call.Rparen
	if idx < len(call.Args)-1 {
		endPos = call.Args[idx+1].Pos()
	}
	for _, cg := range f.Comments {
		if cg.Pos() > endPos || cg.End() < startPos {
			continue
		}
		for _, c := range cg.List {
			if strings.Contains(c.Text, "nolint:fmtsafe") {
				return true
			}
		}
	}
	return false
}

func findContainingFile(pass *analysis.Pass, n ast.Node) (*token.File, *ast.File) {
	fPos := pass.Fset.File(n.Pos())
	for _, f := range pass.Files {
		if pass.Fset.File(f.Pos()) == fPos {
			return fPos, f
		}
	}
	panic(fmt.Errorf("cannot file file for %v", n))
}

func stripVendor(s string) string {
	if i := strings.Index(s, "/vendor/"); i != -1 {
		s = s[i+len("/vendor/"):]
	}
	return s
}

func escNl(msg string) string {
	return strings.ReplaceAll(msg, "\n", "\\n++")
}
