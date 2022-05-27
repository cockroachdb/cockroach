// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loopvarcapture

import (
	"fmt"
	"go/ast"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	astinspector "golang.org/x/tools/go/ast/inspector"
)

// statementType indicates which type of statement (`go` or `defer`)
// incorrectly captures a loop variable.
type statementType int

const (
	name = "loopvarcapture"

	doc = `check for loop variables captured by reference in Go routines
or defer calls.`

	goCall = statementType(iota)
	deferCall
)

// Analyzer implements this linter, looking for loop variables
// captured by reference in closures called in Go routines
var Analyzer = &analysis.Analyzer{
	Name:     name,
	Doc:      doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// run is the linter entrypoint
func run(pass *analysis.Pass) (interface{}, error) {
	inspector := pass.ResultOf[inspect.Analyzer].(*astinspector.Inspector)
	loops := []ast.Node{
		(*ast.RangeStmt)(nil),
		(*ast.ForStmt)(nil),
	}

	// visit every `for` and `range` loops; when a loop is found,
	// instantiate a new `Visitor` that is reponsible for finding
	// references to loop variables captured by reference in Go
	// routines.
	inspector.Preorder(loops, func(n ast.Node) {
		loop := NewLoop(n)
		if loop.IsEmpty() {
			return
		}

		v := NewVisitor(pass, loop)
		for _, issue := range v.FindCaptures() {
			pass.Report(issue)
		}
	})

	return nil, nil
}

// Visitor implements the logic of checking for use of loop variables
// in Go routines either directly (referencing a loop variable in the
// function literal passed to `go`) or indirectly (calling a local
// function that captures loop variables by reference).
type Visitor struct {
	loop *Loop
	pass *analysis.Pass

	// closures maps a closure assigned to a variable to the
	// captured-by-reference loop variable.
	closures map[*ast.Object]*ast.Ident
	// issues accumulates issues found in a loop
	issues []analysis.Diagnostic
}

// NewVisitor creates a new Visitor instance for the given loop.
func NewVisitor(pass *analysis.Pass, loop *Loop) *Visitor {
	return &Visitor{
		loop:     loop,
		pass:     pass,
		closures: map[*ast.Object]*ast.Ident{},
	}
}

// FindCaptures returns a list of Diagnostic instances to be reported
// to the user
func (v *Visitor) FindCaptures() []analysis.Diagnostic {
	ast.Inspect(v.loop.Body, v.visitLoopBody)
	return v.issues
}

// visitLoopBody ignores everything but `go`, `defer`, and assignment
// statements.
//
// When an assignment to a closure (function literal) is found, we
// check if the closure captures any of the loop variables; in case it
// does, the `closures` map is updated.
//
// When a `go` or `defer` statement is found, we look for closures in
// either the function being called itself, or in parameters in the
// function call.
//
// In other words, both of the following scenarios are problematic and
// reported by this linter:
//
// 1:
//     for k, v := range myMap {
//         // same for `defer`
//         go func() {
//            fmt.Printf("k = %v, v = %v\n", k, v)
//         }()
//     }
//
// 2:
//     for k, v := range myMap {
//         // same for `defer`
//         go doWork(func() {
//             doMoreWork(k, v)
//         })
//     }
//
// If a go (or `defer`) routine calls a previously-defined closure
// that captures a loop variable, that is also reported.
func (v *Visitor) visitLoopBody(n ast.Node) bool {
	switch node := n.(type) {
	case *ast.GoStmt:
		v.visitCallExpr(goCall, node.Call)

	case *ast.DeferStmt:
		v.visitCallExpr(deferCall, node.Call)

	case *ast.AssignStmt:
		for i, rhs := range node.Rhs {
			lhs, ok := node.Lhs[i].(*ast.Ident)
			if !ok || lhs.Obj == nil {
				continue
			}

			// inspect closure's body, looking for captured variables; if
			// found, store the mapping below.
			ast.Inspect(rhs, v.funcLitInspector(func(id *ast.Ident) {
				v.closures[lhs.Obj] = id
			}))
		}

	default:
		return true
	}

	return false
}

// visitCallExpr inspects function calls passed to `go` or `defer`
// staments, looking for closures that capture loop variables by
// reference in the body of the closure or in any of the arguments
// passed to it.
func (v *Visitor) visitCallExpr(stmtType statementType, call *ast.CallExpr) {
	ast.Inspect(call, v.funcLitInspector(func(ident *ast.Ident) {
		v.addIssue(stmtType, ident)
	}))

	if ident, ok := call.Fun.(*ast.Ident); ok {
		if _, ok := v.closures[ident.Obj]; ok {
			v.addIssue(stmtType, ident)
		}
	}
}

// funcLitInspector returns a function that can be passed to
// `ast.Inspect`. When a closure (function literal) that references a
// loop variable is found, the `onInvalidReference` function is called.
func (v *Visitor) funcLitInspector(onInvalidReference func(*ast.Ident)) func(ast.Node) bool {
	return func(n ast.Node) bool {
		funcLit, ok := n.(*ast.FuncLit)
		if !ok {
			return true
		}

		// inspect the closure's body, calling the `onInvalidReference`
		// function when a reference to a loop variable is found
		ast.Inspect(funcLit.Body, v.findLoopVariableReferences(onInvalidReference))
		return false
	}
}

// findLoopVariableReferences inspects a closure's body.  When a
// reference to a loop variable is found, or when a function that is
// known to capture a loop variable by reference is called, the
// `onInvalidReference` function passed is called. The return value of
// this function can be passed to `ast.Inspect`.
func (v *Visitor) findLoopVariableReferences(
	onInvalidReference func(*ast.Ident),
) func(ast.Node) bool {
	return func(n ast.Node) bool {
		switch expr := n.(type) {
		case *ast.Ident:
			if expr.Obj == nil {
				return true
			}

			for _, loopVar := range v.loop.Vars {
				// Comparing the *ast.Object associated with the identifiers
				// frees us from having to keep tracking of shadowing. If the
				// comparison below returns true, it means that the closure
				// directly references a loop variable.
				if expr.Obj == loopVar.Obj {
					onInvalidReference(expr)
					return false
				}
			}
			return false

		case *ast.CallExpr:
			target, ok := expr.Fun.(*ast.Ident)
			if !ok || target.Obj == nil {
				return true
			}

			if _, ok := v.closures[target.Obj]; ok {
				onInvalidReference(target)
				return false
			}

			return true
		}

		return true
	}
}

// addIssue adds a new issue in the `issues` field of the visitor
// associated with the identifier passed. The message is slightly
// different depending on whether the identifier is a loop variable
// directly, or invoking a closure that captures a loop variable by
// reference. In the latter case, the chain of calls that lead to the
// capture is included in the diagnostic. If a `//nolint` comment is
// associated with the use of this identifier, no issue is reported.
func (v *Visitor) addIssue(stmtType statementType, id *ast.Ident) {
	if passesutil.HasNolintComment(v.pass, id, name) {
		return
	}

	var (
		chain        = []*ast.Ident{id}
		currentIdent = id
		ok           bool
	)

	for currentIdent, ok = v.closures[currentIdent.Obj]; ok; currentIdent, ok = v.closures[currentIdent.Obj] {
		chain = append(chain, currentIdent)
	}

	v.issues = append(v.issues, analysis.Diagnostic{
		Pos:     id.Pos(),
		Message: reportMessage(stmtType, chain),
	})
}

// reportMessage constructs the message to be reported to the user
// based on the chain of identifiers that lead to the loop variable
// being captured. The last identifier in the chain is always the loop
// variable being captured; everything else is the chain of closure
// calls that lead to the capture.
func reportMessage(stmtType statementType, chain []*ast.Ident) string {
	var suffixMsg string
	if stmtType == goCall {
		suffixMsg = "often leading to data races"
	} else {
		suffixMsg = "and may hold an undesirable value by the time the deferred function is called"
	}

	if len(chain) == 1 {
		return fmt.Sprintf("loop variable '%s' captured by reference, %s", chain[0].String(), suffixMsg)
	}

	functionName := chain[0]
	loopVar := chain[len(chain)-1]

	var path []string
	for i := 1; i < len(chain)-1; i++ {
		path = append(path, fmt.Sprintf("'%s'", chain[i].String()))
	}

	var pathMsg string
	if len(path) > 0 {
		pathMsg = fmt.Sprintf(" (via %s)", strings.Join(path, " -> "))
	}

	return fmt.Sprintf(
		"'%s' function captures loop variable '%s'%s by reference, %s",
		functionName.String(),
		loopVar.String(),
		pathMsg,
		suffixMsg,
	)
}
