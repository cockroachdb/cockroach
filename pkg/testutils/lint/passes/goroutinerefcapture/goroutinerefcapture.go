// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package goroutinerefcapture

import (
	"fmt"
	"go/ast"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	astinspector "golang.org/x/tools/go/ast/inspector"
)

const (
	name = "goroutinerefcapture"

	doc = `check for loop variables captured by reference in Goroutines,
which often leads to data races.`
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

	// visit every `for` and `range` loops; when a loop is found, we
	// stop traversing (return false) since the logic responsible for
	// checking the body of loops is implemented in the `Visitor` struct
	inspector.Nodes(loops, func(n ast.Node, _ bool) bool {
		loop := NewLoop(n)
		if loop.IsEmpty() {
			return true
		}

		v := NewVisitor(pass, loop)
		for _, issue := range v.FindCaptures() {
			pass.Report(issue)
		}
		return false
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

	// closures keeps a map from local closures that capture loop
	// variables by reference, to the captured loop variable.
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

// visitLoopBody ignores everything but `go` statements and
// assignments.
//
// When an assignment to a function literal is found, we check if the
// closure captures any of the loop variables; in case it does, the
// 'closures' map is updated.
//
// When a `go` statement is found, we look for function literals
// (closures) in either the function being called itself, or in
// parameters in the function call.
//
// In other words, both of the following scenarios are problematic and
// reported by this linter:
//
// 1:
//     for k, v := range myMap {
//         go func() {
//            fmt.Printf("k = %v, v = %v\n", k, v)
//         }()
//     }
//
// 2:
//     for k, v := range myMap {
//         go doWork(func() {
//             doMoreWork(k, v)
//         })
//     }
//
// If a go routine calls a previously-defined closure that captures a
// loop variable, that is also reported.
func (v *Visitor) visitLoopBody(n ast.Node) bool {
	switch stmt := n.(type) {
	case *ast.GoStmt:
		ast.Inspect(stmt.Call, v.funcLitInspector(v.addIssue))
		switch ident := stmt.Call.Fun.(type) {
		case *ast.Ident:
			if _, ok := v.closures[ident.Obj]; ok {
				v.addIssue(ident)
			}
		}

	case *ast.AssignStmt:
		for i, rhs := range stmt.Rhs {
			lhs, ok := stmt.Lhs[i].(*ast.Ident)
			if !ok || lhs.Obj == nil {
				continue
			}

			ast.Inspect(rhs, v.funcLitInspector(func(id *ast.Ident) {
				v.closures[lhs.Obj] = id
			}))
		}

	default:
		return true
	}

	return false
}

// funcLitInspector returns a function that can be passed to
// `ast.Inspect`. When a function literal that references a loop
// variable is found, the `onInvalidReference` function is called.
func (v *Visitor) funcLitInspector(onInvalidReference func(*ast.Ident)) func(ast.Node) bool {
	return func(n ast.Node) bool {
		funcLit, ok := n.(*ast.FuncLit)
		if !ok {
			return true
		}

		ast.Inspect(funcLit.Body, v.findVariableCaptures(onInvalidReference))
		return false
	}
}

// findVariableCaptures returns a function that can be passed to
// `ast.Inspect`. When a reference to a loop variable is found, or
// when a function that is known to capture a loop variable by
// reference is called, the `onInvalidReference` function passed is
// called.
func (v *Visitor) findVariableCaptures(onInvalidReference func(*ast.Ident)) func(ast.Node) bool {
	return func(n ast.Node) bool {
		switch expr := n.(type) {
		case *ast.Ident:
			if expr.Obj == nil {
				return true
			}

			for _, loopVar := range v.loop.Vars {
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
func (v *Visitor) addIssue(id *ast.Ident) {
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
		Message: reportMessage(chain),
	})
}

// reportMessage constructs the message to be reported to the user
// based on the chain of identifiers that lead to the loop variable
// being captured. The last identifier in the chain is always the loop
// variable being captured; everything else is the chain of closure
// calls that lead to the capture.
func reportMessage(chain []*ast.Ident) string {
	if len(chain) == 1 {
		return fmt.Sprintf("loop variable '%s' captured by reference, often leading to data races", chain[0].String())
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
		"'%s' function captures loop variable '%s'%s by reference, often leading to data races",
		functionName.String(),
		loopVar.String(),
		pathMsg,
	)
}
