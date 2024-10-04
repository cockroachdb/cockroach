// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loopvarcapture

import (
	"fmt"
	"go/ast"
	"go/types"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	astinspector "golang.org/x/tools/go/ast/inspector"
	"golang.org/x/tools/go/types/typeutil"
)

type (
	// statementType indicates which type of statement (`go` or `defer`)
	// incorrectly captures a loop variable.
	statementType int

	// Function defines the location of a function (package-level or
	// method on a type).
	Function struct {
		Pkg  string
		Type string // empty for package-level functions
		Name string
	}
)

const (
	name = "loopvarcapture"

	doc = `check for loop variables captured by reference in Go routines
or defer calls.`

	goCall = statementType(iota)
	deferCall
)

var (
	// Analyzer implements this linter, looking for loop variables
	// captured by reference in closures called in Go routines
	Analyzer = &analysis.Analyzer{
		Name:     name,
		Doc:      doc,
		Requires: []*analysis.Analyzer{inspect.Analyzer},
		Run:      run,
	}

	// GoRoutineFunctions is a collection of functions that are known to
	// take closures as parameters and invoke them asynchronously (in a
	// Go routine). Calling these functions should be equivalent to
	// using the `go` keyword in this linter.
	GoRoutineFunctions = []Function{
		{Pkg: "golang.org/x/sync/errgroup", Type: "Group", Name: "Go"},
		{Pkg: "github.com/cockroachdb/cockroach/pkg/util/ctxgroup", Type: "Group", Name: "Go"},
		{Pkg: "github.com/cockroachdb/cockroach/pkg/util/ctxgroup", Type: "Group", Name: "GoCtx"},
		{Pkg: "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster", Type: "Monitor", Name: "Go"},
	}
)

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

		v := NewVisitor(pass, loop, false)
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

	// withinClosure indicates whether the visitor is within a closure
	// that is defined in the loop. In this case, we want the linter's
	// behavior to be slightly different. For example, references to
	// loop variables in `defer` calls are safe because they are called
	// in a closure that is called within an interation of the loop. One
	// simple example of this is the following idiom:
	//
	// for _, loopVar := range ... {
	//     func() {
	//         // ...
	//         defer loopVar.Close() // guaranteed to be called in the current iteration
	//        // ...
	//     }()
	// }
	withinClosure bool
}

// NewVisitor creates a new Visitor instance for the given loop.
func NewVisitor(pass *analysis.Pass, loop *Loop, withinClosure bool) *Visitor {
	return &Visitor{
		loop:          loop,
		pass:          pass,
		withinClosure: withinClosure,
		closures:      map[*ast.Object]*ast.Ident{},
	}
}

// FindCaptures returns a list of Diagnostic instances to be reported
// to the user
func (v *Visitor) FindCaptures() []analysis.Diagnostic {
	ast.Inspect(v.loop.Body, v.visitLoopBody)
	return v.issues
}

// visitLoopBody ignores everything but `go` (and GoRoutineFunctions),
// `defer`, and assignment statements.
//
// When an assignment to a closure (function literal) is found, we
// check if the closure captures any of the loop variables; in case it
// does, the `closures` map is updated.
//
// When a `go`, a call to a GoRoutineFunction, or `defer` statement is
// found, we look for closures in either the function being called
// itself, or in parameters in the function call.
//
// In other words, both of the following scenarios are problematic and
// reported by this linter:
//
// 1:
//
//	for k, v := range myMap {
//	    // same for `defer`, errgroup.Group.Go(), etc
//	    go func() {
//	       fmt.Printf("k = %v, v = %v\n", k, v)
//	    }()
//	}
//
// 2:
//
//	for k, v := range myMap {
//	    // same for `defer`, errgroup.Group.Go(), etc
//	    go doWork(func() {
//	        doMoreWork(k, v)
//	    })
//	}
//
// If a `go` routine (or `defer`) calls a previously-defined closure
// that captures a loop variable, that is also reported.
func (v *Visitor) visitLoopBody(n ast.Node) bool {
	switch node := n.(type) {
	case *ast.GoStmt:
		v.findLoopVarRefsInCall(goCall, node.Call)
		// no need to keep traversing the AST, the function above is
		// already doing that.
		return false

	case *ast.CallExpr:
		if v.isGoRoutineFunction(node) {
			v.findLoopVarRefsInCall(goCall, node)
			// no need to keep traversing the AST, the function above is
			// already doing that.
			return false
		}

	case *ast.FuncLit:
		// when a function literal is found in the body of the loop (i.e.,
		// not a part of a `defer` or `go` statements), visit the closure
		// recursively
		v.visitLoopClosure(node)
		// no need to keep traversing the AST using this visitor, as the
		// previous function is doing that.
		return false

	case *ast.DeferStmt:
		if !v.withinClosure {
			v.findLoopVarRefsInCall(deferCall, node.Call)
			// no need to keep traversing the AST, the function above is
			// already doing that.
			return false
		}

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
	}

	// if the node is none of the above or if there the subtree needs to
	// be traverse, keep going
	return true
}

// findLoopVarRefsInCall inspects function calls passed to `go` (or
// GoRoutineFunctions) or `defer` staments, looking for closures that
// capture loop variables by reference in the body of the closure or
// in any of the arguments passed to it. Any references are saved the
// visitor's `issues` field.
func (v *Visitor) findLoopVarRefsInCall(stmtType statementType, call *ast.CallExpr) {
	ast.Inspect(call, v.funcLitInspector(func(ident *ast.Ident) {
		v.addIssue(stmtType, ident)
	}))

	if funcName, ok := call.Fun.(*ast.Ident); ok {
		if _, ok := v.closures[funcName.Obj]; ok {
			v.addIssue(stmtType, funcName)
		}
	}
}

// funcLitInspector returns a function that can be passed to
// `ast.Inspect`. When a closure (function literal) that references a
// loop variable is found, the `onLoopVarCapture` function is called.
func (v *Visitor) funcLitInspector(onLoopVarCapture func(*ast.Ident)) func(ast.Node) bool {
	return func(n ast.Node) bool {
		funcLit, ok := n.(*ast.FuncLit)
		if !ok {
			// not a function literal -- keep traversing the AST
			return true
		}

		// inspect the closure's body, calling the `onLoopVarCapture`
		// function when a reference to a loop variable is found
		ast.Inspect(funcLit.Body, v.findLoopVariableReferences(onLoopVarCapture))
		return false
	}
}

// findLoopVariableReferences inspects a closure's body.  When a
// reference to a loop variable is found, or when a function that is
// known to capture a loop variable by reference is called, the
// `onLoopVarCapture` function passed is called (whether the capture
// is valid or not is determined by the caller). The return value of
// this function can be passed to `ast.Inspect`.
func (v *Visitor) findLoopVariableReferences(
	onLoopVarCapture func(*ast.Ident),
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
					onLoopVarCapture(expr)
					break
				}
			}
			// `Ident` is a child node; stopping the traversal here
			// shouldn't matter
			return false

		case *ast.CallExpr:
			funcName, ok := expr.Fun.(*ast.Ident)
			if ok && funcName.Obj != nil {
				if _, ok := v.closures[funcName.Obj]; ok {
					onLoopVarCapture(funcName)
					return false
				}
			}

			// if the function call is not to a closure that captures a loop
			// variable, keep traversing the AST, as there could be invalid
			// references down the subtree
			return true
		}

		// when the node being visited is not an identifier or a function
		// call, keep traversing the AST
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
	if passesutil.HasNolintComment(v.pass, id, name) ||
		(stmtType == deferCall && v.withinClosure) {
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

// visitLoopClosure traverses the subtree of a function literal
// (closure) present in the body of the loop (outside `go` or `defer`
// statements). A new Visitor instance is created to do the traversal,
// with the `withinClosure` field set to `true`.
func (v *Visitor) visitLoopClosure(closure *ast.FuncLit) {
	closureVisitor := NewVisitor(v.pass, v.loop, true)
	ast.Inspect(closure.Body, closureVisitor.visitLoopBody)

	// merge the `issues` and `closures` field back to the
	// calling Visitor
	v.issues = append(v.issues, closureVisitor.issues...)
	for obj, loopVarObj := range closureVisitor.closures {
		v.closures[obj] = loopVarObj
	}
}

// isGoRoutineFunction takes a call expression node and returns
// whether that call is being made to one of the functions in the
// GoRoutineFunctions slice.
func (v *Visitor) isGoRoutineFunction(call *ast.CallExpr) bool {
	callee, ok := typeutil.Callee(v.pass.TypesInfo, call).(*types.Func)
	if !ok {
		return false
	}
	pkg := callee.Pkg()
	if pkg == nil {
		return false
	}

	calleePkg := pkg.Path()
	calleeFunc := callee.Name()
	calleeObj := ""

	recv := callee.Type().(*types.Signature).Recv()
	if recv != nil {
		// if there is a receiver (i.e., this is a method call), get the
		// name of the type of the receiver
		recvType := recv.Type()
		if pointerType, ok := recvType.(*types.Pointer); ok {
			recvType = pointerType.Elem()
		}
		named, ok := recvType.(*types.Named)
		if !ok {
			return false
		}

		calleeObj = named.Obj().Name()
	}

	for _, goFunc := range GoRoutineFunctions {
		if goFunc.Pkg == calleePkg && goFunc.Type == calleeObj && goFunc.Name == calleeFunc {
			return true
		}
	}

	return false
}
