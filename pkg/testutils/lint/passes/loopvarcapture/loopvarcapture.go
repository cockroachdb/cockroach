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
	"go/token"
	"go/types"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	astinspector "golang.org/x/tools/go/ast/inspector"
	"golang.org/x/tools/go/types/typeutil"
)

type (
	// statementType indicates which type of statement (`go`, `defer`,
	// or `closure`) incorrectly captures a loop variable.
	statementType int

	// Function defines the location of a function (package-level or
	// method on a type).
	Function struct {
		Pkg  string // empty for builtins
		Type string // empty for package-level functions
		Name string
	}

	// GoWrapper represents a function that wraps a call to `go`;
	// generally these structs provide a way for the caller to spawn
	// multiple go routines, wait for all of them, stop them, etc.
	//
	// This linter treats calls to these wrappers as if they were calls
	// to the `go` keyword; in addition, `WaitFuncName`, if any,
	// indicates that the struct also provides a way to wait for the go
	// routine to finish, providing a synchronization mechanism.
	GoWrapper struct {
		Func         Function
		WaitFuncName string
	}

	// GoWrappers is a convenience type so that we can get the list of
	// Go wrapper functions and their corresponding wait functions.
	GoWrappers []GoWrapper
)

const (
	name = "loopvarcapture"

	doc = `check for loop variables captured by reference in Go routines
or defer calls.`

	goCall = statementType(iota)
	deferCall
	closure
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

	// function definitions that wrap `go` calls
	errgroupGo         = Function{Pkg: "golang.org/x/sync/errgroup", Type: "Group", Name: "Go"}
	ctxgroupGo         = Function{Pkg: "github.com/cockroachdb/cockroach/pkg/util/ctxgroup", Type: "Group", Name: "Go"}
	ctxgroupGoCtx      = Function{Pkg: "github.com/cockroachdb/cockroach/pkg/util/ctxgroup", Type: "Group", Name: "GoCtx"}
	monitorGo          = Function{Pkg: "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster", Type: "Monitor", Name: "Go"}
	stopperAsyncTask   = Function{Pkg: "github.com/cockroachdb/cockroach/pkg/util/stop", Type: "Stopper", Name: "RunAsyncTask"}
	stopperAsyncTaskEx = Function{Pkg: "github.com/cockroachdb/cockroach/pkg/util/stop", Type: "Stopper", Name: "RunAsyncTaskEx"}

	// GoRoutineFunctions is a collection of `go` wrappers that are
	// known to take closures as parameters and invoke them
	// asynchronously (in a Go routine). Calling these functions should
	// be equivalent to using the `go` keyword in this linter. In
	// addition, they may optionally include a 'wait' function to wait
	// for the Go routine to finish, providing synchronization.
	GoRoutineFunctions = GoWrappers{
		{Func: errgroupGo, WaitFuncName: "Wait"},
		{Func: ctxgroupGo, WaitFuncName: "Wait"},
		{Func: ctxgroupGoCtx, WaitFuncName: "Wait"},
		{Func: monitorGo, WaitFuncName: "Wait"},
		{Func: stopperAsyncTask},
		{Func: stopperAsyncTaskEx},
	}

	// test-related function locations. We are interested in calls to
	// t.Run() and t.Parallel()
	testRun      = Function{Pkg: "testing", Type: "T", Name: "Run"}
	testParallel = Function{Pkg: "testing", Type: "T", Name: "Parallel"}
)

// Functions returns a list of function definitions for the target
// GoWrappers.
func (gw GoWrappers) Functions() []Function {
	var fs []Function
	for _, f := range gw {
		fs = append(fs, f.Func)
	}

	return fs
}

// WaitFunctions returns a list of function definitions for the go
// wrappers that provide a 'wait' mechanism.
func (gw GoWrappers) WaitFunctions() []Function {
	var fs []Function
	for _, f := range gw {
		if f.WaitFuncName != "" {
			fs = append(fs, Function{Pkg: f.Func.Pkg, Type: f.Func.Type, Name: f.WaitFuncName})
		}
	}

	return fs
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

// suspectReference is a reference to a loop variable that may
// or may not be safe. If any synchronization mechanism is found
// (waitgroups, channels), it is stored in `synchronizationObjs`.
type suspectReference struct {
	// ref is the reference to a loop variable or a closure that
	// captures a loop variable
	ref *ast.Ident
	// stmtPos refers to the position of the statement in a loop
	// where the capture takes place
	stmtPos int
	// synchronizationObjs is a collection of identifiers (waitgroups,
	// channels, etc) that can be used to make wait for Go routines to
	// finish. They can make references to loop variables in Go routines
	// safe.
	synchronizationObjs []*ast.Ident

	// stmtType indicates the type of statement where the reference
	// occurs (go call, defer statement)
	stmtType statementType

	// synchronized indicates that access to the reference is
	// synchronized, making it safe
	synchronized bool
}

// Visitor implements the logic of checking for use of loop variables
// in Go routines either directly (referencing a loop variable in the
// function literal passed to `go`) or indirectly (calling a local
// function that captures loop variables by reference).
type Visitor struct {
	loop *Loop
	pass *analysis.Pass

	// pos is the position of the statement the visitor is currently
	// visiting
	pos int

	// closures maps a closure assigned to a variable to the
	// loop variable that may have been captured by reference.
	closures map[*ast.Object]*suspectReference
	// suspects is a list of suspect references found in a
	// loop. References that are found to not be synchronized are
	// reported to the user.
	suspects []*suspectReference

	// safeDefer indicates whether the visitor is within a closure that
	// is defined in the loop, making references to loop variables in
	// `defer` statements safe. One common example of this situation is
	// following idiom:
	//
	// for _, loopVar := range ... {
	//     func() {
	//         // ...
	//         defer loopVar.Close() // guaranteed to be called in the current iteration
	//        // ...
	//     }()
	// }
	safeDefer bool

	// isTestRun indicates if the visitor is inside a `t.Run()`
	// closure. We encode the semantics of that call in this linter
	// since the pattern of calling t.Run() is one of the most common
	// ones, and not taking it into account can lead to lots of false
	// reports.
	isTestRun bool
	// isTestParallel indicates that the visitor is inside a `t.Run()`
	// test that called `t.Parallel()`. In such cases, synchronization
	// mechanisms in the closure itself should be ignored, since the
	// closure passed is invoked asynchronously
	isTestParallel bool
}

// NewVisitor creates a new Visitor instance for the given loop.
func NewVisitor(pass *analysis.Pass, loop *Loop) *Visitor {
	return &Visitor{
		loop:     loop,
		pass:     pass,
		closures: map[*ast.Object]*suspectReference{},
	}
}

// FindCaptures returns a list of Diagnostic instances to be reported
// to the user
func (v *Visitor) FindCaptures() []analysis.Diagnostic {
	v.runOnStatementBlock(v.loop.Body)

	// at the end of the analysis, a list of suspect references will
	// exist in the visitor's `suspects` field. For each suspect
	// reference, if no identifier in the stack is found to be
	// synchronized, an issue is reported.
	var issues []analysis.Diagnostic
	for _, suspect := range v.suspects {
		stack := v.callStack(suspect)
		var synchronized bool
		for _, ref := range stack {
			if ref.synchronized {
				synchronized = true
				break
			}
		}
		if synchronized {
			continue
		}

		issues = append(issues, analysis.Diagnostic{
			Pos:     suspect.ref.Pos(),
			Message: reportMessage(suspect.stmtType, stack),
		})
	}

	return issues
}

// runOnStatementBlock takes a collection of statements and runs the
// visitor on them, incrementing the internal visitor position
// accordingly.
func (v *Visitor) runOnStatementBlock(block *ast.BlockStmt) {
	for _, stmt := range block.List {
		v.pos++
		ast.Inspect(stmt, v.loopStatementInspector())
	}
}

// loopStatementInspector visits statement at position `pos` in a loop.
//
// When an assignment to a closure (function literal) is found, we
// check if the closure captures any of the loop variables; in case it
// does, the `closures` map is updated.
//
// When a `go` statement, a call to a GoRoutineFunction, or a `defer`
// statement is found, we look for closures in either the function
// being called itself, or in parameters in the function call.
//
// When a reference to a loop variable is found in a closure passed to
// `go` or `defer`, the linter will associate that reference to any objects
// that could later be used to synchronize access to the variable, waiting
// for the Go routine to finish before the next loop iteration. These
// objects are typically WaitGroup (and similar implementations), and
// channels. These are called `synchronization objects`.
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
// In the other hand, the following are *not* reported, because the loop
// waits for the go routines to finish:
//
// 1:
//
//	for k, v := range myMap {
//	    var wg sync.WaitGroup
//	    // same for `defer`, errgroup.Group.Go(), etc
//	    go func() {
//	       defer wg.Done()
//	       fmt.Printf("k = %v, v = %v\n", k, v)
//	    }()
//	    wg.Wait() // loop variable will not change until Go routine is finished
//	}
//
// 2:
//
//	for k, v := range myMap {
//	    ch := make(chan error)
//	    // same for `defer`, errgroup.Group.Go(), etc
//	    go func() {
//	       defer close(ch)
//	       fmt.Printf("k = %v, v = %v\n", k, v)
//	    }()
//	    <-ch // loop variable will not change until Go routine is finished
//	}
//
// If a `go` routine (or `defer`) calls a previously-defined closure
// that captures a loop variable, that is also reported.
func (v *Visitor) loopStatementInspector() func(n ast.Node) bool {
	return func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.GoStmt:
			// if we are in the context of a parallel test, we should ignore
			// `go` AST nodes, as the entire test function is
			// asynchronous. If we eventually hit an identifier node that
			// matches a loop variable, a suspect reference will be created
			if v.isTestParallel {
				return true
			}

			// this checks for calls to functions defined on pointer types
			// on the loop variable, such as:
			//
			// type T struct {}
			//
			// func (t *T) run() { /* ... */ }
			//
			// ts = []*T{...}
			// for _, t := range ts {
			//     go t.run() // `t` will change while the Go routine is running
			// }
			ident := objFromCall(node.Call)
			isInvalid :=
				// this is a method call
				ident != nil &&
					// the identifier is a loop variable
					v.isLoopVar(ident) &&
					// the method is defined on *T (not T)
					v.isPointerReceiver(node.Call) &&
					// the loop is ranging over structs of type T, not *T
					v.rangeOverNonPointer()

			if isInvalid {
				v.maybeAddSuspect(&suspectReference{stmtType: goCall, stmtPos: v.pos, ref: ident})
			}

			v.findLoopVarRefsInCall(goCall, node.Call, false /* go routine wrapper */)
			// no need to keep traversing the AST, the function above is
			// already doing that.
			return false

		case *ast.CallExpr:
			if v.matchFunctionCall(node, GoRoutineFunctions.Functions()) {
				// see comments on `*ast.GotStmt` branch
				if v.isTestParallel {
					return true
				}

				v.findLoopVarRefsInCall(goCall, node, true /* go routine wrapper */)
				// no need to keep traversing the AST, the function above is
				// already doing that.
				return false
			} else if v.matchFunctionCall(node, GoRoutineFunctions.WaitFunctions()) {
				// if this is a call to a go wrapper wait function, extract the
				// identifier object from this call, and mark references guarded
				// by this object as synchronized
				if ident := objFromCall(node); ident != nil {
					v.markSynchronized(ident)
				}
			} else if v.matchFunctionCall(node, []Function{testRun}) {
				// if this is a call to t.Run(), treat the closure passed as
				// argument as if it were the body of the loop itself, since
				// we know that the semantics of t.Run is to execute the
				// closure passed as soon as t.Run() is called.
				if len(node.Args) == 2 {
					if funcLit, ok := node.Args[1].(*ast.FuncLit); ok {
						v.ForkAndRun(func(visitor *Visitor) {
							visitor.isTestRun = true
							visitor.safeDefer = true

							visitor.runOnStatementBlock(funcLit.Body)
						})

						return false
					}
				}
			} else if v.isTestRun && v.matchFunctionCall(node, []Function{testParallel}) {
				// if this is a call to t.Parallel(), update the visitor's
				// state so that synchronization can be dealt with accordingly.
				v.isTestParallel = true
				v.safeDefer = false
			} else if ident := v.waitGroupCallee(node, "Wait"); ident != nil {
				// if this is a call to Done() on a WaitGroup variable, mark
				// references that are associated with that wait group as
				// synchronized
				v.markSynchronized(ident)
			}

		case *ast.FuncLit:
			// when a function literal is found in the body of the loop (i.e.,
			// not a part of a `defer` or `go` statements), visit the closure
			// recursively
			v.ForkAndRun(func(visitor *Visitor) {
				visitor.safeDefer = true
				ast.Inspect(node.Body, visitor.loopStatementInspector())
			})
			// no need to keep traversing the AST using this visitor, as the
			// previous function is doing that.
			return false

		case *ast.DeferStmt:
			if !v.safeDefer {
				v.findLoopVarRefsInCall(deferCall, node.Call, false /* go routine wrapper */)
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
				if funcLit, ok := rhs.(*ast.FuncLit); ok {
					for _, suspect := range v.funcLitSuspectRefs(closure, funcLit) {
						v.closures[lhs.Obj] = suspect
					}
				}
			}

		case *ast.UnaryExpr:
			// if the loop body is reading from a channel, mark all loop variable
			// references that write to this channel (or close it) as synchronized
			if ident, ok := node.X.(*ast.Ident); ok && node.Op == token.ARROW {
				v.markSynchronized(ident)
			}

		case *ast.Ident:
			// if we are in a parallel test and a reference to a loop
			// variable is made, flag it as unsafe
			if v.isTestParallel && v.isLoopVar(node) {
				v.maybeAddSuspect(&suspectReference{stmtType: goCall, stmtPos: v.pos, ref: node})
				return false
			}

		case *ast.RangeStmt:
			// if we are ranging over a channel, mark accesses to loop
			// variables guarded by that channel as synchronized
			if ident, ok := node.X.(*ast.Ident); ok {
				if _, isChan := v.pass.TypesInfo.TypeOf(ident).Underlying().(*types.Chan); isChan {
					v.markSynchronized(ident)
				}
			}
		}

		// if the node is none of the above or if there the subtree needs to
		// be traverse, keep going
		return true
	}
}

// findLoopVarRefsInCall inspects function calls passed to `go` (or
// GoRoutineFunctions) or `defer` staments, looking for closures that
// capture loop variables by reference in the body of the closure or
// in any of the arguments passed to it. Any references are saved the
// visitor's `suspects` field.
func (v *Visitor) findLoopVarRefsInCall(
	stmtType statementType, call *ast.CallExpr, isWrapper bool,
) {
	var wrapperIdent *ast.Ident
	if isWrapper {
		wrapperIdent = objFromCall(call)
	}

	// add suspect is a convenience function called in the ast.Inspect
	// call below; other than calling the appropriate function in the
	// visitor, it also ensures the suspect has the right
	// synchronization objects attached to it.
	addSuspect := func(suspect *suspectReference) {
		if v.isTestParallel {
			// if this is a test case that called `t.Parallel()`, adding
			// synchronization mechanisms is not a reliable way to ensure
			// the loop variable will not change in the closure passed to
			// `t.Run()`, so we ignore any synchronization found in the body
			// of the closure
			suspect.synchronizationObjs = nil
		} else if wrapperIdent != nil {
			// we add the go routine wrapper object to the list of
			// synchronization objects if this function is being called in
			// the context of a Go routine wrapper.
			suspect.synchronizationObjs = append(suspect.synchronizationObjs, wrapperIdent)
		}
		v.maybeAddSuspect(suspect)
	}

	// inspect the call itself
	ast.Inspect(call, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.FuncLit:
			// when a function literal is found, traverse it, and add any
			// suspect references found in the body of the closure
			for _, suspect := range v.funcLitSuspectRefs(stmtType, node) {
				addSuspect(suspect)
			}

			// the function above already traverse the closure tree; we can
			// stop traversal here
			return false

		case *ast.Ident:
			// if this is an identifier (found in some expression in the
			// function being called or in one of the call's arguments),
			// check if it is known to capture a loop variable. If so, add
			// the suspect reference accordingly
			if _, ok := v.closures[node.Obj]; ok {
				addSuspect(&suspectReference{stmtType: stmtType, stmtPos: v.pos, ref: node})
			}

		case *ast.UnaryExpr:
			// check if this is taking the address of a loop variable; that
			// is not safe, and should be reported.
			if ident, ok := node.X.(*ast.Ident); ok && node.Op == token.AND && v.isLoopVar(ident) {
				addSuspect(&suspectReference{stmtType: stmtType, stmtPos: v.pos, ref: ident})
				// return false to stop traversing this subtree; otherwise, we
				// would hit the identifier node and report the same invalid
				// reference twice.
				return false
			}
		}

		// keep traversing AST
		return true
	})
}

// positionedIdent is a wrapper around an *ast.Ident that annotates it
// with a relative position (typically within a closure), and whether
// the identifier was used in the context of a `defer` statement
type positionedIdent struct {
	pos     int
	isDefer bool
	ident   *ast.Ident
}

// visitFuncLit inspects a closure's body. This function returns:
//
//  1. A collection of references to loop variables present in the closure.
//  2. A collection of objects that could be used to wait for the Go
//     routine to finish (synchronization objects). These could be wait
//     groups, channels, etc.
func (v *Visitor) visitFuncLit(funcLit *ast.FuncLit) ([]positionedIdent, []positionedIdent) {
	var refs, syncObjs []positionedIdent

	// define the inspector function so that we can call it recursively
	var inspector func(int, bool, bool) func(ast.Node) bool
	inspector = func(pos int, insideDefer, insideStructLit bool) func(ast.Node) bool {
		positioned := func(ident *ast.Ident, isDefer bool) positionedIdent {
			return positionedIdent{pos: pos, isDefer: isDefer, ident: ident}
		}

		return func(n ast.Node) bool {
			switch expr := n.(type) {
			case *ast.DeferStmt:
				ast.Inspect(expr.Call, inspector(pos, true, insideStructLit))
				return false

			case *ast.CompositeLit:
				// in a composite literal ({a: b, c: d}), check if the value
				// being initialized is a struct. In case it is, set the
				// `insideStructLit` argument accordingly in the recursive
				// call.
				var isStructLit bool
				compositeType := v.pass.TypesInfo.TypeOf(expr.Type)
				if compositeType != nil {
					_, isStructLit = compositeType.Underlying().(*types.Struct)
				}
				for _, elt := range expr.Elts {
					ast.Inspect(elt, inspector(pos, insideDefer, insideStructLit || isStructLit))
				}
				return false

			case *ast.KeyValueExpr:
				// we only validate the key in a KeyValueExpr if we are *not*
				// inside a struct literal initialization. In case we are, we
				// don't want to mistakenly flag a field name as an invalid
				// reference to a loop variable.
				//
				// See https://github.com/golang/go/issues/45160.
				if !insideStructLit {
					ast.Inspect(expr.Key, inspector(pos, insideDefer, insideStructLit))
				}
				ast.Inspect(expr.Value, inspector(pos, insideDefer, insideStructLit))
				return false

			case *ast.Ident:
				if expr.Obj == nil {
					return true
				}

				if v.isLoopVar(expr) {
					refs = append(refs, positioned(expr, insideDefer))
				}

				// `Ident` is a child node; stopping the traversal here
				// shouldn't matter
				return false

			case *ast.CallExpr:
				// if this is a call to Done() on a variable of type WaitGroup,
				// the variable should be considered a synchronization object
				if ident := v.waitGroupCallee(expr, "Done"); ident != nil {
					syncObjs = append(syncObjs, positioned(ident, insideDefer))
				}

				// if we are calling the builtin `close`, the associated channel
				// should be considered a synchronization object
				if ident := v.closeChan(expr); ident != nil {
					syncObjs = append(syncObjs, positioned(ident, insideDefer))
				}

				// if we are calling a local closure that is known to capture a
				// loop variable, mark that as a suspect reference
				funcName, ok := expr.Fun.(*ast.Ident)
				if ok && funcName.Obj != nil {
					if _, ok := v.closures[funcName.Obj]; ok {
						refs = append(refs, positioned(funcName, insideDefer))
					}
				}

				// keep traversing the AST, as there could be invalid references
				// down the subtree
				return true

			case *ast.SendStmt:
				// if we are sending something to a channel, the channel should
				// be considered a synchronization object
				if ident, ok := expr.Chan.(*ast.Ident); ok {
					syncObjs = append(syncObjs, positioned(ident, insideDefer))
				}
			}

			// when the node being visited is not an identifier or a function
			// call, keep traversing the AST
			return true
		}
	}

	for pos, stmt := range funcLit.Body.List {
		ast.Inspect(stmt, inspector(pos, false, false))
	}

	return refs, syncObjs
}

// funcLitSuspectRefs returns a collection of `suspectReference`
// objects found in a closure (function literal)
func (v *Visitor) funcLitSuspectRefs(
	stmtType statementType, funcLit *ast.FuncLit,
) []*suspectReference {
	newSuspect := func(ref *ast.Ident, syncObjs []*ast.Ident) *suspectReference {
		return &suspectReference{stmtPos: v.pos, stmtType: stmtType, ref: ref, synchronizationObjs: syncObjs}
	}

	refs, syncObjs := v.visitFuncLit(funcLit)
	suspects := make([]*suspectReference, 0, len(refs))
	for _, ref := range refs {
		var s []*ast.Ident
		// synchronization objects in defer statements should be
		// ignored, as the semantics of `defer` calls is that the
		// functions will be executed when the function returns,
		// regardless of any synchronization
		if stmtType != deferCall {
			for _, syncObj := range syncObjs {
				// only add the synchronization object if the call happens
				// *after* the loop variable reference, or if it is called in
				// the context of `defer`
				if syncObj.pos >= ref.pos || syncObj.isDefer {
					s = append(s, syncObj.ident)
				}
			}
		}

		suspects = append(suspects, newSuspect(ref.ident, s))
	}

	return suspects
}

// maybeAddSuspect checks if a new suspect reference should be added
// to the visitor's `suspects` field. If a `//nolint` comment is
// associated with the reference, or if the reference happens in a
// `defer` call that is located inside of a local closure, the reference
// is ignored.
func (v *Visitor) maybeAddSuspect(suspect *suspectReference) {
	if passesutil.HasNolintComment(v.pass, suspect.ref, name) ||
		(suspect.stmtType == deferCall && v.safeDefer) {
		return
	}

	v.suspects = append(v.suspects, suspect)
}

// markSynchronized takes an identifier that is considered to be
// providing synchronization for loop variable references (waiting for
// a Go routine), and marks every known suspect associated with that
// object as 'synchronized'. Note that references are only marked
// synchronized if they happened in a statement prior to the visitor's
// current position. References that are marked synchronized are not
// reported to the user at the end of the analysis.
func (v *Visitor) markSynchronized(syncIdent *ast.Ident) {
	synchronizesOn := func(suspect *suspectReference, syncObj *ast.Object) bool {
		for _, suspectSyncIdent := range suspect.synchronizationObjs {
			if suspectSyncIdent.Obj == syncObj {
				return true
			}
		}

		return false
	}

	for _, suspect := range v.suspects {
		if synchronizesOn(suspect, syncIdent.Obj) && suspect.stmtPos < v.pos {
			suspect.synchronized = true
		}
	}

	for _, suspect := range v.closures {
		if synchronizesOn(suspect, syncIdent.Obj) && suspect.stmtPos < v.pos {
			suspect.synchronized = true
		}
	}
}

// reportMessage constructs the message to be reported to the user
// based on the stack of identifiers that lead to the loop variable
// being captured. The last identifier in the stack is always the loop
// variable being captured; everything else is the stack of closure
// calls that lead to the capture.
func reportMessage(stmtType statementType, stack []*suspectReference) string {
	var suffixMsg string
	if stmtType == goCall {
		suffixMsg = "often leading to data races"
	} else {
		suffixMsg = "and may hold an undesirable value by the time the deferred function is called"
	}

	if len(stack) == 1 {
		return fmt.Sprintf("loop variable '%s' captured by reference, %s", stack[0].ref.String(), suffixMsg)
	}

	functionName := stack[0].ref
	loopVar := stack[len(stack)-1].ref

	var path []string
	for i := 1; i < len(stack)-1; i++ {
		path = append(path, fmt.Sprintf("'%s'", stack[i].ref.String()))
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

// ForkAndRun is a convenience function to create a copy of the
// visitor, starting with the caller's state. The copy is passed to
// the function `f`, which is expected to change the state of the
// visitor as desired, and the visit a specific subtree. Once the
// function returns, any suspect references found are merged back to
// the caller.
func (v *Visitor) ForkAndRun(f func(*Visitor)) {
	fork := &Visitor{
		loop:           v.loop,
		pass:           v.pass,
		pos:            v.pos,
		closures:       v.closures,
		safeDefer:      v.safeDefer,
		isTestRun:      v.isTestRun,
		isTestParallel: v.isTestParallel,
	}

	f(fork)

	// merge the `suspects` field back to the calling Visitor
	v.suspects = append(v.suspects, fork.suspects...)
}

// waitGroupCallee checks if the function being called in the given
// CallExpr is on a variable of type `sync.WaitGroup`, with the given
// `funcName`. When it is, the wait group identifier is returned;
// otherwise, the function returns `nil`.
func (v *Visitor) waitGroupCallee(call *ast.CallExpr, funcName string) *ast.Ident {
	function := Function{Pkg: "sync", Type: "WaitGroup", Name: funcName}
	if !v.matchFunctionCall(call, []Function{function}) {
		return nil
	}

	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil
	}

	ident, ok := sel.X.(*ast.Ident)
	if !ok {
		return nil
	}

	return ident
}

// closeChan checks if the function being called in the given CallExpr
// is the `close` builtin. If it is, the channel being closed is
// returned; `nil` is returned otherwise.
func (v *Visitor) closeChan(call *ast.CallExpr) *ast.Ident {
	if len(call.Args) != 1 {
		return nil
	}

	if !v.matchFunctionCall(call, []Function{{Name: "close"}}) {
		return nil
	}

	ident, isIdent := call.Args[0].(*ast.Ident)
	if !isIdent {
		return nil
	}

	return ident
}

// callStack takes in a suspectReference passed as parameter, and
// returns the stack of references that lead to the loop variable;
// this is done by traversing the visitor's `closures` map.
func (v *Visitor) callStack(suspect *suspectReference) []*suspectReference {
	var (
		stack          = []*suspectReference{suspect}
		currentSuspect = suspect
		ok             bool
	)

	for currentSuspect, ok = v.closures[currentSuspect.ref.Obj]; ok; currentSuspect, ok = v.closures[currentSuspect.ref.Obj] {
		stack = append(stack, currentSuspect)
	}

	return stack
}

// matchFunctionCall takes in a CallExpr and a list of Function
// locations, and returns whether the call is being made to any of the
// functions passed.
func (v *Visitor) matchFunctionCall(call *ast.CallExpr, functions []Function) bool {
	callee := typeutil.Callee(v.pass.TypesInfo, call)
	// type conversion (e.g., int(n))
	if callee == nil {
		return false
	}

	pkg := callee.Pkg()
	// call to a builtin
	if pkg == nil {
		ident, isIdent := call.Fun.(*ast.Ident)
		if !isIdent {
			return false
		}

		return matchFunctions("", "", ident.Name, functions)
	}

	calleePkg := pkg.Path()
	calleeFunc := callee.Name()
	calleeObj := ""

	var signature *types.Signature
	switch t := callee.Type().(type) {
	case *types.Signature:
		signature = t
	case *types.Named:
		signature = t.Underlying().(*types.Signature)
	default:
		return false
	}
	recv := signature.Recv()
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

	return matchFunctions(calleePkg, calleeObj, calleeFunc, functions)
}

// isLoopVar checks if the identifier passed is a loop variable
func (v *Visitor) isLoopVar(ident *ast.Ident) bool {
	for _, loopVar := range v.loop.Vars {
		// Comparing the *ast.Object associated with the identifiers
		// frees us from having to keep tracking of shadowing. If the
		// comparison below returns true, it means that the closure
		// directly references a loop variable.
		if ident.Obj == loopVar.Obj {
			return true
		}
	}

	return false
}

// isPointerReceiver returns whether the call expression passed is of
// the form `obj.Fun()`, where `Fun` is defined on pointers on the
// type of `obj`.
//
// In other words, it will return `true` for calls of
//
//	func (t *T) F() { ... }
//
// and `false` for calls of
//
//	func (t T) F() {...}
//
// Note that this function *assumes*, for simplicity, that the call
// expression given is already of the form `obj.Foo()`. Ensuring that
// this assumption is held is the caller's responsibility.
func (v *Visitor) isPointerReceiver(call *ast.CallExpr) bool {
	callee := typeutil.Callee(v.pass.TypesInfo, call)
	// type conversion (e.g., int(n))
	if callee == nil {
		return false
	}

	var signature *types.Signature
	switch t := callee.Type().(type) {
	case *types.Signature:
		signature = t
	case *types.Named:
		signature = t.Underlying().(*types.Signature)
	default:
		return false
	}

	recv := signature.Recv()
	if recv == nil {
		return false
	}

	_, isPointer := recv.Type().(*types.Pointer)
	return isPointer
}

// rangeOverNonPointer returns true if:
//
// - the loop is a `range` statement
// - the expression being iterated a slice of T (not a slice of *T)
func (v *Visitor) rangeOverNonPointer() bool {
	if v.loop.RangeExpr == nil {
		return false
	}

	sliceT, isSlice := v.pass.TypesInfo.TypeOf(v.loop.RangeExpr).Underlying().(*types.Slice)
	if !isSlice {
		return false
	}

	_, isPointer := sliceT.Elem().Underlying().(*types.Pointer)
	return !isPointer
}

func matchFunctions(pkg, obj, name string, functions []Function) bool {
	for _, fun := range functions {
		if fun.Pkg == pkg && fun.Type == obj && fun.Name == name {
			return true
		}
	}

	return false
}

// objFromCall is a convenience function to extract the identifier
// (ast.Ident) object from a call expression.
//
// e.g., if the `call` parameter represents the `obj.Run("foo")` tree,
// this function will return the AST node for `obj`.
//
// It will return `nil` if the call does not fit the pattern above.
func objFromCall(call *ast.CallExpr) *ast.Ident {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil
	}

	ident, ok := selector.X.(*ast.Ident)
	if !ok {
		return nil
	}

	return ident
}
