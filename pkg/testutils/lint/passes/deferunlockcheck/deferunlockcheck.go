// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package deferunlockcheck

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
	"golang.org/x/tools/go/types/typeutil"
)

const (
	// Doc documents this pass.
	Doc             = "checks that usages of mutex Unlock() are deferred."
	noLintName      = "deferunlock"
	maxLineDistance = 5
)

// Analyzer is an analysis pass that checks for mutex unlocks which
// aren't deferred.
var Analyzer = &analysis.Analyzer{
	Name:     "deferunlockcheck",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// Function defines the location of a function (package-level or
// method on a type).
type Function struct {
	Pkg  string
	Type string // empty for package-level functions
	Name string
}

var mutexFunctions = []Function{
	{Pkg: "github.com/cockroachdb/cockroach/pkg/util/syncutil", Type: "Mutex", Name: "Lock"},
	{Pkg: "github.com/cockroachdb/cockroach/pkg/util/syncutil", Type: "Mutex", Name: "Unlock"},
	{Pkg: "github.com/cockroachdb/cockroach/pkg/util/syncutil", Type: "RWMutex", Name: "Lock"},
	{Pkg: "github.com/cockroachdb/cockroach/pkg/util/syncutil", Type: "RWMutex", Name: "RLock"},
	{Pkg: "github.com/cockroachdb/cockroach/pkg/util/syncutil", Type: "RWMutex", Name: "Unlock"},
	{Pkg: "github.com/cockroachdb/cockroach/pkg/util/syncutil", Type: "RWMutex", Name: "RUnlock"},
	{Pkg: "sync", Type: "Mutex", Name: "Lock"},
	{Pkg: "sync", Type: "Mutex", Name: "Unlock"},
	{Pkg: "sync", Type: "RWMutex", Name: "Lock"},
	{Pkg: "sync", Type: "RWMutex", Name: "Unlock"},
	{Pkg: "sync", Type: "RWMutex", Name: "RLock"},
	{Pkg: "sync", Type: "RWMutex", Name: "RUnlock"},
}

type LockTracker struct {
	body   *ast.BlockStmt
	pass   *analysis.Pass
	locks  []*LockInfo
	issues []analysis.Diagnostic
}

type LockInfo struct {
	// the prefix structure before calling Lock()/Unlock() e.g. s.mu.
	prefix string
	// lineNum tells us where the lock/unlock statement is.
	lineNum int
	// isRead tells us if its a read lock or normal lock
	isRead bool
	// foundUnlockMatch tells us if we found an unlock for this lock. Used to determine
	// if we should report an issue when we find an if condition, loop or function call
	// and haven't found a matching unlock.
	foundUnlockMatch bool
}

func run(pass *analysis.Pass) (interface{}, error) {
	astInspector := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	// Note: defer ...Unlock() expressions are captured as ast.DeferStmt nodes so we don't have to worry about those results returning
	// for ast.ExprStmt nodes.
	filter := []ast.Node{
		(*ast.FuncDecl)(nil),
	}
	astInspector.Preorder(filter, func(n ast.Node) {
		fn := n.(*ast.FuncDecl)
		if fn == nil || fn.Body == nil {
			return
		}
		lockTracker := NewLockTracker(fn.Body, pass)
		for _, issue := range lockTracker.findIssues() {
			pass.Report(issue)
		}
	})

	return nil, nil
}

func NewLockTracker(body *ast.BlockStmt, pass *analysis.Pass) *LockTracker {
	return &LockTracker{
		body:   body,
		pass:   pass,
		locks:  []*LockInfo{},
		issues: []analysis.Diagnostic{},
	}
}

func (lt *LockTracker) findIssues() []analysis.Diagnostic {
	ast.Inspect(lt.body, lt.traverseBody)
	return lt.issues
}

func (lt *LockTracker) traverseBody(n ast.Node) bool {
	switch node := n.(type) {
	// This case is where we find Locks and Unlocks. If we don't find them,
	// This is some other function call and we should see if there are any
	// locks without a matching lock.
	// We also are only done if this happens to be a Lock/Unlock otherwise
	// it could be an inline function and we need to keep going.
	case *ast.CallExpr:
		if lt.isMutexCall(node) {
			return false
		} else if lt.shouldReportNonLinearControlFlow(node) {
			lt.addNonLinearControlFlowIssue(node.Pos(), "function call")

		}
	// Check if this if statement is after a Lock that doesn't have a matching
	// Unlock.
	case *ast.IfStmt:
		if lt.shouldReportNonLinearControlFlow(node) {
			lt.addNonLinearControlFlowIssue(node.Body.Pos(), "if statement")
			return false
		}
	// Check if this loop is after a Lock that doesn't have a matching Unlock.
	case *ast.RangeStmt, *ast.ForStmt:
		if lt.shouldReportNonLinearControlFlow(node) {
			lt.addNonLinearControlFlowIssue(node.Pos(), "for loop")
			return false
		}
	case *ast.DeferStmt:
		// We only want to stop traversal if we found a defer Unlock.
		// If it is a deferred function we want to keep traversing the body
		// in order to see if there is an Unlock inside.
		if lt.isMutexCall(node.Call) {
			return false
		}
		fn, ok := node.Call.Fun.(*ast.FuncLit)
		if !ok {
			return false
		}
		ast.Inspect(fn.Body, lt.traverseBody)
		return false
	}
	// Keep traversing if we didn't find any relevant nodes.
	return true
}

// isMutexCall determines where the given *ast.CallExpr is a Lock/Unlock.
// If it is a Lock/Unlock it will return true but will first:
//   - If it is a Lock it calls addLock
//   - If it is an Unlock it calls maybeReportUnlock.
//
// Otherewise returning false.
func (lt *LockTracker) isMutexCall(call *ast.CallExpr) bool {
	callee, ok := typeutil.Callee(lt.pass.TypesInfo, call).(*types.Func)
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
	for _, fn := range mutexFunctions {
		if fn.Pkg == calleePkg && fn.Type == calleeObj && fn.Name == calleeFunc {
			switch calleeFunc {
			case "Lock":
				lt.addLock(call, false)
			case "RLock":
				lt.addLock(call, true)
			case "Unlock":
				lt.maybeReportUnlock(call, false)
			case "RUnlock":
				lt.maybeReportUnlock(call, true)
			}
			return true
		}
	}

	return false
}

// addLock adds a new LockInfo to the end of the locks slice.
func (lt *LockTracker) addLock(call *ast.CallExpr, isRead bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return
	}
	// We add the other fields when we traverse and reach the *ast.Ident
	lt.locks = append(lt.locks, &LockInfo{
		isRead:           isRead,
		foundUnlockMatch: false,
	})
	ast.Inspect(sel, func(n ast.Node) bool {
		ident, ok := n.(*ast.Ident)
		if !ok {
			return true
		}
		// If we find Lock or RLock set the line and we are done
		// traversing.
		if ident.Name == "Lock" || ident.Name == "RLock" {
			position := lt.pass.Fset.Position(n.Pos())
			lt.locks[len(lt.locks)-1].lineNum = position.Line
			return false
		}
		// Otherwise its part of the prefix that we'll add.
		lt.locks[len(lt.locks)-1].prefix += ident.Name + "."
		// Keep traversing if we didn't find an *ast.Ident node.
		return true
	})
}

// maybeReportUnlock tries to find a matching lock for a given unlock by
// iterating backwards in the locks slice. If one is found, it checks if the
// distance between is greater than maxLineDistance and also has no nolint
// comment and reports on that if both are true.
func (lt *LockTracker) maybeReportUnlock(call *ast.CallExpr, isRead bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return
	}
	var node ast.Node
	var unlockPrefix string
	var unlockLineNum int
	var unlockPos token.Pos
	ast.Inspect(sel, func(n ast.Node) bool {
		ident, ok := n.(*ast.Ident)
		if !ok {
			return true
		}
		// If we find Unlock or RUnlock set the line and we are done
		// traversing.
		if ident.Name == "Unlock" || ident.Name == "RUnlock" {
			position := lt.pass.Fset.Position(n.Pos())
			unlockPos = n.Pos()
			unlockLineNum = position.Line
			node = n
			return false
		}
		// Otherwise its part of the prefix that we'll add.
		unlockPrefix += ident.Name + "."
		// Keep traversing if we didn't find an *ast.Ident node.
		return true
	})
	// Reverse iterate through the locks slice searching for a match.
	for i := len(lt.locks) - 1; i >= 0; i-- {
		lock := lt.locks[i]
		if lock.foundUnlockMatch {
			continue
		}
		// It's only an issue if prefixes match, the lock type matches, distance between is >5 lines away and
		// there is no nolint comment.
		if unlockPrefix == lock.prefix && isRead == lock.isRead {
			lockDistance := unlockLineNum - lock.lineNum
			if lockDistance > maxLineDistance && !passesutil.HasNolintComment(lt.pass, node, noLintName) {
				lt.issues = append(lt.issues, analysis.Diagnostic{
					Pos:     unlockPos,
					Message: fmt.Sprintf("Unlock is >%d lines away from matching Lock, move it to a defer statement after Lock", maxLineDistance),
				})
			}
			// This should set on close enough unlocks, nolint unlocks and defer unlocks.
			lock.foundUnlockMatch = true
		}
	}
}

// shouldReportNonLinearControlFlow checks if locks has a *LockInfo without a matching unlock
// and if the node has a nolint comment near it. If it finds both it returns true.
func (lt *LockTracker) shouldReportNonLinearControlFlow(node ast.Node) bool {
	hasNoLintComment := passesutil.HasNolintComment(lt.pass, node, noLintName)
	if hasNoLintComment {
		return false
	}
	for i := len(lt.locks) - 1; i >= 0; i-- {
		lock := lt.locks[i]
		if !lock.foundUnlockMatch && !hasNoLintComment {
			return true
		}
	}
	return false
}

func (lt *LockTracker) addNonLinearControlFlowIssue(pos token.Pos, stmt string) {
	lt.issues = append(lt.issues, analysis.Diagnostic{
		Pos:     pos,
		Message: fmt.Sprintf("%s between Lock and Unlock may be unsafe, move Unlock to a defer statement after Lock", stmt),
	})
}
