// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package hash defines an Analyzer that detects correct use of hash.Hash.
package hash

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
)

// Doc documents this pass.
const Doc = `check for correct use of hash.Hash`

// Analyzer defines this pass.
var Analyzer = &analysis.Analyzer{
	Name:     "hash",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// hashChecker assures that the hash.Hash interface is not misused. A common
// mistake is to assume that the Sum function returns the hash of its input,
// like so:
//
//     hashedBytes := sha256.New().Sum(inputBytes)
//
// In fact, the parameter to Sum is not the bytes to be hashed, but a slice that
// will be used as output in case the caller wants to avoid an allocation. In
// the example above, hashedBytes is not the SHA-256 hash of inputBytes, but
// the concatenation of inputBytes with the hash of the empty string.
//
// Correct uses of the hash.Hash interface are as follows:
//
//     h := sha256.New()
//     h.Write(inputBytes)
//     hashedBytes := h.Sum(nil)
//
//     h := sha256.New()
//     h.Write(inputBytes)
//     var hashedBytes [sha256.Size]byte
//     h.Sum(hashedBytes[:0])
//
// To differentiate between correct and incorrect usages, hashChecker applies a
// simple heuristic: it flags calls to Sum where a) the parameter is non-nil and
// b) the return value is used.
//
// The hash.Hash interface may be remedied in Go 2. See golang/go#21070.
func run(pass *analysis.Pass) (interface{}, error) {
	selectorIsHash := func(s *ast.SelectorExpr) bool {
		tv, ok := pass.TypesInfo.Types[s.X]
		if !ok {
			return false
		}
		named, ok := tv.Type.(*types.Named)
		if !ok {
			return false
		}
		if named.Obj().Type().String() != "hash.Hash" {
			return false
		}
		return true
	}

	stack := make([]ast.Node, 0, 32)
	forAllFiles(pass.Files, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1] // pop
			return true
		}
		stack = append(stack, n) // push

		// Find a call to hash.Hash.Sum.
		selExpr, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if selExpr.Sel.Name != "Sum" {
			return true
		}
		if !selectorIsHash(selExpr) {
			return true
		}
		callExpr, ok := stack[len(stack)-2].(*ast.CallExpr)
		if !ok {
			return true
		}
		if len(callExpr.Args) != 1 {
			return true
		}
		// We have a valid call to hash.Hash.Sum.

		// Is the argument nil?
		var nilArg bool
		if id, ok := callExpr.Args[0].(*ast.Ident); ok && id.Name == "nil" {
			nilArg = true
		}

		// Is the return value unused?
		var retUnused bool
	Switch:
		switch t := stack[len(stack)-3].(type) {
		case *ast.AssignStmt:
			for i := range t.Rhs {
				if t.Rhs[i] == stack[len(stack)-2] {
					if id, ok := t.Lhs[i].(*ast.Ident); ok && id.Name == "_" {
						// Assigning to the blank identifier does not count as using the
						// return value.
						retUnused = true
					}
					break Switch
				}
			}
			panic("unreachable")
		case *ast.ExprStmt:
			// An expression statement means the return value is unused.
			retUnused = true
		default:
		}

		if !nilArg && !retUnused {
			pass.Reportf(callExpr.Pos(), "probable misuse of hash.Hash.Sum: "+
				"provide parameter or use return value, but not both")
		}
		return true
	})

	return nil, nil
}

func forAllFiles(files []*ast.File, fn func(node ast.Node) bool) {
	for _, f := range files {
		ast.Inspect(f, fn)
	}
}
