// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// +build lint

package lint

import (
	"go/ast"
	"go/types"
	"log"

	"honnef.co/go/tools/lint"
)

// hashChecker assures that the hash.Hash interface is not misused. A common
// mistake is to assume that the Sum function returns the hash of its input,
// like so:
//
//     hashedBytes := sha256.New().Sum(inputBytes)
//
// In fact, the parameter to Sum is not the bytes to be hashed, but a slice that
// will be used as output in case the caller wants to avoid an allocation. In
// the example above, hashedBytes is not the SHA-256 hash of inputBytes, but a
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
type hashChecker struct {
	hashType *types.Interface
}

func (*hashChecker) Name() string {
	return "hashcheck"
}

func (*hashChecker) Prefix() string {
	return "HC"
}

func (c *hashChecker) Init(program *lint.Program) {
	hashPkg := program.Prog.Package("hash")
	if hashPkg == nil {
		log.Fatal("hash package not found")
	}
	hashIface := hashPkg.Pkg.Scope().Lookup("Hash")
	if hashIface == nil {
		log.Fatal("hash.Hash type not found")
	}
	c.hashType = hashIface.Type().Underlying().(*types.Interface)
}

func (c *hashChecker) Funcs() map[string]lint.Func {
	return map[string]lint.Func{
		"HC1000": c.checkHashWritten,
	}
}

func (c *hashChecker) checkHashWritten(j *lint.Job) {
	stack := make([]ast.Node, 0, 32)
	forAllFiles(j, func(n ast.Node) bool {
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
		if !types.Implements(j.Program.Info.TypeOf(selExpr.X), c.hashType) {
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
			j.Errorf(callExpr, "probable misuse of hash.Hash.Sum: "+
				"provide parameter or use return value, but not both")
		}
		return true
	})
}
