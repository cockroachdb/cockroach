// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package descriptormarshal defines an suite of Analyzers that
// detects correct setting of timestamps when unmarshaling table
// descriptors.
package descriptormarshal

import (
	"fmt"
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/ast/inspector"
)

// Doc documents this pass.
const Doc = `check for correct unmarshaling of sqlbase descriptors`

// TODO(ajwerner): write an Analyzer which determines whether a function passes
// a pointer to a struct which contains a sqlbase.Descriptor to a function
// which will pass that pointer to protoutil.Unmarshal and verify that said
// function also calls Descriptor.Table().

const sqlbasePkg = "github.com/cockroachdb/cockroach/pkg/sql/sqlbase"

// Analyzer is a linter that ensures there are no calls to
// sqlbase.Descriptor.GetTable() except where appropriate.
var Analyzer = &analysis.Analyzer{
	Name:     "descriptormarshal",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run: func(pass *analysis.Pass) (interface{}, error) {
		inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
		inspect.Preorder([]ast.Node{
			(*ast.CallExpr)(nil),
		}, func(n ast.Node) {
			call := n.(*ast.CallExpr)
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return
			}
			obj, ok := pass.TypesInfo.Uses[sel.Sel]
			if !ok {
				return
			}
			f, ok := obj.(*types.Func)
			if !ok {
				return
			}
			if f.Pkg() == nil || f.Pkg().Path() != sqlbasePkg || f.Name() != "GetTable" {
				return
			}
			if !isMethodForNamedType(f, "Descriptor") {
				return
			}
			containing := findContainingFunc(pass, n)
			if isAllowed(containing) {
				return
			}
			pass.Report(analysis.Diagnostic{
				Pos: n.Pos(),
				Message: fmt.Sprintf("Illegal call to Descriptor.GetTable() in %s, see Descriptor.Table()",
					containing.Name()),
			})
		})
		return nil, nil
	},
}

var allowedFunctions = []string{
	"(*github.com/cockroachdb/cockroach/pkg/sql/sqlbase.Descriptor).Table",
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase.TestDefaultExprNil",
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase.TestKeysPerRow",
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl.readBackupManifest",
}

func isAllowed(obj *types.Func) bool {
	str := obj.FullName()
	for _, allowed := range allowedFunctions {
		if allowed == str {
			return true
		}
	}
	return false
}

func findContainingFile(pass *analysis.Pass, n ast.Node) *ast.File {
	fPos := pass.Fset.File(n.Pos())
	for _, f := range pass.Files {
		if pass.Fset.File(f.Pos()) == fPos {
			return f
		}
	}
	panic(fmt.Errorf("cannot file file for %v", n))
}

func findContainingFunc(pass *analysis.Pass, n ast.Node) *types.Func {
	stack, _ := astutil.PathEnclosingInterval(findContainingFile(pass, n), n.Pos(), n.End())
	for i := len(stack) - 1; i >= 0; i-- {
		// If we stumble upon a func decl or func lit then we're in an interesting spot
		funcDecl, ok := stack[i].(*ast.FuncDecl)
		if !ok {
			continue
		}
		return pass.TypesInfo.ObjectOf(funcDecl.Name).(*types.Func)
	}
	return nil
}

func isMethodForNamedType(f *types.Func, name string) bool {
	sig := f.Type().(*types.Signature)
	recv := sig.Recv()
	if recv == nil { // not a method
		return false
	}
	switch recv := recv.Type().(type) {
	case *types.Named:
		return recv.Obj().Name() == name
	case *types.Pointer:
		named, ok := recv.Elem().(*types.Named)
		return ok && named.Obj().Name() == name
	}
	return false
}
