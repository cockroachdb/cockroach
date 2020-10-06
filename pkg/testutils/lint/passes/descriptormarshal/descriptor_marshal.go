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
	"go/ast"
	"go/types"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Doc documents this pass.
const Doc = `check for correct unmarshaling of descpb descriptors`

// TODO(ajwerner): write an Analyzer which determines whether a function passes
// a pointer to a struct which contains a descpb.Descriptor to a function
// which will pass that pointer to protoutil.Unmarshal and verify that said
// function also calls Descriptor.Table().

const descpbPkg = "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

const name = "descriptormarshal"

// Analyzer is a linter that ensures there are no calls to
// descpb.Descriptor.GetTable() except where appropriate.
var Analyzer = &analysis.Analyzer{
	Name:     name,
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
			if f.Pkg() == nil || f.Pkg().Path() != descpbPkg || f.Name() != "GetTable" {
				return
			}
			if !isMethodForNamedType(f, "Descriptor") {
				return
			}

			if passesutil.HasNolintComment(pass, sel, name) {
				return
			}
			pass.Report(analysis.Diagnostic{
				Pos:     n.Pos(),
				Message: "Illegal call to Descriptor.GetTable(), see descpb.TableFromDescriptor()",
			})
		})
		return nil, nil
	},
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
