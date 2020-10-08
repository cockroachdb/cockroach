// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package nocopy defines an Analyzer that detects invalid uses of util.NoCopy.
package nocopy

import (
	"go/ast"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Doc documents this pass.
const Doc = `check for invalid uses of util.NoCopy`

// Analyzer defines this pass.
var Analyzer = &analysis.Analyzer{
	Name:     "nocopy",
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

const noCopyType = "github.com/cockroachdb/cockroach/pkg/util.NoCopy"

// nocopy ensures that the util.NoCopy type is not misused. Specifically, it
// ensures that the type is always embedded without a name as the first field in
// a parent struct like:
//
//     type s struct {
//         _ util.NoCopy
//         ...
//     }
//
// We lint against including the type in other positions in structs both for
// uniformity and because it can have runtime performance effects. Specifically,
// if util.NoCopy is included as the last field in a parent struct then it will
// increase the size of the parent struct even though util.NoCopy is zero-sized.
// This is explained in detail in https://github.com/golang/go/issues/9401 and
// is demonstrated in https://play.golang.org/p/jwB2Az5owcm.
func run(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	inspect.Preorder([]ast.Node{
		(*ast.StructType)(nil),
	}, func(n ast.Node) {
		str := n.(*ast.StructType)
		if str.Fields == nil {
			return
		}
		for i, f := range str.Fields.List {
			tv, ok := pass.TypesInfo.Types[f.Type]
			if !ok {
				continue
			}
			if tv.Type.String() != noCopyType {
				continue
			}
			switch {
			case i != 0:
				pass.Reportf(f.Pos(), "Illegal use of util.NoCopy - must be first field in struct")
			case len(f.Names) == 0:
				pass.Reportf(f.Pos(), "Illegal use of util.NoCopy - should not be embedded")
			case len(f.Names) > 1:
				pass.Reportf(f.Pos(), "Illegal use of util.NoCopy - should be included only once")
			case f.Names[0].Name != "_":
				pass.Reportf(f.Pos(), "Illegal use of util.NoCopy - should be unnamed")
			default:
				// Valid use.
			}
		}
	})
	return nil, nil
}
