// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package forbiddenmethod provides a general-purpose Analyzer
// factory to vet against calls to a forbidden method.
package forbiddenmethod

import (
	"fmt"
	"go/ast"
	"go/types"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// NB: see the sibling descriptormarshal package for tests.

// Options are used to create an Analyzer that vets against calls
// to a forbidden method.
type Options struct {
	// PassName is the name of the pass. It is also what is used
	// for `// nolint:<PassName>` comments. By convention, this
	// is a single lower-case word.
	PassName string
	// Doc is a short description of the pass.
	Doc string
	// Package, Type, and Method specify the disallowed method.
	// If calls to `(github.com/somewhere/somepkg.Foo).Bar` are
	// disallowed, this is populated as `github.com/somewhere/somepkg`,
	// `Foo`, and `Bar`, respectively.
	//
	// Note that vendoring is handled transparently. The Package should be
	// specified via its ultimate import path, i.e. no "vendor" component.
	Package, Type, Method string
	// Hint is added when the pass emits a warning. The warning already
	// contains forbidden method, so the hint typically concerns itself
	// with guiding the user towards satisfying the lint, for instance
	// by suggesting an alternative method to call instead.
	Hint string
}

// Analyzer returns an Analyzer that vets against calls to the method
// described in the provided Options.
func Analyzer(options Options) *analysis.Analyzer {
	methodRe := regexp.MustCompile(options.Method)
	return &analysis.Analyzer{
		Name:     options.PassName,
		Doc:      options.Doc,
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

				// Set this to true to emit a warning for each function call.
				// This makes it easier to figure out what to set the pkg to when
				// vendoring is in play. (However, see gprcconnclose for an example
				// of what the pkg is in that case).
				const debug = false
				if debug {
					pkgPath := ""
					if f.Pkg() != nil {
						pkgPath = f.Pkg().Path()
					}
					pass.Report(analysis.Diagnostic{
						Pos:     n.Pos(),
						Message: fmt.Sprintf("method call: pkgpath=%s recvtype=%s method=%s", pkgPath, namedRecvTypeFor(f), f.Name()),
					})
				}

				if f.Pkg() == nil || f.Pkg().Path() != options.Package || !methodRe.MatchString(f.Name()) {
					return
				}
				if !isMethodForNamedType(f, options.Type) {
					return
				}

				if passesutil.HasNolintComment(pass, sel, options.PassName) {
					return
				}
				pass.Report(analysis.Diagnostic{
					Pos:     n.Pos(),
					Message: fmt.Sprintf("Illegal call to %s.%s(), %s", options.Type, f.Name(), options.Hint),
				})
			})
			return nil, nil
		},
	}
}

func namedRecvTypeFor(f *types.Func) string {
	sig := f.Type().(*types.Signature)
	recv := sig.Recv()
	if recv == nil { // not a method
		return ""
	}
	switch recv := recv.Type().(type) {
	case *types.Named:
		return recv.Obj().Name()
	case *types.Pointer:
		named, ok := recv.Elem().(*types.Named)
		if !ok {
			return ""
		}
		return named.Obj().Name()
	}
	return ""
}

func isMethodForNamedType(f *types.Func, name string) bool {
	return namedRecvTypeFor(f) == name
}
