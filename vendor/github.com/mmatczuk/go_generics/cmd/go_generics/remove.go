// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"go/ast"
	"go/token"
)

type typeSet map[string]struct{}

// isTypeOrPointerToType determines if the given AST expression represents a
// type or a pointer to a type that exists in the provided type set.
func isTypeOrPointerToType(set typeSet, expr ast.Expr, starCount int) bool {
	switch e := expr.(type) {
	case *ast.Ident:
		_, ok := set[e.Name]
		return ok
	case *ast.StarExpr:
		if starCount > 1 {
			return false
		}
		return isTypeOrPointerToType(set, e.X, starCount+1)
	case *ast.ParenExpr:
		return isTypeOrPointerToType(set, e.X, starCount)
	default:
		return false
	}
}

// isMethodOf determines if the given function declaration is a method of one
// of the types in the provided type set. To do that, it checks if the function
// has a receiver and that its type is either T or *T, where T is a type that
// exists in the set. This is per the spec:
//
// That parameter section must declare a single parameter, the receiver. Its
// type must be of the form T or *T (possibly using parentheses) where T is a
// type name. The type denoted by T is called the receiver base type; it must
// not be a pointer or interface type and it must be declared in the same
// package as the method.
func isMethodOf(set typeSet, f *ast.FuncDecl) bool {
	// If the function doesn't have exactly one receiver, then it's
	// definitely not a method.
	if f.Recv == nil || len(f.Recv.List) != 1 {
		return false
	}

	return isTypeOrPointerToType(set, f.Recv.List[0].Type, 0)
}

// removeTypeDefinitions removes the definition of all types contained in the
// provided type set.
func removeTypeDefinitions(set typeSet, d *ast.GenDecl) {
	if d.Tok != token.TYPE {
		return
	}

	i := 0
	for _, gs := range d.Specs {
		s := gs.(*ast.TypeSpec)
		if _, ok := set[s.Name.Name]; !ok {
			d.Specs[i] = gs
			i++
		}
	}

	d.Specs = d.Specs[:i]
}

// removeTypes removes from the AST the definition of all types and their
// method sets that are contained in the provided type set.
func removeTypes(set typeSet, f *ast.File) {
	// Go through the top-level declarations.
	i := 0
	for _, decl := range f.Decls {
		keep := true
		switch d := decl.(type) {
		case *ast.GenDecl:
			countBefore := len(d.Specs)
			removeTypeDefinitions(set, d)
			keep = countBefore == 0 || len(d.Specs) > 0
		case *ast.FuncDecl:
			keep = !isMethodOf(set, d)
		}

		if keep {
			f.Decls[i] = decl
			i++
		}
	}

	f.Decls = f.Decls[:i]
}
