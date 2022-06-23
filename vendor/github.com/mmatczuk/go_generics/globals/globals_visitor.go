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

// Package globals provides an AST visitor that calls the visit function for all
// global identifiers.
package globals

import (
	"fmt"

	"go/ast"
	"go/token"
	"path/filepath"
	"strconv"
)

// globalsVisitor holds the state used while traversing the nodes of a file in
// search of globals.
//
// The visitor does two passes on the global declarations: the first one adds
// all globals to the global scope (since Go allows references to globals that
// haven't been declared yet), and the second one calls f() for the definition
// and uses of globals found in the first pass.
//
// The implementation correctly handles cases when globals are aliased by
// locals; in such cases, f() is not called.
type globalsVisitor struct {
	// file is the file whose nodes are being visited.
	file *ast.File

	// fset is the file set the file being visited belongs to.
	fset *token.FileSet

	// f is the visit function to be called when a global symbol is reached.
	f func(*ast.Ident, SymKind)

	// scope is the current scope as nodes are visited.
	scope *scope
}

// unexpected is called when an unexpected node appears in the AST. It dumps
// the location of the associated token and panics because this should only
// happen when there is a bug in the traversal code.
func (v *globalsVisitor) unexpected(p token.Pos) {
	panic(fmt.Sprintf("Unable to parse at %v", v.fset.Position(p)))
}

// pushScope creates a new scope and pushes it to the top of the scope stack.
func (v *globalsVisitor) pushScope() {
	v.scope = newScope(v.scope)
}

// popScope removes the scope created by the last call to pushScope.
func (v *globalsVisitor) popScope() {
	v.scope = v.scope.outer
}

// visitType is called when an expression is known to be a type, for example,
// on the first argument of make(). It visits all children nodes and reports
// any globals.
func (v *globalsVisitor) visitType(ge ast.Expr) {
	switch e := ge.(type) {
	case *ast.Ident:
		if s := v.scope.deepLookup(e.Name); s != nil && s.scope.isGlobal() {
			v.f(e, s.kind)
		}

	case *ast.SelectorExpr:
		id := GetIdent(e.X)
		if id == nil {
			v.unexpected(e.X.Pos())
		}

	case *ast.StarExpr:
		v.visitType(e.X)
	case *ast.ParenExpr:
		v.visitType(e.X)
	case *ast.ChanType:
		v.visitType(e.Value)
	case *ast.Ellipsis:
		v.visitType(e.Elt)
	case *ast.ArrayType:
		v.visitExpr(e.Len)
		v.visitType(e.Elt)
	case *ast.MapType:
		v.visitType(e.Key)
		v.visitType(e.Value)
	case *ast.StructType:
		v.visitFields(e.Fields, KindUnknown)
	case *ast.FuncType:
		v.visitFields(e.Params, KindUnknown)
		v.visitFields(e.Results, KindUnknown)
	case *ast.InterfaceType:
		v.visitFields(e.Methods, KindUnknown)
	default:
		v.unexpected(ge.Pos())
	}
}

// visitFields visits all fields, and add symbols if kind isn't KindUnknown.
func (v *globalsVisitor) visitFields(l *ast.FieldList, kind SymKind) {
	if l == nil {
		return
	}

	for _, f := range l.List {
		if kind != KindUnknown {
			for _, n := range f.Names {
				v.scope.add(n.Name, kind, n.Pos())
			}
		}
		v.visitType(f.Type)
		if f.Tag != nil {
			tag := ast.NewIdent(f.Tag.Value)
			v.f(tag, KindTag)
			// Replace the tag if updated.
			if tag.Name != f.Tag.Value {
				f.Tag.Value = tag.Name
			}
		}
	}
}

// visitGenDecl is called when a generic declation is encountered, for example,
// on variable, constant and type declarations. It adds all newly defined
// symbols to the current scope and reports them if the current scope is the
// global one.
func (v *globalsVisitor) visitGenDecl(d *ast.GenDecl) {
	switch d.Tok {
	case token.IMPORT:
	case token.TYPE:
		for _, gs := range d.Specs {
			s := gs.(*ast.TypeSpec)
			v.scope.add(s.Name.Name, KindType, s.Name.Pos())
			if v.scope.isGlobal() {
				v.f(s.Name, KindType)
			}
			v.visitType(s.Type)
		}
	case token.CONST, token.VAR:
		kind := KindConst
		if d.Tok == token.VAR {
			kind = KindVar
		}

		for _, gs := range d.Specs {
			s := gs.(*ast.ValueSpec)
			if s.Type != nil {
				v.visitType(s.Type)
			}

			for _, e := range s.Values {
				v.visitExpr(e)
			}

			for _, n := range s.Names {
				if v.scope.isGlobal() {
					v.f(n, kind)
				}
				v.scope.add(n.Name, kind, n.Pos())
			}
		}
	default:
		v.unexpected(d.Pos())
	}
}

// isViableType determines if the given expression is a viable type expression,
// that is, if it could be interpreted as a type, for example, sync.Mutex,
// myType, func(int)int, as opposed to -1, 2 * 2, a + b, etc.
func (v *globalsVisitor) isViableType(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.Ident:
		// This covers the plain identifier case. When we see it, we
		// have to check if it resolves to a type; if the symbol is not
		// known, we'll claim it's viable as a type.
		s := v.scope.deepLookup(e.Name)
		return s == nil || s.kind == KindType

	case *ast.ChanType, *ast.ArrayType, *ast.MapType, *ast.StructType, *ast.FuncType, *ast.InterfaceType, *ast.Ellipsis:
		// This covers the following cases:
		// 1. ChanType:
		//   chan T
		//   <-chan T
		//   chan<- T
		// 2. ArrayType:
		//   [Expr]T
		// 3. MapType:
		//   map[T]U
		// 4. StructType:
		//   struct { Fields }
		// 5. FuncType:
		//   func(Fields)Returns
		// 6. Interface:
		//   interface { Fields }
		// 7. Ellipsis:
		//   ...T
		return true

	case *ast.SelectorExpr:
		// The only case in which an expression involving a selector can
		// be a type is if it has the following form X.T, where X is an
		// import, and T is a type exported by X.
		//
		// There's no way to know whether T is a type because we don't
		// parse imports. So we just claim that this is a viable type;
		// it doesn't affect the general result because we don't visit
		// imported symbols.
		id := GetIdent(e.X)
		if id == nil {
			return false
		}

		s := v.scope.deepLookup(id.Name)
		return s != nil && s.kind == KindImport

	case *ast.StarExpr:
		// This covers the *T case. The expression is a viable type if
		// T is.
		return v.isViableType(e.X)

	case *ast.ParenExpr:
		// This covers the (T) case. The expression is a viable type if
		// T is.
		return v.isViableType(e.X)

	default:
		return false
	}
}

// visitCallExpr visits a "call expression" which can be either a
// function/method call (e.g., f(), pkg.f(), obj.f(), etc.) call or a type
// conversion (e.g., int32(1), (*sync.Mutex)(ptr), etc.).
func (v *globalsVisitor) visitCallExpr(e *ast.CallExpr) {
	if v.isViableType(e.Fun) {
		v.visitType(e.Fun)
	} else {
		v.visitExpr(e.Fun)
	}

	// If the function being called is new or make, the first argument is
	// a type, so it needs to be visited as such.
	first := 0
	if id := GetIdent(e.Fun); id != nil && (id.Name == "make" || id.Name == "new") {
		if len(e.Args) > 0 {
			v.visitType(e.Args[0])
		}
		first = 1
	}

	for i := first; i < len(e.Args); i++ {
		v.visitExpr(e.Args[i])
	}
}

// visitExpr visits all nodes of an expression, and reports any globals that it
// finds.
func (v *globalsVisitor) visitExpr(ge ast.Expr) {
	switch e := ge.(type) {
	case nil:
	case *ast.Ident:
		if s := v.scope.deepLookup(e.Name); s != nil && s.scope.isGlobal() {
			v.f(e, s.kind)
		}

	case *ast.BasicLit:
	case *ast.CompositeLit:
		v.visitType(e.Type)
		for _, ne := range e.Elts {
			v.visitExpr(ne)
		}
	case *ast.FuncLit:
		v.pushScope()
		v.visitFields(e.Type.Params, KindParameter)
		v.visitFields(e.Type.Results, KindResult)
		v.visitBlockStmt(e.Body)
		v.popScope()

	case *ast.BinaryExpr:
		v.visitExpr(e.X)
		v.visitExpr(e.Y)

	case *ast.CallExpr:
		v.visitCallExpr(e)

	case *ast.IndexExpr:
		v.visitExpr(e.X)
		v.visitExpr(e.Index)

	case *ast.KeyValueExpr:
		v.visitExpr(e.Value)

	case *ast.ParenExpr:
		v.visitExpr(e.X)

	case *ast.SelectorExpr:
		v.visitExpr(e.X)

	case *ast.SliceExpr:
		v.visitExpr(e.X)
		v.visitExpr(e.Low)
		v.visitExpr(e.High)
		v.visitExpr(e.Max)

	case *ast.StarExpr:
		v.visitExpr(e.X)

	case *ast.TypeAssertExpr:
		v.visitExpr(e.X)
		if e.Type != nil {
			v.visitType(e.Type)
		}

	case *ast.UnaryExpr:
		v.visitExpr(e.X)

	default:
		v.unexpected(ge.Pos())
	}
}

// GetIdent returns the identifier associated with the given expression by
// removing parentheses if needed.
func GetIdent(expr ast.Expr) *ast.Ident {
	switch e := expr.(type) {
	case *ast.Ident:
		return e
	case *ast.ParenExpr:
		return GetIdent(e.X)
	default:
		return nil
	}
}

// visitStmt visits all nodes of a statement, and reports any globals that it
// finds. It also adds to the current scope new symbols defined/declared.
func (v *globalsVisitor) visitStmt(gs ast.Stmt) {
	switch s := gs.(type) {
	case nil, *ast.BranchStmt, *ast.EmptyStmt:
	case *ast.AssignStmt:
		for _, e := range s.Rhs {
			v.visitExpr(e)
		}

		// We visit the LHS after the RHS because the symbols we'll
		// potentially add to the table aren't meant to be visible to
		// the RHS.
		for _, e := range s.Lhs {
			if s.Tok == token.DEFINE {
				if n := GetIdent(e); n != nil {
					v.scope.add(n.Name, KindVar, n.Pos())
				}
			}
			v.visitExpr(e)
		}

	case *ast.BlockStmt:
		v.visitBlockStmt(s)

	case *ast.DeclStmt:
		v.visitGenDecl(s.Decl.(*ast.GenDecl))

	case *ast.DeferStmt:
		v.visitCallExpr(s.Call)

	case *ast.ExprStmt:
		v.visitExpr(s.X)

	case *ast.ForStmt:
		v.pushScope()
		v.visitStmt(s.Init)
		v.visitExpr(s.Cond)
		v.visitStmt(s.Post)
		v.visitBlockStmt(s.Body)
		v.popScope()

	case *ast.GoStmt:
		v.visitCallExpr(s.Call)

	case *ast.IfStmt:
		v.pushScope()
		v.visitStmt(s.Init)
		v.visitExpr(s.Cond)
		v.visitBlockStmt(s.Body)
		v.visitStmt(s.Else)
		v.popScope()

	case *ast.IncDecStmt:
		v.visitExpr(s.X)

	case *ast.LabeledStmt:
		v.visitStmt(s.Stmt)

	case *ast.RangeStmt:
		v.pushScope()
		v.visitExpr(s.X)
		if s.Tok == token.DEFINE {
			if n := GetIdent(s.Key); n != nil {
				v.scope.add(n.Name, KindVar, n.Pos())
			}

			if n := GetIdent(s.Value); n != nil {
				v.scope.add(n.Name, KindVar, n.Pos())
			}
		}
		v.visitExpr(s.Key)
		v.visitExpr(s.Value)
		v.visitBlockStmt(s.Body)
		v.popScope()

	case *ast.ReturnStmt:
		for _, r := range s.Results {
			v.visitExpr(r)
		}

	case *ast.SelectStmt:
		for _, ns := range s.Body.List {
			c := ns.(*ast.CommClause)

			v.pushScope()
			v.visitStmt(c.Comm)
			for _, bs := range c.Body {
				v.visitStmt(bs)
			}
			v.popScope()
		}

	case *ast.SendStmt:
		v.visitExpr(s.Chan)
		v.visitExpr(s.Value)

	case *ast.SwitchStmt:
		v.pushScope()
		v.visitStmt(s.Init)
		v.visitExpr(s.Tag)
		for _, ns := range s.Body.List {
			c := ns.(*ast.CaseClause)
			v.pushScope()
			for _, ce := range c.List {
				v.visitExpr(ce)
			}
			for _, bs := range c.Body {
				v.visitStmt(bs)
			}
			v.popScope()
		}
		v.popScope()

	case *ast.TypeSwitchStmt:
		v.pushScope()
		v.visitStmt(s.Init)
		v.visitStmt(s.Assign)
		for _, ns := range s.Body.List {
			c := ns.(*ast.CaseClause)
			v.pushScope()
			for _, ce := range c.List {
				v.visitType(ce)
			}
			for _, bs := range c.Body {
				v.visitStmt(bs)
			}
			v.popScope()
		}
		v.popScope()

	default:
		v.unexpected(gs.Pos())
	}
}

// visitBlockStmt visits all statements in the block, adding symbols to a newly
// created scope.
func (v *globalsVisitor) visitBlockStmt(s *ast.BlockStmt) {
	v.pushScope()
	for _, c := range s.List {
		v.visitStmt(c)
	}
	v.popScope()
}

// visitFuncDecl is called when a function or method declation is encountered.
// it creates a new scope for the function [optional] receiver, parameters and
// results, and visits all children nodes.
func (v *globalsVisitor) visitFuncDecl(d *ast.FuncDecl) {
	// We don't report methods.
	if d.Recv == nil {
		v.f(d.Name, KindFunction)
	}

	v.pushScope()
	v.visitFields(d.Recv, KindReceiver)
	v.visitFields(d.Type.Params, KindParameter)
	v.visitFields(d.Type.Results, KindResult)
	if d.Body != nil {
		v.visitBlockStmt(d.Body)
	}
	v.popScope()
}

// globalsFromDecl is called in the first, and adds symbols to global scope.
func (v *globalsVisitor) globalsFromGenDecl(d *ast.GenDecl) {
	switch d.Tok {
	case token.IMPORT:
		for _, gs := range d.Specs {
			s := gs.(*ast.ImportSpec)
			if s.Name == nil {
				str, _ := strconv.Unquote(s.Path.Value)
				v.scope.add(filepath.Base(str), KindImport, s.Path.Pos())
			} else if s.Name.Name != "_" {
				v.scope.add(s.Name.Name, KindImport, s.Name.Pos())
			}
		}
	case token.TYPE:
		for _, gs := range d.Specs {
			s := gs.(*ast.TypeSpec)
			v.scope.add(s.Name.Name, KindType, s.Name.Pos())
		}
	case token.CONST, token.VAR:
		kind := KindConst
		if d.Tok == token.VAR {
			kind = KindVar
		}

		for _, s := range d.Specs {
			for _, n := range s.(*ast.ValueSpec).Names {
				v.scope.add(n.Name, kind, n.Pos())
			}
		}
	default:
		v.unexpected(d.Pos())
	}
}

// visit implements the visiting of globals. It does performs the two passes
// described in the description of the globalsVisitor struct.
func (v *globalsVisitor) visit() {
	// Gather all symbols in the global scope. This excludes methods.
	v.pushScope()
	for _, gd := range v.file.Decls {
		switch d := gd.(type) {
		case *ast.GenDecl:
			v.globalsFromGenDecl(d)
		case *ast.FuncDecl:
			if d.Recv == nil {
				v.scope.add(d.Name.Name, KindFunction, d.Name.Pos())
			}
		default:
			v.unexpected(gd.Pos())
		}
	}

	// Go through the contents of the declarations.
	for _, gd := range v.file.Decls {
		switch d := gd.(type) {
		case *ast.GenDecl:
			v.visitGenDecl(d)
		case *ast.FuncDecl:
			v.visitFuncDecl(d)
		}
	}
}

// Visit traverses the provided AST and calls f() for each identifier that
// refers to global names. The global name must be defined in the file itself.
//
// The function f() is allowed to modify the identifier, for example, to rename
// uses of global references.
func Visit(fset *token.FileSet, file *ast.File, f func(*ast.Ident, SymKind)) {
	v := globalsVisitor{
		fset: fset,
		file: file,
		f:    f,
	}

	v.visit()
}
