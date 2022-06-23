// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// See https://github.com/golang/go/blob/master/src/cmd/gofmt/simplify.go

package render

import (
	"go/ast"
	"go/token"
	"reflect"
	"unicode"
	"unicode/utf8"
)

func Simplify(f *ast.File) {
	// remove empty declarations such as "const ()", etc
	removeEmptyDeclGroups(f)

	var s simplifier
	ast.Walk(s, f)
}

// Values/types for special cases.
var (
	objectPtrNil = reflect.ValueOf((*ast.Object)(nil))
	scopePtrNil  = reflect.ValueOf((*ast.Scope)(nil))

	identType     = reflect.TypeOf((*ast.Ident)(nil))
	objectPtrType = reflect.TypeOf((*ast.Object)(nil))
	positionType  = reflect.TypeOf(token.NoPos)
	callExprType  = reflect.TypeOf((*ast.CallExpr)(nil))
	scopePtrType  = reflect.TypeOf((*ast.Scope)(nil))
)

func isWildcard(s string) bool {
	rune, size := utf8.DecodeRuneInString(s)
	return size == len(s) && unicode.IsLower(rune)
}

// match reports whether pattern matches val,
// recording wildcard submatches in m.
// If m == nil, match checks whether pattern == val.
func match(m map[string]reflect.Value, pattern, val reflect.Value) bool {
	// Wildcard matches any expression. If it appears multiple
	// times in the pattern, it must match the same expression
	// each time.
	if m != nil && pattern.IsValid() && pattern.Type() == identType {
		name := pattern.Interface().(*ast.Ident).Name
		if isWildcard(name) && val.IsValid() {
			// wildcards only match valid (non-nil) expressions.
			if _, ok := val.Interface().(ast.Expr); ok && !val.IsNil() {
				if old, ok := m[name]; ok {
					return match(nil, old, val)
				}
				m[name] = val
				return true
			}
		}
	}

	// Otherwise, pattern and val must match recursively.
	if !pattern.IsValid() || !val.IsValid() {
		return !pattern.IsValid() && !val.IsValid()
	}
	if pattern.Type() != val.Type() {
		return false
	}

	// Special cases.
	switch pattern.Type() {
	case identType:
		// For identifiers, only the names need to match
		// (and none of the other *ast.Object information).
		// This is a common case, handle it all here instead
		// of recursing down any further via reflection.
		p := pattern.Interface().(*ast.Ident)
		v := val.Interface().(*ast.Ident)
		return p == nil && v == nil || p != nil && v != nil && p.Name == v.Name
	case objectPtrType, positionType:
		// object pointers and token positions always match
		return true
	case callExprType:
		// For calls, the Ellipsis fields (token.Position) must
		// match since that is how f(x) and f(x...) are different.
		// Check them here but fall through for the remaining fields.
		p := pattern.Interface().(*ast.CallExpr)
		v := val.Interface().(*ast.CallExpr)
		if p.Ellipsis.IsValid() != v.Ellipsis.IsValid() {
			return false
		}
	}

	p := reflect.Indirect(pattern)
	v := reflect.Indirect(val)
	if !p.IsValid() || !v.IsValid() {
		return !p.IsValid() && !v.IsValid()
	}

	switch p.Kind() {
	case reflect.Slice:
		if p.Len() != v.Len() {
			return false
		}
		for i := 0; i < p.Len(); i++ {
			if !match(m, p.Index(i), v.Index(i)) {
				return false
			}
		}
		return true

	case reflect.Struct:
		for i := 0; i < p.NumField(); i++ {
			if !match(m, p.Field(i), v.Field(i)) {
				return false
			}
		}
		return true

	case reflect.Interface:
		return match(m, p.Elem(), v.Elem())
	}

	// Handle token integers, etc.
	return p.Interface() == v.Interface()
}

type simplifier struct{}

func (s simplifier) Visit(node ast.Node) ast.Visitor {
	switch n := node.(type) {
	case *ast.CompositeLit:
		// array, slice, and map composite literals may be simplified
		outer := n
		var keyType, eltType ast.Expr
		switch typ := outer.Type.(type) {
		case *ast.ArrayType:
			eltType = typ.Elt
		case *ast.MapType:
			keyType = typ.Key
			eltType = typ.Value
		}

		if eltType != nil {
			var ktyp reflect.Value
			if keyType != nil {
				ktyp = reflect.ValueOf(keyType)
			}
			typ := reflect.ValueOf(eltType)
			for i, x := range outer.Elts {
				px := &outer.Elts[i]
				// look at value of indexed/named elements
				if t, ok := x.(*ast.KeyValueExpr); ok {
					if keyType != nil {
						s.simplifyLiteral(ktyp, keyType, t.Key, &t.Key)
					}
					x = t.Value
					px = &t.Value
				}
				s.simplifyLiteral(typ, eltType, x, px)
			}
			// node was simplified - stop walk (there are no subnodes to simplify)
			return nil
		}

	case *ast.SliceExpr:
		// a slice expression of the form: s[a:len(s)]
		// can be simplified to: s[a:]
		// if s is "simple enough" (for now we only accept identifiers)
		//
		// Note: This may not be correct because len may have been redeclared in another
		//       file belonging to the same package. However, this is extremely unlikely
		//       and so far (April 2016, after years of supporting this rewrite feature)
		//       has never come up, so let's keep it working as is (see also #15153).
		if n.Max != nil {
			// - 3-index slices always require the 2nd and 3rd index
			break
		}
		if s, _ := n.X.(*ast.Ident); s != nil && s.Obj != nil {
			// the array/slice object is a single, resolved identifier
			if call, _ := n.High.(*ast.CallExpr); call != nil && len(call.Args) == 1 && !call.Ellipsis.IsValid() {
				// the high expression is a function call with a single argument
				if fun, _ := call.Fun.(*ast.Ident); fun != nil && fun.Name == "len" && fun.Obj == nil {
					// the function called is "len" and it is not locally defined; and
					// because we don't have dot imports, it must be the predefined len()
					if arg, _ := call.Args[0].(*ast.Ident); arg != nil && arg.Obj == s.Obj {
						// the len argument is the array/slice object
						n.High = nil
					}
				}
			}
		}
		// Note: We could also simplify slice expressions of the form s[0:b] to s[:b]
		//       but we leave them as is since sometimes we want to be very explicit
		//       about the lower bound.
		// An example where the 0 helps:
		//       x, y, z := b[0:2], b[2:4], b[4:6]
		// An example where it does not:
		//       x, y := b[:n], b[n:]

	case *ast.RangeStmt:
		// - a range of the form: for x, _ = range v {...}
		// can be simplified to: for x = range v {...}
		// - a range of the form: for _ = range v {...}
		// can be simplified to: for range v {...}
		if isBlank(n.Value) {
			n.Value = nil
		}
		if isBlank(n.Key) && n.Value == nil {
			n.Key = nil
		}
	}

	return s
}

func (s simplifier) simplifyLiteral(typ reflect.Value, astType, x ast.Expr, px *ast.Expr) {
	ast.Walk(s, x) // simplify x

	// if the element is a composite literal and its literal type
	// matches the outer literal's element type exactly, the inner
	// literal type may be omitted
	if inner, ok := x.(*ast.CompositeLit); ok {
		if match(nil, typ, reflect.ValueOf(inner.Type)) {
			inner.Type = nil
		}
	}
	// if the outer literal's element type is a pointer type *T
	// and the element is & of a composite literal of type T,
	// the inner &T may be omitted.
	if ptr, ok := astType.(*ast.StarExpr); ok {
		if addr, ok := x.(*ast.UnaryExpr); ok && addr.Op == token.AND {
			if inner, ok := addr.X.(*ast.CompositeLit); ok {
				if match(nil, reflect.ValueOf(ptr.X), reflect.ValueOf(inner.Type)) {
					inner.Type = nil // drop T
					*px = inner      // drop &
				}
			}
		}
	}
}

func isBlank(x ast.Expr) bool {
	ident, ok := x.(*ast.Ident)
	return ok && ident.Name == "_"
}

func simplify(f *ast.File) {
	// remove empty declarations such as "const ()", etc
	removeEmptyDeclGroups(f)

	var s simplifier
	ast.Walk(s, f)
}

func removeEmptyDeclGroups(f *ast.File) {
	i := 0
	for _, d := range f.Decls {
		if g, ok := d.(*ast.GenDecl); !ok || !isEmpty(f, g) {
			f.Decls[i] = d
			i++
		}
	}
	f.Decls = f.Decls[:i]
}

func isEmpty(f *ast.File, g *ast.GenDecl) bool {
	if g.Doc != nil || g.Specs != nil {
		return false
	}

	for _, c := range f.Comments {
		// if there is a comment in the declaration, it is not considered empty
		if g.Pos() <= c.Pos() && c.End() <= g.End() {
			return false
		}
	}

	return true
}
