// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file implements initialization and assignment checks.

package types

import (
	"go/ast"
	"go/token"
)

// assignment reports whether x can be assigned to a variable of type T,
// if necessary by attempting to convert untyped values to the appropriate
// type. If x.mode == invalid upon return, then assignment has already
// issued an error message and the caller doesn't have to report another.
// Use T == nil to indicate assignment to an untyped blank identifier.
//
// TODO(gri) Should find a better way to handle in-band errors.
//
func (check *Checker) assignment(x *operand, T Type) bool {
	switch x.mode {
	case invalid:
		return true // error reported before
	case constant, variable, mapindex, value, commaok:
		// ok
	default:
		unreachable()
	}

	// x must be a single value
	// (tuple types are never named - no need for underlying type)
	if t, _ := x.typ.(*Tuple); t != nil {
		assert(t.Len() > 1)
		check.errorf(x.pos(), "%d-valued expression %s used as single value", t.Len(), x)
		x.mode = invalid
		return false
	}

	if isUntyped(x.typ) {
		target := T
		// spec: "If an untyped constant is assigned to a variable of interface
		// type or the blank identifier, the constant is first converted to type
		// bool, rune, int, float64, complex128 or string respectively, depending
		// on whether the value is a boolean, rune, integer, floating-point, complex,
		// or string constant."
		if T == nil || IsInterface(T) {
			if T == nil && x.typ == Typ[UntypedNil] {
				check.errorf(x.pos(), "use of untyped nil")
				x.mode = invalid
				return false
			}
			target = defaultType(x.typ)
		}
		check.convertUntyped(x, target)
		if x.mode == invalid {
			return false
		}
	}

	// spec: "If a left-hand side is the blank identifier, any typed or
	// non-constant value except for the predeclared identifier nil may
	// be assigned to it."
	return T == nil || x.assignableTo(check.conf, T)
}

func (check *Checker) initConst(lhs *Const, x *operand) {
	if x.mode == invalid || x.typ == Typ[Invalid] || lhs.typ == Typ[Invalid] {
		if lhs.typ == nil {
			lhs.typ = Typ[Invalid]
		}
		return
	}

	// rhs must be a constant
	if x.mode != constant {
		check.errorf(x.pos(), "%s is not constant", x)
		if lhs.typ == nil {
			lhs.typ = Typ[Invalid]
		}
		return
	}
	assert(isConstType(x.typ))

	// If the lhs doesn't have a type yet, use the type of x.
	if lhs.typ == nil {
		lhs.typ = x.typ
	}

	if !check.assignment(x, lhs.typ) {
		if x.mode != invalid {
			check.errorf(x.pos(), "cannot define constant %s (type %s) as %s", lhs.Name(), lhs.typ, x)
		}
		return
	}

	lhs.val = x.val
}

// If result is set, lhs is a function result parameter and x is a return result.
func (check *Checker) initVar(lhs *Var, x *operand, result bool) Type {
	if x.mode == invalid || x.typ == Typ[Invalid] || lhs.typ == Typ[Invalid] {
		if lhs.typ == nil {
			lhs.typ = Typ[Invalid]
		}
		return nil
	}

	// If the lhs doesn't have a type yet, use the type of x.
	if lhs.typ == nil {
		typ := x.typ
		if isUntyped(typ) {
			// convert untyped types to default types
			if typ == Typ[UntypedNil] {
				check.errorf(x.pos(), "use of untyped nil")
				lhs.typ = Typ[Invalid]
				return nil
			}
			typ = defaultType(typ)
		}
		lhs.typ = typ
	}

	if !check.assignment(x, lhs.typ) {
		if x.mode != invalid {
			if result {
				// don't refer to lhs.name because it may be an anonymous result parameter
				check.errorf(x.pos(), "cannot return %s as value of type %s", x, lhs.typ)
			} else {
				check.errorf(x.pos(), "cannot initialize %s with %s", lhs, x)
			}
		}
		return nil
	}

	return x.typ
}

func (check *Checker) assignVar(lhs ast.Expr, x *operand) Type {
	if x.mode == invalid || x.typ == Typ[Invalid] {
		return nil
	}

	// Determine if the lhs is a (possibly parenthesized) identifier.
	ident, _ := unparen(lhs).(*ast.Ident)

	// Don't evaluate lhs if it is the blank identifier.
	if ident != nil && ident.Name == "_" {
		check.recordDef(ident, nil)
		if !check.assignment(x, nil) {
			assert(x.mode == invalid)
			x.typ = nil
		}
		return x.typ
	}

	// If the lhs is an identifier denoting a variable v, this assignment
	// is not a 'use' of v. Remember current value of v.used and restore
	// after evaluating the lhs via check.expr.
	var v *Var
	var v_used bool
	if ident != nil {
		if _, obj := check.scope.LookupParent(ident.Name, token.NoPos); obj != nil {
			v, _ = obj.(*Var)
			if v != nil {
				v_used = v.used
			}
		}
	}

	var z operand
	check.expr(&z, lhs)
	if v != nil {
		v.used = v_used // restore v.used
	}

	if z.mode == invalid || z.typ == Typ[Invalid] {
		return nil
	}

	// spec: "Each left-hand side operand must be addressable, a map index
	// expression, or the blank identifier. Operands may be parenthesized."
	switch z.mode {
	case invalid:
		return nil
	case variable, mapindex:
		// ok
	default:
		check.errorf(z.pos(), "cannot assign to %s", &z)
		return nil
	}

	if !check.assignment(x, z.typ) {
		if x.mode != invalid {
			check.errorf(x.pos(), "cannot assign %s to %s", x, &z)
		}
		return nil
	}

	return x.typ
}

// If returnPos is valid, initVars is called to type-check the assignment of
// return expressions, and returnPos is the position of the return statement.
func (check *Checker) initVars(lhs []*Var, rhs []ast.Expr, returnPos token.Pos) {
	l := len(lhs)
	get, r, commaOk := unpack(func(x *operand, i int) { check.expr(x, rhs[i]) }, len(rhs), l == 2 && !returnPos.IsValid())
	if get == nil || l != r {
		// invalidate lhs and use rhs
		for _, obj := range lhs {
			if obj.typ == nil {
				obj.typ = Typ[Invalid]
			}
		}
		if get == nil {
			return // error reported by unpack
		}
		check.useGetter(get, r)
		if returnPos.IsValid() {
			check.errorf(returnPos, "wrong number of return values (want %d, got %d)", l, r)
			return
		}
		check.errorf(rhs[0].Pos(), "assignment count mismatch (%d vs %d)", l, r)
		return
	}

	var x operand
	if commaOk {
		var a [2]Type
		for i := range a {
			get(&x, i)
			a[i] = check.initVar(lhs[i], &x, returnPos.IsValid())
		}
		check.recordCommaOkTypes(rhs[0], a)
		return
	}

	for i, lhs := range lhs {
		get(&x, i)
		check.initVar(lhs, &x, returnPos.IsValid())
	}
}

func (check *Checker) assignVars(lhs, rhs []ast.Expr) {
	l := len(lhs)
	get, r, commaOk := unpack(func(x *operand, i int) { check.expr(x, rhs[i]) }, len(rhs), l == 2)
	if get == nil {
		return // error reported by unpack
	}
	if l != r {
		check.useGetter(get, r)
		check.errorf(rhs[0].Pos(), "assignment count mismatch (%d vs %d)", l, r)
		return
	}

	var x operand
	if commaOk {
		var a [2]Type
		for i := range a {
			get(&x, i)
			a[i] = check.assignVar(lhs[i], &x)
		}
		check.recordCommaOkTypes(rhs[0], a)
		return
	}

	for i, lhs := range lhs {
		get(&x, i)
		check.assignVar(lhs, &x)
	}
}

func (check *Checker) shortVarDecl(pos token.Pos, lhs, rhs []ast.Expr) {
	scope := check.scope

	// collect lhs variables
	var newVars []*Var
	var lhsVars = make([]*Var, len(lhs))
	for i, lhs := range lhs {
		var obj *Var
		if ident, _ := lhs.(*ast.Ident); ident != nil {
			// Use the correct obj if the ident is redeclared. The
			// variable's scope starts after the declaration; so we
			// must use Scope.Lookup here and call Scope.Insert
			// (via check.declare) later.
			name := ident.Name
			if alt := scope.Lookup(name); alt != nil {
				// redeclared object must be a variable
				if alt, _ := alt.(*Var); alt != nil {
					obj = alt
				} else {
					check.errorf(lhs.Pos(), "cannot assign to %s", lhs)
				}
				check.recordUse(ident, alt)
			} else {
				// declare new variable, possibly a blank (_) variable
				obj = NewVar(ident.Pos(), check.pkg, name, nil)
				if name != "_" {
					newVars = append(newVars, obj)
				}
				check.recordDef(ident, obj)
			}
		} else {
			check.errorf(lhs.Pos(), "cannot declare %s", lhs)
		}
		if obj == nil {
			obj = NewVar(lhs.Pos(), check.pkg, "_", nil) // dummy variable
		}
		lhsVars[i] = obj
	}

	check.initVars(lhsVars, rhs, token.NoPos)

	// declare new variables
	if len(newVars) > 0 {
		// spec: "The scope of a constant or variable identifier declared inside
		// a function begins at the end of the ConstSpec or VarSpec (ShortVarDecl
		// for short variable declarations) and ends at the end of the innermost
		// containing block."
		scopePos := rhs[len(rhs)-1].End()
		for _, obj := range newVars {
			check.declare(scope, nil, obj, scopePos) // recordObject already called
		}
	} else {
		check.softErrorf(pos, "no new variables on left side of :=")
	}
}
