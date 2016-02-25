// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file implements typechecking of expressions.

package types

import (
	"fmt"
	"go/ast"
	"go/token"
	"math"

	"golang.org/x/tools/go/exact"
)

/*
Basic algorithm:

Expressions are checked recursively, top down. Expression checker functions
are generally of the form:

  func f(x *operand, e *ast.Expr, ...)

where e is the expression to be checked, and x is the result of the check.
The check performed by f may fail in which case x.mode == invalid, and
related error messages will have been issued by f.

If a hint argument is present, it is the composite literal element type
of an outer composite literal; it is used to type-check composite literal
elements that have no explicit type specification in the source
(e.g.: []T{{...}, {...}}, the hint is the type T in this case).

All expressions are checked via rawExpr, which dispatches according
to expression kind. Upon returning, rawExpr is recording the types and
constant values for all expressions that have an untyped type (those types
may change on the way up in the expression tree). Usually these are constants,
but the results of comparisons or non-constant shifts of untyped constants
may also be untyped, but not constant.

Untyped expressions may eventually become fully typed (i.e., not untyped),
typically when the value is assigned to a variable, or is used otherwise.
The updateExprType method is used to record this final type and update
the recorded types: the type-checked expression tree is again traversed down,
and the new type is propagated as needed. Untyped constant expression values
that become fully typed must now be representable by the full type (constant
sub-expression trees are left alone except for their roots). This mechanism
ensures that a client sees the actual (run-time) type an untyped value would
have. It also permits type-checking of lhs shift operands "as if the shift
were not present": when updateExprType visits an untyped lhs shift operand
and assigns it it's final type, that type must be an integer type, and a
constant lhs must be representable as an integer.

When an expression gets its final type, either on the way out from rawExpr,
on the way down in updateExprType, or at the end of the type checker run,
the type (and constant value, if any) is recorded via Info.Types, if present.
*/

type opPredicates map[token.Token]func(Type) bool

var unaryOpPredicates = opPredicates{
	token.ADD: isNumeric,
	token.SUB: isNumeric,
	token.XOR: isInteger,
	token.NOT: isBoolean,
}

func (check *Checker) op(m opPredicates, x *operand, op token.Token) bool {
	if pred := m[op]; pred != nil {
		if !pred(x.typ) {
			check.invalidOp(x.pos(), "operator %s not defined for %s", op, x)
			return false
		}
	} else {
		check.invalidAST(x.pos(), "unknown operator %s", op)
		return false
	}
	return true
}

// The unary expression e may be nil. It's passed in for better error messages only.
func (check *Checker) unary(x *operand, e *ast.UnaryExpr, op token.Token) {
	switch op {
	case token.AND:
		// spec: "As an exception to the addressability
		// requirement x may also be a composite literal."
		if _, ok := unparen(x.expr).(*ast.CompositeLit); !ok && x.mode != variable {
			check.invalidOp(x.pos(), "cannot take address of %s", x)
			x.mode = invalid
			return
		}
		x.mode = value
		x.typ = &Pointer{base: x.typ}
		return

	case token.ARROW:
		typ, ok := x.typ.Underlying().(*Chan)
		if !ok {
			check.invalidOp(x.pos(), "cannot receive from non-channel %s", x)
			x.mode = invalid
			return
		}
		if typ.dir == SendOnly {
			check.invalidOp(x.pos(), "cannot receive from send-only channel %s", x)
			x.mode = invalid
			return
		}
		x.mode = commaok
		x.typ = typ.elem
		check.hasCallOrRecv = true
		return
	}

	if !check.op(unaryOpPredicates, x, op) {
		x.mode = invalid
		return
	}

	if x.mode == constant {
		typ := x.typ.Underlying().(*Basic)
		size := -1
		if isUnsigned(typ) {
			size = int(check.conf.sizeof(typ))
		}
		x.val = exact.UnaryOp(op, x.val, size)
		// Typed constants must be representable in
		// their type after each constant operation.
		if isTyped(typ) {
			if e != nil {
				x.expr = e // for better error message
			}
			check.representable(x, typ)
		}
		return
	}

	x.mode = value
	// x.typ remains unchanged
}

func isShift(op token.Token) bool {
	return op == token.SHL || op == token.SHR
}

func isComparison(op token.Token) bool {
	// Note: tokens are not ordered well to make this much easier
	switch op {
	case token.EQL, token.NEQ, token.LSS, token.LEQ, token.GTR, token.GEQ:
		return true
	}
	return false
}

func fitsFloat32(x exact.Value) bool {
	f32, _ := exact.Float32Val(x)
	f := float64(f32)
	return !math.IsInf(f, 0)
}

func roundFloat32(x exact.Value) exact.Value {
	f32, _ := exact.Float32Val(x)
	f := float64(f32)
	if !math.IsInf(f, 0) {
		return exact.MakeFloat64(f)
	}
	return nil
}

func fitsFloat64(x exact.Value) bool {
	f, _ := exact.Float64Val(x)
	return !math.IsInf(f, 0)
}

func roundFloat64(x exact.Value) exact.Value {
	f, _ := exact.Float64Val(x)
	if !math.IsInf(f, 0) {
		return exact.MakeFloat64(f)
	}
	return nil
}

// representableConst reports whether x can be represented as
// value of the given basic type kind and for the configuration
// provided (only needed for int/uint sizes).
//
// If rounded != nil, *rounded is set to the rounded value of x for
// representable floating-point values; it is left alone otherwise.
// It is ok to provide the addressof the first argument for rounded.
func representableConst(x exact.Value, conf *Config, as BasicKind, rounded *exact.Value) bool {
	switch x.Kind() {
	case exact.Unknown:
		return true

	case exact.Bool:
		return as == Bool || as == UntypedBool

	case exact.Int:
		if x, ok := exact.Int64Val(x); ok {
			switch as {
			case Int:
				var s = uint(conf.sizeof(Typ[as])) * 8
				return int64(-1)<<(s-1) <= x && x <= int64(1)<<(s-1)-1
			case Int8:
				const s = 8
				return -1<<(s-1) <= x && x <= 1<<(s-1)-1
			case Int16:
				const s = 16
				return -1<<(s-1) <= x && x <= 1<<(s-1)-1
			case Int32:
				const s = 32
				return -1<<(s-1) <= x && x <= 1<<(s-1)-1
			case Int64:
				return true
			case Uint, Uintptr:
				if s := uint(conf.sizeof(Typ[as])) * 8; s < 64 {
					return 0 <= x && x <= int64(1)<<s-1
				}
				return 0 <= x
			case Uint8:
				const s = 8
				return 0 <= x && x <= 1<<s-1
			case Uint16:
				const s = 16
				return 0 <= x && x <= 1<<s-1
			case Uint32:
				const s = 32
				return 0 <= x && x <= 1<<s-1
			case Uint64:
				return 0 <= x
			case Float32, Float64, Complex64, Complex128,
				UntypedInt, UntypedFloat, UntypedComplex:
				return true
			}
		}

		n := exact.BitLen(x)
		switch as {
		case Uint, Uintptr:
			var s = uint(conf.sizeof(Typ[as])) * 8
			return exact.Sign(x) >= 0 && n <= int(s)
		case Uint64:
			return exact.Sign(x) >= 0 && n <= 64
		case Float32, Complex64:
			if rounded == nil {
				return fitsFloat32(x)
			}
			r := roundFloat32(x)
			if r != nil {
				*rounded = r
				return true
			}
		case Float64, Complex128:
			if rounded == nil {
				return fitsFloat64(x)
			}
			r := roundFloat64(x)
			if r != nil {
				*rounded = r
				return true
			}
		case UntypedInt, UntypedFloat, UntypedComplex:
			return true
		}

	case exact.Float:
		switch as {
		case Float32, Complex64:
			if rounded == nil {
				return fitsFloat32(x)
			}
			r := roundFloat32(x)
			if r != nil {
				*rounded = r
				return true
			}
		case Float64, Complex128:
			if rounded == nil {
				return fitsFloat64(x)
			}
			r := roundFloat64(x)
			if r != nil {
				*rounded = r
				return true
			}
		case UntypedFloat, UntypedComplex:
			return true
		}

	case exact.Complex:
		switch as {
		case Complex64:
			if rounded == nil {
				return fitsFloat32(exact.Real(x)) && fitsFloat32(exact.Imag(x))
			}
			re := roundFloat32(exact.Real(x))
			im := roundFloat32(exact.Imag(x))
			if re != nil && im != nil {
				*rounded = exact.BinaryOp(re, token.ADD, exact.MakeImag(im))
				return true
			}
		case Complex128:
			if rounded == nil {
				return fitsFloat64(exact.Real(x)) && fitsFloat64(exact.Imag(x))
			}
			re := roundFloat64(exact.Real(x))
			im := roundFloat64(exact.Imag(x))
			if re != nil && im != nil {
				*rounded = exact.BinaryOp(re, token.ADD, exact.MakeImag(im))
				return true
			}
		case UntypedComplex:
			return true
		}

	case exact.String:
		return as == String || as == UntypedString

	default:
		unreachable()
	}

	return false
}

// representable checks that a constant operand is representable in the given basic type.
func (check *Checker) representable(x *operand, typ *Basic) {
	assert(x.mode == constant)
	if !representableConst(x.val, check.conf, typ.kind, &x.val) {
		var msg string
		if isNumeric(x.typ) && isNumeric(typ) {
			// numeric conversion : error msg
			//
			// integer -> integer : overflows
			// integer -> float   : overflows (actually not possible)
			// float   -> integer : truncated
			// float   -> float   : overflows
			//
			if !isInteger(x.typ) && isInteger(typ) {
				msg = "%s truncated to %s"
			} else {
				msg = "%s overflows %s"
			}
		} else {
			msg = "cannot convert %s to %s"
		}
		check.errorf(x.pos(), msg, x, typ)
		x.mode = invalid
	}
}

// updateExprType updates the type of x to typ and invokes itself
// recursively for the operands of x, depending on expression kind.
// If typ is still an untyped and not the final type, updateExprType
// only updates the recorded untyped type for x and possibly its
// operands. Otherwise (i.e., typ is not an untyped type anymore,
// or it is the final type for x), the type and value are recorded.
// Also, if x is a constant, it must be representable as a value of typ,
// and if x is the (formerly untyped) lhs operand of a non-constant
// shift, it must be an integer value.
//
func (check *Checker) updateExprType(x ast.Expr, typ Type, final bool) {
	old, found := check.untyped[x]
	if !found {
		return // nothing to do
	}

	// update operands of x if necessary
	switch x := x.(type) {
	case *ast.BadExpr,
		*ast.FuncLit,
		*ast.CompositeLit,
		*ast.IndexExpr,
		*ast.SliceExpr,
		*ast.TypeAssertExpr,
		*ast.StarExpr,
		*ast.KeyValueExpr,
		*ast.ArrayType,
		*ast.StructType,
		*ast.FuncType,
		*ast.InterfaceType,
		*ast.MapType,
		*ast.ChanType:
		// These expression are never untyped - nothing to do.
		// The respective sub-expressions got their final types
		// upon assignment or use.
		if debug {
			check.dump("%s: found old type(%s): %s (new: %s)", x.Pos(), x, old.typ, typ)
			unreachable()
		}
		return

	case *ast.CallExpr:
		// Resulting in an untyped constant (e.g., built-in complex).
		// The respective calls take care of calling updateExprType
		// for the arguments if necessary.

	case *ast.Ident, *ast.BasicLit, *ast.SelectorExpr:
		// An identifier denoting a constant, a constant literal,
		// or a qualified identifier (imported untyped constant).
		// No operands to take care of.

	case *ast.ParenExpr:
		check.updateExprType(x.X, typ, final)

	case *ast.UnaryExpr:
		// If x is a constant, the operands were constants.
		// They don't need to be updated since they never
		// get "materialized" into a typed value; and they
		// will be processed at the end of the type check.
		if old.val != nil {
			break
		}
		check.updateExprType(x.X, typ, final)

	case *ast.BinaryExpr:
		if old.val != nil {
			break // see comment for unary expressions
		}
		if isComparison(x.Op) {
			// The result type is independent of operand types
			// and the operand types must have final types.
		} else if isShift(x.Op) {
			// The result type depends only on lhs operand.
			// The rhs type was updated when checking the shift.
			check.updateExprType(x.X, typ, final)
		} else {
			// The operand types match the result type.
			check.updateExprType(x.X, typ, final)
			check.updateExprType(x.Y, typ, final)
		}

	default:
		unreachable()
	}

	// If the new type is not final and still untyped, just
	// update the recorded type.
	if !final && isUntyped(typ) {
		old.typ = typ.Underlying().(*Basic)
		check.untyped[x] = old
		return
	}

	// Otherwise we have the final (typed or untyped type).
	// Remove it from the map of yet untyped expressions.
	delete(check.untyped, x)

	// If x is the lhs of a shift, its final type must be integer.
	// We already know from the shift check that it is representable
	// as an integer if it is a constant.
	if old.isLhs && !isInteger(typ) {
		check.invalidOp(x.Pos(), "shifted operand %s (type %s) must be integer", x, typ)
		return
	}

	// Everything's fine, record final type and value for x.
	check.recordTypeAndValue(x, old.mode, typ, old.val)
}

// updateExprVal updates the value of x to val.
func (check *Checker) updateExprVal(x ast.Expr, val exact.Value) {
	if info, ok := check.untyped[x]; ok {
		info.val = val
		check.untyped[x] = info
	}
}

// convertUntyped attempts to set the type of an untyped value to the target type.
func (check *Checker) convertUntyped(x *operand, target Type) {
	if x.mode == invalid || isTyped(x.typ) || target == Typ[Invalid] {
		return
	}

	// TODO(gri) Sloppy code - clean up. This function is central
	//           to assignment and expression checking.

	if isUntyped(target) {
		// both x and target are untyped
		xkind := x.typ.(*Basic).kind
		tkind := target.(*Basic).kind
		if isNumeric(x.typ) && isNumeric(target) {
			if xkind < tkind {
				x.typ = target
				check.updateExprType(x.expr, target, false)
			}
		} else if xkind != tkind {
			goto Error
		}
		return
	}

	// typed target
	switch t := target.Underlying().(type) {
	case *Basic:
		if x.mode == constant {
			check.representable(x, t)
			if x.mode == invalid {
				return
			}
			// expression value may have been rounded - update if needed
			// TODO(gri) A floating-point value may silently underflow to
			// zero. If it was negative, the sign is lost. See issue 6898.
			check.updateExprVal(x.expr, x.val)
		} else {
			// Non-constant untyped values may appear as the
			// result of comparisons (untyped bool), intermediate
			// (delayed-checked) rhs operands of shifts, and as
			// the value nil.
			switch x.typ.(*Basic).kind {
			case UntypedBool:
				if !isBoolean(target) {
					goto Error
				}
			case UntypedInt, UntypedRune, UntypedFloat, UntypedComplex:
				if !isNumeric(target) {
					goto Error
				}
			case UntypedString:
				// Non-constant untyped string values are not
				// permitted by the spec and should not occur.
				unreachable()
			case UntypedNil:
				// Unsafe.Pointer is a basic type that includes nil.
				if !hasNil(target) {
					goto Error
				}
			default:
				goto Error
			}
		}
	case *Interface:
		if !x.isNil() && !t.Empty() /* empty interfaces are ok */ {
			goto Error
		}
		// Update operand types to the default type rather then
		// the target (interface) type: values must have concrete
		// dynamic types. If the value is nil, keep it untyped
		// (this is important for tools such as go vet which need
		// the dynamic type for argument checking of say, print
		// functions)
		if x.isNil() {
			target = Typ[UntypedNil]
		} else {
			// cannot assign untyped values to non-empty interfaces
			if !t.Empty() {
				goto Error
			}
			target = defaultType(x.typ)
		}
	case *Pointer, *Signature, *Slice, *Map, *Chan:
		if !x.isNil() {
			goto Error
		}
		// keep nil untyped - see comment for interfaces, above
		target = Typ[UntypedNil]
	default:
		goto Error
	}

	x.typ = target
	check.updateExprType(x.expr, target, true) // UntypedNils are final
	return

Error:
	check.errorf(x.pos(), "cannot convert %s to %s", x, target)
	x.mode = invalid
}

func (check *Checker) comparison(x, y *operand, op token.Token) {
	// spec: "In any comparison, the first operand must be assignable
	// to the type of the second operand, or vice versa."
	err := ""
	if x.assignableTo(check.conf, y.typ) || y.assignableTo(check.conf, x.typ) {
		defined := false
		switch op {
		case token.EQL, token.NEQ:
			// spec: "The equality operators == and != apply to operands that are comparable."
			defined = Comparable(x.typ) || x.isNil() && hasNil(y.typ) || y.isNil() && hasNil(x.typ)
		case token.LSS, token.LEQ, token.GTR, token.GEQ:
			// spec: The ordering operators <, <=, >, and >= apply to operands that are ordered."
			defined = isOrdered(x.typ)
		default:
			unreachable()
		}
		if !defined {
			typ := x.typ
			if x.isNil() {
				typ = y.typ
			}
			err = check.sprintf("operator %s not defined for %s", op, typ)
		}
	} else {
		err = check.sprintf("mismatched types %s and %s", x.typ, y.typ)
	}

	if err != "" {
		check.errorf(x.pos(), "cannot compare %s %s %s (%s)", x.expr, op, y.expr, err)
		x.mode = invalid
		return
	}

	if x.mode == constant && y.mode == constant {
		x.val = exact.MakeBool(exact.Compare(x.val, op, y.val))
		// The operands are never materialized; no need to update
		// their types.
	} else {
		x.mode = value
		// The operands have now their final types, which at run-
		// time will be materialized. Update the expression trees.
		// If the current types are untyped, the materialized type
		// is the respective default type.
		check.updateExprType(x.expr, defaultType(x.typ), true)
		check.updateExprType(y.expr, defaultType(y.typ), true)
	}

	// spec: "Comparison operators compare two operands and yield
	//        an untyped boolean value."
	x.typ = Typ[UntypedBool]
}

func (check *Checker) shift(x, y *operand, op token.Token) {
	untypedx := isUntyped(x.typ)

	// The lhs must be of integer type or be representable
	// as an integer; otherwise the shift has no chance.
	if !x.isInteger() {
		check.invalidOp(x.pos(), "shifted operand %s must be integer", x)
		x.mode = invalid
		return
	}

	// spec: "The right operand in a shift expression must have unsigned
	// integer type or be an untyped constant that can be converted to
	// unsigned integer type."
	switch {
	case isInteger(y.typ) && isUnsigned(y.typ):
		// nothing to do
	case isUntyped(y.typ):
		check.convertUntyped(y, Typ[UntypedInt])
		if y.mode == invalid {
			x.mode = invalid
			return
		}
	default:
		check.invalidOp(y.pos(), "shift count %s must be unsigned integer", y)
		x.mode = invalid
		return
	}

	if x.mode == constant {
		if y.mode == constant {
			// rhs must be an integer value
			if !y.isInteger() {
				check.invalidOp(y.pos(), "shift count %s must be unsigned integer", y)
				x.mode = invalid
				return
			}
			// rhs must be within reasonable bounds
			const stupidShift = 1023 - 1 + 52 // so we can express smallestFloat64
			s, ok := exact.Uint64Val(y.val)
			if !ok || s > stupidShift {
				check.invalidOp(y.pos(), "stupid shift count %s", y)
				x.mode = invalid
				return
			}
			// The lhs is representable as an integer but may not be an integer
			// (e.g., 2.0, an untyped float) - this can only happen for untyped
			// non-integer numeric constants. Correct the type so that the shift
			// result is of integer type.
			if !isInteger(x.typ) {
				x.typ = Typ[UntypedInt]
			}
			x.val = exact.Shift(x.val, op, uint(s))
			return
		}

		// non-constant shift with constant lhs
		if untypedx {
			// spec: "If the left operand of a non-constant shift
			// expression is an untyped constant, the type of the
			// constant is what it would be if the shift expression
			// were replaced by its left operand alone.".
			//
			// Delay operand checking until we know the final type:
			// The lhs expression must be in the untyped map, mark
			// the entry as lhs shift operand.
			info, found := check.untyped[x.expr]
			assert(found)
			info.isLhs = true
			check.untyped[x.expr] = info
			// keep x's type
			x.mode = value
			return
		}
	}

	// constant rhs must be >= 0
	if y.mode == constant && exact.Sign(y.val) < 0 {
		check.invalidOp(y.pos(), "shift count %s must not be negative", y)
	}

	// non-constant shift - lhs must be an integer
	if !isInteger(x.typ) {
		check.invalidOp(x.pos(), "shifted operand %s must be integer", x)
		x.mode = invalid
		return
	}

	x.mode = value
}

var binaryOpPredicates = opPredicates{
	token.ADD: func(typ Type) bool { return isNumeric(typ) || isString(typ) },
	token.SUB: isNumeric,
	token.MUL: isNumeric,
	token.QUO: isNumeric,
	token.REM: isInteger,

	token.AND:     isInteger,
	token.OR:      isInteger,
	token.XOR:     isInteger,
	token.AND_NOT: isInteger,

	token.LAND: isBoolean,
	token.LOR:  isBoolean,
}

// The binary expression e may be nil. It's passed in for better error messages only.
func (check *Checker) binary(x *operand, e *ast.BinaryExpr, lhs, rhs ast.Expr, op token.Token) {
	var y operand

	check.expr(x, lhs)
	check.expr(&y, rhs)

	if x.mode == invalid {
		return
	}
	if y.mode == invalid {
		x.mode = invalid
		x.expr = y.expr
		return
	}

	if isShift(op) {
		check.shift(x, &y, op)
		return
	}

	check.convertUntyped(x, y.typ)
	if x.mode == invalid {
		return
	}
	check.convertUntyped(&y, x.typ)
	if y.mode == invalid {
		x.mode = invalid
		return
	}

	if isComparison(op) {
		check.comparison(x, &y, op)
		return
	}

	if !Identical(x.typ, y.typ) {
		// only report an error if we have valid types
		// (otherwise we had an error reported elsewhere already)
		if x.typ != Typ[Invalid] && y.typ != Typ[Invalid] {
			check.invalidOp(x.pos(), "mismatched types %s and %s", x.typ, y.typ)
		}
		x.mode = invalid
		return
	}

	if !check.op(binaryOpPredicates, x, op) {
		x.mode = invalid
		return
	}

	if (op == token.QUO || op == token.REM) && (x.mode == constant || isInteger(x.typ)) && y.mode == constant && exact.Sign(y.val) == 0 {
		check.invalidOp(y.pos(), "division by zero")
		x.mode = invalid
		return
	}

	if x.mode == constant && y.mode == constant {
		typ := x.typ.Underlying().(*Basic)
		// force integer division of integer operands
		if op == token.QUO && isInteger(typ) {
			op = token.QUO_ASSIGN
		}
		x.val = exact.BinaryOp(x.val, op, y.val)
		// Typed constants must be representable in
		// their type after each constant operation.
		if isTyped(typ) {
			if e != nil {
				x.expr = e // for better error message
			}
			check.representable(x, typ)
		}
		return
	}

	x.mode = value
	// x.typ is unchanged
}

// index checks an index expression for validity.
// If max >= 0, it is the upper bound for index.
// If index is valid and the result i >= 0, then i is the constant value of index.
func (check *Checker) index(index ast.Expr, max int64) (i int64, valid bool) {
	var x operand
	check.expr(&x, index)
	if x.mode == invalid {
		return
	}

	// an untyped constant must be representable as Int
	check.convertUntyped(&x, Typ[Int])
	if x.mode == invalid {
		return
	}

	// the index must be of integer type
	if !isInteger(x.typ) {
		check.invalidArg(x.pos(), "index %s must be integer", &x)
		return
	}

	// a constant index i must be in bounds
	if x.mode == constant {
		if exact.Sign(x.val) < 0 {
			check.invalidArg(x.pos(), "index %s must not be negative", &x)
			return
		}
		i, valid = exact.Int64Val(x.val)
		if !valid || max >= 0 && i >= max {
			check.errorf(x.pos(), "index %s is out of bounds", &x)
			return i, false
		}
		// 0 <= i [ && i < max ]
		return i, true
	}

	return -1, true
}

// indexElts checks the elements (elts) of an array or slice composite literal
// against the literal's element type (typ), and the element indices against
// the literal length if known (length >= 0). It returns the length of the
// literal (maximum index value + 1).
//
func (check *Checker) indexedElts(elts []ast.Expr, typ Type, length int64) int64 {
	visited := make(map[int64]bool, len(elts))
	var index, max int64
	for _, e := range elts {
		// determine and check index
		validIndex := false
		eval := e
		if kv, _ := e.(*ast.KeyValueExpr); kv != nil {
			if i, ok := check.index(kv.Key, length); ok {
				if i >= 0 {
					index = i
					validIndex = true
				} else {
					check.errorf(e.Pos(), "index %s must be integer constant", kv.Key)
				}
			}
			eval = kv.Value
		} else if length >= 0 && index >= length {
			check.errorf(e.Pos(), "index %d is out of bounds (>= %d)", index, length)
		} else {
			validIndex = true
		}

		// if we have a valid index, check for duplicate entries
		if validIndex {
			if visited[index] {
				check.errorf(e.Pos(), "duplicate index %d in array or slice literal", index)
			}
			visited[index] = true
		}
		index++
		if index > max {
			max = index
		}

		// check element against composite literal element type
		var x operand
		check.exprWithHint(&x, eval, typ)
		if !check.assignment(&x, typ) && x.mode != invalid {
			check.errorf(x.pos(), "cannot use %s as %s value in array or slice literal", &x, typ)
		}
	}
	return max
}

// exprKind describes the kind of an expression; the kind
// determines if an expression is valid in 'statement context'.
type exprKind int

const (
	conversion exprKind = iota
	expression
	statement
)

// rawExpr typechecks expression e and initializes x with the expression
// value or type. If an error occurred, x.mode is set to invalid.
// If hint != nil, it is the type of a composite literal element.
//
func (check *Checker) rawExpr(x *operand, e ast.Expr, hint Type) exprKind {
	if trace {
		check.trace(e.Pos(), "%s", e)
		check.indent++
		defer func() {
			check.indent--
			check.trace(e.Pos(), "=> %s", x)
		}()
	}

	kind := check.exprInternal(x, e, hint)

	// convert x into a user-friendly set of values
	// TODO(gri) this code can be simplified
	var typ Type
	var val exact.Value
	switch x.mode {
	case invalid:
		typ = Typ[Invalid]
	case novalue:
		typ = (*Tuple)(nil)
	case constant:
		typ = x.typ
		val = x.val
	default:
		typ = x.typ
	}
	assert(x.expr != nil && typ != nil)

	if isUntyped(typ) {
		// delay type and value recording until we know the type
		// or until the end of type checking
		check.rememberUntyped(x.expr, false, x.mode, typ.(*Basic), val)
	} else {
		check.recordTypeAndValue(e, x.mode, typ, val)
	}

	return kind
}

// exprInternal contains the core of type checking of expressions.
// Must only be called by rawExpr.
//
func (check *Checker) exprInternal(x *operand, e ast.Expr, hint Type) exprKind {
	// make sure x has a valid state in case of bailout
	// (was issue 5770)
	x.mode = invalid
	x.typ = Typ[Invalid]

	switch e := e.(type) {
	case *ast.BadExpr:
		goto Error // error was reported before

	case *ast.Ident:
		check.ident(x, e, nil, nil)

	case *ast.Ellipsis:
		// ellipses are handled explicitly where they are legal
		// (array composite literals and parameter lists)
		check.error(e.Pos(), "invalid use of '...'")
		goto Error

	case *ast.BasicLit:
		x.setConst(e.Kind, e.Value)
		if x.mode == invalid {
			check.invalidAST(e.Pos(), "invalid literal %v", e.Value)
			goto Error
		}

	case *ast.FuncLit:
		if sig, ok := check.typ(e.Type).(*Signature); ok {
			// Anonymous functions are considered part of the
			// init expression/func declaration which contains
			// them: use existing package-level declaration info.
			check.funcBody(check.decl, "", sig, e.Body)
			x.mode = value
			x.typ = sig
		} else {
			check.invalidAST(e.Pos(), "invalid function literal %s", e)
			goto Error
		}

	case *ast.CompositeLit:
		typ := hint
		openArray := false
		if e.Type != nil {
			// [...]T array types may only appear with composite literals.
			// Check for them here so we don't have to handle ... in general.
			typ = nil
			if atyp, _ := e.Type.(*ast.ArrayType); atyp != nil && atyp.Len != nil {
				if ellip, _ := atyp.Len.(*ast.Ellipsis); ellip != nil && ellip.Elt == nil {
					// We have an "open" [...]T array type.
					// Create a new ArrayType with unknown length (-1)
					// and finish setting it up after analyzing the literal.
					typ = &Array{len: -1, elem: check.typ(atyp.Elt)}
					openArray = true
				}
			}
			if typ == nil {
				typ = check.typ(e.Type)
			}
		}
		if typ == nil {
			// TODO(gri) provide better error messages depending on context
			check.error(e.Pos(), "missing type in composite literal")
			goto Error
		}

		switch typ, _ := deref(typ); utyp := typ.Underlying().(type) {
		case *Struct:
			if len(e.Elts) == 0 {
				break
			}
			fields := utyp.fields
			if _, ok := e.Elts[0].(*ast.KeyValueExpr); ok {
				// all elements must have keys
				visited := make([]bool, len(fields))
				for _, e := range e.Elts {
					kv, _ := e.(*ast.KeyValueExpr)
					if kv == nil {
						check.error(e.Pos(), "mixture of field:value and value elements in struct literal")
						continue
					}
					key, _ := kv.Key.(*ast.Ident)
					if key == nil {
						check.errorf(kv.Pos(), "invalid field name %s in struct literal", kv.Key)
						continue
					}
					i := fieldIndex(utyp.fields, check.pkg, key.Name)
					if i < 0 {
						check.errorf(kv.Pos(), "unknown field %s in struct literal", key.Name)
						continue
					}
					fld := fields[i]
					check.recordUse(key, fld)
					// 0 <= i < len(fields)
					if visited[i] {
						check.errorf(kv.Pos(), "duplicate field name %s in struct literal", key.Name)
						continue
					}
					visited[i] = true
					check.expr(x, kv.Value)
					etyp := fld.typ
					if !check.assignment(x, etyp) {
						if x.mode != invalid {
							check.errorf(x.pos(), "cannot use %s as %s value in struct literal", x, etyp)
						}
						continue
					}
				}
			} else {
				// no element must have a key
				for i, e := range e.Elts {
					if kv, _ := e.(*ast.KeyValueExpr); kv != nil {
						check.error(kv.Pos(), "mixture of field:value and value elements in struct literal")
						continue
					}
					check.expr(x, e)
					if i >= len(fields) {
						check.error(x.pos(), "too many values in struct literal")
						break // cannot continue
					}
					// i < len(fields)
					fld := fields[i]
					if !fld.Exported() && fld.pkg != check.pkg {
						check.errorf(x.pos(), "implicit assignment to unexported field %s in %s literal", fld.name, typ)
						continue
					}
					etyp := fld.typ
					if !check.assignment(x, etyp) {
						if x.mode != invalid {
							check.errorf(x.pos(), "cannot use %s as %s value in struct literal", x, etyp)
						}
						continue
					}
				}
				if len(e.Elts) < len(fields) {
					check.error(e.Rbrace, "too few values in struct literal")
					// ok to continue
				}
			}

		case *Array:
			n := check.indexedElts(e.Elts, utyp.elem, utyp.len)
			// if we have an "open" [...]T array, set the length now that we know it
			if openArray {
				utyp.len = n
			}

		case *Slice:
			check.indexedElts(e.Elts, utyp.elem, -1)

		case *Map:
			visited := make(map[interface{}][]Type, len(e.Elts))
			for _, e := range e.Elts {
				kv, _ := e.(*ast.KeyValueExpr)
				if kv == nil {
					check.error(e.Pos(), "missing key in map literal")
					continue
				}
				check.exprWithHint(x, kv.Key, utyp.key)
				if !check.assignment(x, utyp.key) {
					if x.mode != invalid {
						check.errorf(x.pos(), "cannot use %s as %s key in map literal", x, utyp.key)
					}
					continue
				}
				if x.mode == constant {
					duplicate := false
					// if the key is of interface type, the type is also significant when checking for duplicates
					if _, ok := utyp.key.Underlying().(*Interface); ok {
						for _, vtyp := range visited[x.val] {
							if Identical(vtyp, x.typ) {
								duplicate = true
								break
							}
						}
						visited[x.val] = append(visited[x.val], x.typ)
					} else {
						_, duplicate = visited[x.val]
						visited[x.val] = nil
					}
					if duplicate {
						check.errorf(x.pos(), "duplicate key %s in map literal", x.val)
						continue
					}
				}
				check.exprWithHint(x, kv.Value, utyp.elem)
				if !check.assignment(x, utyp.elem) {
					if x.mode != invalid {
						check.errorf(x.pos(), "cannot use %s as %s value in map literal", x, utyp.elem)
					}
					continue
				}
			}

		default:
			// if utyp is invalid, an error was reported before
			if utyp != Typ[Invalid] {
				check.errorf(e.Pos(), "invalid composite literal type %s", typ)
				goto Error
			}
		}

		x.mode = value
		x.typ = typ

	case *ast.ParenExpr:
		kind := check.rawExpr(x, e.X, nil)
		x.expr = e
		return kind

	case *ast.SelectorExpr:
		check.selector(x, e)

	case *ast.IndexExpr:
		check.expr(x, e.X)
		if x.mode == invalid {
			goto Error
		}

		valid := false
		length := int64(-1) // valid if >= 0
		switch typ := x.typ.Underlying().(type) {
		case *Basic:
			if isString(typ) {
				valid = true
				if x.mode == constant {
					length = int64(len(exact.StringVal(x.val)))
				}
				// an indexed string always yields a byte value
				// (not a constant) even if the string and the
				// index are constant
				x.mode = value
				x.typ = universeByte // use 'byte' name
			}

		case *Array:
			valid = true
			length = typ.len
			if x.mode != variable {
				x.mode = value
			}
			x.typ = typ.elem

		case *Pointer:
			if typ, _ := typ.base.Underlying().(*Array); typ != nil {
				valid = true
				length = typ.len
				x.mode = variable
				x.typ = typ.elem
			}

		case *Slice:
			valid = true
			x.mode = variable
			x.typ = typ.elem

		case *Map:
			var key operand
			check.expr(&key, e.Index)
			if !check.assignment(&key, typ.key) {
				if key.mode != invalid {
					check.invalidOp(key.pos(), "cannot use %s as map index of type %s", &key, typ.key)
				}
				goto Error
			}
			x.mode = mapindex
			x.typ = typ.elem
			x.expr = e
			return expression
		}

		if !valid {
			check.invalidOp(x.pos(), "cannot index %s", x)
			goto Error
		}

		if e.Index == nil {
			check.invalidAST(e.Pos(), "missing index for %s", x)
			goto Error
		}

		check.index(e.Index, length)
		// ok to continue

	case *ast.SliceExpr:
		check.expr(x, e.X)
		if x.mode == invalid {
			goto Error
		}

		valid := false
		length := int64(-1) // valid if >= 0
		switch typ := x.typ.Underlying().(type) {
		case *Basic:
			if isString(typ) {
				if e.Slice3 {
					check.invalidOp(x.pos(), "3-index slice of string")
					goto Error
				}
				valid = true
				if x.mode == constant {
					length = int64(len(exact.StringVal(x.val)))
				}
				// spec: "For untyped string operands the result
				// is a non-constant value of type string."
				if typ.kind == UntypedString {
					x.typ = Typ[String]
				}
			}

		case *Array:
			valid = true
			length = typ.len
			if x.mode != variable {
				check.invalidOp(x.pos(), "cannot slice %s (value not addressable)", x)
				goto Error
			}
			x.typ = &Slice{elem: typ.elem}

		case *Pointer:
			if typ, _ := typ.base.Underlying().(*Array); typ != nil {
				valid = true
				length = typ.len
				x.typ = &Slice{elem: typ.elem}
			}

		case *Slice:
			valid = true
			// x.typ doesn't change
		}

		if !valid {
			check.invalidOp(x.pos(), "cannot slice %s", x)
			goto Error
		}

		x.mode = value

		// spec: "Only the first index may be omitted; it defaults to 0."
		if e.Slice3 && (e.High == nil || e.Max == nil) {
			check.error(e.Rbrack, "2nd and 3rd index required in 3-index slice")
			goto Error
		}

		// check indices
		var ind [3]int64
		for i, expr := range []ast.Expr{e.Low, e.High, e.Max} {
			x := int64(-1)
			switch {
			case expr != nil:
				// The "capacity" is only known statically for strings, arrays,
				// and pointers to arrays, and it is the same as the length for
				// those types.
				max := int64(-1)
				if length >= 0 {
					max = length + 1
				}
				if t, ok := check.index(expr, max); ok && t >= 0 {
					x = t
				}
			case i == 0:
				// default is 0 for the first index
				x = 0
			case length >= 0:
				// default is length (== capacity) otherwise
				x = length
			}
			ind[i] = x
		}

		// constant indices must be in range
		// (check.index already checks that existing indices >= 0)
	L:
		for i, x := range ind[:len(ind)-1] {
			if x > 0 {
				for _, y := range ind[i+1:] {
					if y >= 0 && x > y {
						check.errorf(e.Rbrack, "invalid slice indices: %d > %d", x, y)
						break L // only report one error, ok to continue
					}
				}
			}
		}

	case *ast.TypeAssertExpr:
		check.expr(x, e.X)
		if x.mode == invalid {
			goto Error
		}
		xtyp, _ := x.typ.Underlying().(*Interface)
		if xtyp == nil {
			check.invalidOp(x.pos(), "%s is not an interface", x)
			goto Error
		}
		// x.(type) expressions are handled explicitly in type switches
		if e.Type == nil {
			check.invalidAST(e.Pos(), "use of .(type) outside type switch")
			goto Error
		}
		T := check.typ(e.Type)
		if T == Typ[Invalid] {
			goto Error
		}
		check.typeAssertion(x.pos(), x, xtyp, T)
		x.mode = commaok
		x.typ = T

	case *ast.CallExpr:
		return check.call(x, e)

	case *ast.StarExpr:
		check.exprOrType(x, e.X)
		switch x.mode {
		case invalid:
			goto Error
		case typexpr:
			x.typ = &Pointer{base: x.typ}
		default:
			if typ, ok := x.typ.Underlying().(*Pointer); ok {
				x.mode = variable
				x.typ = typ.base
			} else {
				check.invalidOp(x.pos(), "cannot indirect %s", x)
				goto Error
			}
		}

	case *ast.UnaryExpr:
		check.expr(x, e.X)
		if x.mode == invalid {
			goto Error
		}
		check.unary(x, e, e.Op)
		if x.mode == invalid {
			goto Error
		}
		if e.Op == token.ARROW {
			x.expr = e
			return statement // receive operations may appear in statement context
		}

	case *ast.BinaryExpr:
		check.binary(x, e, e.X, e.Y, e.Op)
		if x.mode == invalid {
			goto Error
		}

	case *ast.KeyValueExpr:
		// key:value expressions are handled in composite literals
		check.invalidAST(e.Pos(), "no key:value expected")
		goto Error

	case *ast.ArrayType, *ast.StructType, *ast.FuncType,
		*ast.InterfaceType, *ast.MapType, *ast.ChanType:
		x.mode = typexpr
		x.typ = check.typ(e)
		// Note: rawExpr (caller of exprInternal) will call check.recordTypeAndValue
		// even though check.typ has already called it. This is fine as both
		// times the same expression and type are recorded. It is also not a
		// performance issue because we only reach here for composite literal
		// types, which are comparatively rare.

	default:
		panic(fmt.Sprintf("%s: unknown expression type %T", check.fset.Position(e.Pos()), e))
	}

	// everything went well
	x.expr = e
	return expression

Error:
	x.mode = invalid
	x.expr = e
	return statement // avoid follow-up errors
}

// typeAssertion checks that x.(T) is legal; xtyp must be the type of x.
func (check *Checker) typeAssertion(pos token.Pos, x *operand, xtyp *Interface, T Type) {
	method, wrongType := assertableTo(xtyp, T)
	if method == nil {
		return
	}

	var msg string
	if wrongType {
		msg = "wrong type for method"
	} else {
		msg = "missing method"
	}
	check.errorf(pos, "%s cannot have dynamic type %s (%s %s)", x, T, msg, method.name)
}

// expr typechecks expression e and initializes x with the expression value.
// If an error occurred, x.mode is set to invalid.
//
func (check *Checker) expr(x *operand, e ast.Expr) {
	check.rawExpr(x, e, nil)
	var msg string
	switch x.mode {
	default:
		return
	case novalue:
		msg = "used as value"
	case builtin:
		msg = "must be called"
	case typexpr:
		msg = "is not an expression"
	}
	check.errorf(x.pos(), "%s %s", x, msg)
	x.mode = invalid
}

// exprWithHint typechecks expression e and initializes x with the expression value.
// If an error occurred, x.mode is set to invalid.
// If hint != nil, it is the type of a composite literal element.
//
func (check *Checker) exprWithHint(x *operand, e ast.Expr, hint Type) {
	assert(hint != nil)
	check.rawExpr(x, e, hint)
	var msg string
	switch x.mode {
	default:
		return
	case novalue:
		msg = "used as value"
	case builtin:
		msg = "must be called"
	case typexpr:
		msg = "is not an expression"
	}
	check.errorf(x.pos(), "%s %s", x, msg)
	x.mode = invalid
}

// exprOrType typechecks expression or type e and initializes x with the expression value or type.
// If an error occurred, x.mode is set to invalid.
//
func (check *Checker) exprOrType(x *operand, e ast.Expr) {
	check.rawExpr(x, e, nil)
	if x.mode == novalue {
		check.errorf(x.pos(), "%s used as value or type", x)
		x.mode = invalid
	}
}
