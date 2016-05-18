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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package parser

import (
	"bytes"
	"errors"
	"fmt"
	"go/constant"
	"go/token"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/util/decimal"
	"gopkg.in/inf.v0"
)

// Constant is an constant literal expression which may be resolved to more than one type.
type Constant interface {
	Expr
	// AvailableTypes returns the ordered set of types that the Constant is able to
	// be resolved into. The order of the type slice provides a notion of precedence,
	// with the first element in the ordering being the Constant's "natural type".
	AvailableTypes() []Datum
	// ResolveAsType resolves the Constant as the Datum type specified, or returns an
	// error if the Constant could not be resolved as that type. The method should only
	// be passed a type returned from AvailableTypes and should never be called more than
	// once for a given Constant.
	ResolveAsType(Datum) (Datum, error)
}

var _ Constant = &NumVal{}
var _ Constant = &StrVal{}

func isNumericConstant(expr Expr) bool {
	_, ok := expr.(*NumVal)
	return ok
}

func typeCheckConstant(c Constant, desired Datum) (TypedExpr, error) {
	avail := c.AvailableTypes()
	if desired != nil {
		for _, typ := range avail {
			if desired.TypeEqual(typ) {
				return c.ResolveAsType(desired)
			}
		}
	}

	natural := avail[0]
	return c.ResolveAsType(natural)
}

func naturalConstantType(c Constant) Datum {
	return c.AvailableTypes()[0]
}

// canConstantBecome returns whether the provided Constant can become resolved
// as the provided type.
func canConstantBecome(c Constant, typ Datum) bool {
	avail := c.AvailableTypes()
	for _, availTyp := range avail {
		if availTyp.TypeEqual(typ) {
			return true
		}
	}
	return false
}

// shouldConstantBecome returns whether the provided Constant should or
// should not become the provided type. The function is meant to be differentiated
// from canConstantBecome in that it will exclude certain (Constant, Type) resolution
// pairs that are possible, but not desirable.
//
// An example of this is resolving a floating point numeric constant without a value
// past the decimal point as an DInt. This is possible, but it is not desirable.
func shouldConstantBecome(c Constant, typ Datum) bool {
	if num, ok := c.(*NumVal); ok {
		if typ.TypeEqual(TypeInt) && num.Kind() == constant.Float {
			return false
		}
	}
	return canConstantBecome(c, typ)
}

// NumVal represents a constant numeric value.
type NumVal struct {
	constant.Value

	// We preserve the "original" string representation (before folding).
	OrigString string

	// The following fields are used to avoid allocating Datums on type resolution.
	resInt     DInt
	resFloat   DFloat
	resDecimal DDecimal
}

// Format implements the NodeFormatter interface.
func (expr *NumVal) Format(buf *bytes.Buffer, f FmtFlags) {
	s := expr.OrigString
	if s == "" {
		s = expr.Value.String()
	}
	buf.WriteString(s)
}

// canBeInt64 checks if it's possible for the value to become an int64:
//  1   = yes
//  1.0 = yes
//  1.1 = no
//  123...overflow...456 = no
func (expr *NumVal) canBeInt64() bool {
	_, err := expr.asInt64()
	return err == nil
}

// shouldBeInt64 checks if the value naturally is an int64:
//  1   = yes
//  1.0 = no
//  1.1 = no
//  123...overflow...456 = no
//
// Currently unused so commented out, but useful even just for
// its documentation value.
// func (expr *NumVal) shouldBeInt64() bool {
// 	return expr.Kind() == constant.Int && expr.canBeInt64()
// }

// These errors are statically allocated, because they are returned in the
// common path of asInt64.
var errConstNotInt = errors.New("cannot represent numeric constant as an int")
var errConstOutOfRange = errors.New("numeric constant out of int64 range")

// asInt64 returns the value as a 64-bit integer if possible, or returns an
// error if not possible. The method will set expr.resInt to the value of
// this int64 if it is successful, avoiding the need to call the method again.
func (expr *NumVal) asInt64() (int64, error) {
	intVal, ok := expr.asConstantInt()
	if !ok {
		return 0, errConstNotInt
	}
	i, exact := constant.Int64Val(intVal)
	if !exact {
		return 0, errConstOutOfRange
	}
	expr.resInt = DInt(i)
	return i, nil
}

// asConstantInt returns the value as an constant.Int if possible, along
// with a flag indicating whether the conversion was possible.
func (expr *NumVal) asConstantInt() (constant.Value, bool) {
	intVal := constant.ToInt(expr.Value)
	if intVal.Kind() == constant.Int {
		return intVal, true
	}
	return nil, false
}

var numValAvailIntFloatDec = []Datum{TypeInt, TypeDecimal, TypeFloat}
var numValAvailDecFloatInt = []Datum{TypeDecimal, TypeFloat, TypeInt}
var numValAvailDecFloat = []Datum{TypeDecimal, TypeFloat}

// var numValAvailDec = []Datum{TypeDecimal}

// AvailableTypes implements the Constant interface.
func (expr *NumVal) AvailableTypes() []Datum {
	switch {
	case expr.canBeInt64():
		if expr.Kind() == constant.Int {
			return numValAvailIntFloatDec
		}
		return numValAvailDecFloatInt
	default:
		return numValAvailDecFloat
	}
}

// ResolveAsType implements the Constant interface.
func (expr *NumVal) ResolveAsType(typ Datum) (Datum, error) {
	switch {
	case typ.TypeEqual(TypeInt):
		// We may have already set expr.resInt in asInt64.
		if expr.resInt == 0 {
			if _, err := expr.asInt64(); err != nil {
				return nil, err
			}
		}
		return &expr.resInt, nil
	case typ.TypeEqual(TypeFloat):
		f, _ := constant.Float64Val(expr.Value)
		expr.resFloat = DFloat(f)
		return &expr.resFloat, nil
	case typ.TypeEqual(TypeDecimal):
		dd := &expr.resDecimal
		s := expr.OrigString
		if s == "" {
			// TODO(nvanbenschoten) We should propagate width through constant folding so that we
			// can control precision on folded values as well.
			s = expr.ExactString()
		}
		if idx := strings.IndexRune(s, '/'); idx != -1 {
			// Handle constant.ratVal, which will return a rational string
			// like 6/7. If only we could call big.Rat.FloatString() on it...
			num, den := s[:idx], s[idx+1:]
			if _, ok := dd.SetString(num); !ok {
				return nil, fmt.Errorf("could not evaluate numerator of %v as Datum type DDecimal "+
					"from string %q", expr, num)
			}
			// TODO(nvanbenschoten) Should we try to avoid this allocation?
			denDec := new(inf.Dec)
			if _, ok := denDec.SetString(den); !ok {
				return nil, fmt.Errorf("could not evaluate denominator %v as Datum type DDecimal "+
					"from string %q", expr, den)
			}
			dd.QuoRound(&dd.Dec, denDec, decimal.Precision, inf.RoundHalfUp)
		} else {
			// TODO(nvanbenschoten) Handling e will not be necessary once the TODO about the
			// OrigString workaround from above is addressed.
			eScale := inf.Scale(0)
			if eIdx := strings.IndexRune(s, 'e'); eIdx != -1 {
				eInt, err := strconv.ParseInt(s[eIdx+1:], 10, 32)
				if err != nil {
					return nil, fmt.Errorf("could not evaluate %v as Datum type DDecimal from "+
						"string %q: %v", expr, s, err)
				}
				eScale = inf.Scale(eInt)
				s = s[:eIdx]
			}
			if _, ok := dd.SetString(s); !ok {
				return nil, fmt.Errorf("could not evaluate %v as Datum type DDecimal from "+
					"string %q", expr, s)
			}
			dd.SetScale(dd.Scale() - eScale)
		}
		return dd, nil
	default:
		return nil, fmt.Errorf("could not resolve %T %v into a %T", expr, expr, typ)
	}
}

// commonNumericConstantType returns the best constant type which is shared
// between a set of provided numeric constants. Here, "best" is defined as
// the smallest numeric data type which will not lose information.
//
// The function takes a slice of indexedExprs, but expects all indexedExprs
// to wrap a *NumVal. The reason it does no take a slice of *NumVals instead
// is to avoid forcing callers to allocate separate slices of *NumVals.
func commonNumericConstantType(vals []indexedExpr) Datum {
	for _, c := range vals {
		if !shouldConstantBecome(c.e.(*NumVal), TypeInt) {
			return TypeDecimal
		}
	}
	return TypeInt
}

// StrVal represents a constant string value.
type StrVal struct {
	s        string
	bytesEsc bool

	// The following fields are used to avoid allocating Datums on type resolution.
	resString DString
	resBytes  DBytes
}

// Format implements the NodeFormatter interface.
func (expr *StrVal) Format(buf *bytes.Buffer, f FmtFlags) {
	if expr.bytesEsc {
		encodeSQLBytes(buf, expr.s)
	} else {
		encodeSQLString(buf, expr.s)
	}
}

var strValAvailStringBytes = []Datum{TypeString, TypeBytes}
var strValAvailBytesString = []Datum{TypeBytes, TypeString}
var strValAvailBytes = []Datum{TypeBytes}

// AvailableTypes implements the Constant interface.
func (expr *StrVal) AvailableTypes() []Datum {
	if !expr.bytesEsc {
		return strValAvailStringBytes
	}
	if utf8.ValidString(expr.s) {
		return strValAvailBytesString
	}
	return strValAvailBytes
}

// ResolveAsType implements the Constant interface.
func (expr *StrVal) ResolveAsType(typ Datum) (Datum, error) {
	switch typ {
	case TypeString:
		expr.resString = DString(expr.s)
		return &expr.resString, nil
	case TypeBytes:
		expr.resBytes = DBytes(expr.s)
		return &expr.resBytes, nil
	default:
		return nil, fmt.Errorf("could not resolve %T %v into a %T", expr, expr, typ)
	}
}

type constantFolderVisitor struct{}

var _ Visitor = constantFolderVisitor{}

func (constantFolderVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	return true, expr
}

var unaryOpToToken = map[UnaryOperator]token.Token{
	UnaryPlus:  token.ADD,
	UnaryMinus: token.SUB,
}
var unaryOpToTokenIntOnly = map[UnaryOperator]token.Token{
	UnaryComplement: token.XOR,
}
var binaryOpToToken = map[BinaryOperator]token.Token{
	Plus:  token.ADD,
	Minus: token.SUB,
	Mult:  token.MUL,
	Div:   token.QUO, // token.QUO_ASSIGN to force integer division.
}
var binaryOpToTokenIntOnly = map[BinaryOperator]token.Token{
	Mod:    token.REM,
	Bitand: token.AND,
	Bitor:  token.OR,
	Bitxor: token.XOR,
}
var binaryShiftOpToToken = map[BinaryOperator]token.Token{
	LShift: token.SHL,
	RShift: token.SHR,
}
var comparisonOpToToken = map[ComparisonOperator]token.Token{
	EQ: token.EQL,
	NE: token.NEQ,
	LT: token.LSS,
	LE: token.LEQ,
	GT: token.GTR,
	GE: token.GEQ,
}

func (constantFolderVisitor) VisitPost(expr Expr) (retExpr Expr) {
	defer func() {
		// go/constant operations can panic for a number of reasons (like division
		// by zero), but it's difficult to preemptively detect when they will. It's
		// safest to just recover here without folding the expression and let
		// normalization or evaluation deal with error handling.
		if r := recover(); r != nil {
			retExpr = expr
		}
	}()
	switch t := expr.(type) {
	case *ParenExpr:
		if cv, ok := t.Expr.(*NumVal); ok {
			return cv
		}
	case *UnaryExpr:
		if cv, ok := t.Expr.(*NumVal); ok {
			if token, ok := unaryOpToToken[t.Operator]; ok {
				return &NumVal{Value: constant.UnaryOp(token, cv.Value, 0)}
			}
			if token, ok := unaryOpToTokenIntOnly[t.Operator]; ok {
				if intVal, ok := cv.asConstantInt(); ok {
					return &NumVal{Value: constant.UnaryOp(token, intVal, 0)}
				}
			}
		}
	case *BinaryExpr:
		l, okL := t.Left.(*NumVal)
		r, okR := t.Right.(*NumVal)
		if okL && okR {
			if token, ok := binaryOpToToken[t.Operator]; ok {
				return &NumVal{Value: constant.BinaryOp(l.Value, token, r.Value)}
			}
			if token, ok := binaryOpToTokenIntOnly[t.Operator]; ok {
				if lInt, ok := l.asConstantInt(); ok {
					if rInt, ok := r.asConstantInt(); ok {
						return &NumVal{Value: constant.BinaryOp(lInt, token, rInt)}
					}
				}
			}
			if token, ok := binaryShiftOpToToken[t.Operator]; ok {
				if lInt, ok := l.asConstantInt(); ok {
					if rInt64, err := r.asInt64(); err == nil && rInt64 >= 0 {
						return &NumVal{Value: constant.Shift(lInt, token, uint(rInt64))}
					}
				}
			}
		}
	case *ComparisonExpr:
		l, okL := t.Left.(*NumVal)
		r, okR := t.Right.(*NumVal)
		if okL && okR {
			if token, ok := comparisonOpToToken[t.Operator]; ok {
				return MakeDBool(DBool(constant.Compare(l.Value, token, r.Value)))
			}
		}
	}
	return expr
}

// foldNumericConstants folds all numeric constants using exact arithmetic.
//
// TODO(nvanbenschoten) Can this visitor be preallocated (like normalizeVisitor)?
// TODO(nvanbenschoten) Do we also want to fold string-like constants?
// TODO(nvanbenschoten) Investigate normalizing associative operations to group
//     constants together and permit further numeric constant folding.
func foldNumericConstants(expr Expr) (Expr, error) {
	v := constantFolderVisitor{}
	expr, _ = WalkExpr(v, expr)
	return expr, nil
}

type constantTypeVisitor struct {
	cfv constantFolderVisitor
}

var _ Visitor = constantTypeVisitor{}

func (constantTypeVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	switch t := expr.(type) {
	case Constant:
		typedConst, err := t.TypeCheck(nil, nil)
		if err != nil {
			panic(err)
		}
		return false, typedConst
	}
	return true, expr
}

func (constantTypeVisitor) VisitPost(expr Expr) (retExpr Expr) { return expr }

// TypeConstants type checks all Constant literal expressions, resolving
// them as the Datum representations of their values. Before doing so,
// it first folds all numeric constants.
//
// This means that Constants will become TypedExprs so that they can be
// used in contexts which expect a TypedExpr tree (such as Normalization).
// As such, the function is primarily intended for use while testing
// expressions where full type checking is not desired.
//
// TODO(nvanbenschoten) Can this visitor be preallocated (like normalizeVisitor)?
func TypeConstants(expr Expr) (TypedExpr, error) {
	v := constantTypeVisitor{}
	expr, _ = WalkExpr(v.cfv, expr)
	expr, _ = WalkExpr(v, expr)
	return expr.(TypedExpr), nil
}
