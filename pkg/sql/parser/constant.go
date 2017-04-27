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
	"fmt"
	"go/constant"
	"go/token"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pkg/errors"
)

// Constant is an constant literal expression which may be resolved to more than one type.
type Constant interface {
	Expr
	// AvailableTypes returns the ordered set of types that the Constant is able to
	// be resolved into. The order of the type slice provides a notion of precedence,
	// with the first element in the ordering being the Constant's "natural type".
	AvailableTypes() []Type
	// ResolveAsType resolves the Constant as the Datum type specified, or returns an
	// error if the Constant could not be resolved as that type. The method should only
	// be passed a type returned from AvailableTypes and should never be called more than
	// once for a given Constant.
	ResolveAsType(*SemaContext, Type) (Datum, error)
}

var _ Constant = &NumVal{}
var _ Constant = &StrVal{}

func isConstant(expr Expr) bool {
	_, ok := expr.(Constant)
	return ok
}

func typeCheckConstant(c Constant, ctx *SemaContext, desired Type) (TypedExpr, error) {
	avail := c.AvailableTypes()
	if desired != TypeAny {
		for _, typ := range avail {
			if desired.Equivalent(typ) {
				return c.ResolveAsType(ctx, desired)
			}
		}
	}

	// If a numeric constant will be promoted to a DECIMAL because it was out
	// of range of an INT, but an INT is desired, throw an error here so that
	// the error message specifically mentions the overflow.
	if desired.FamilyEqual(TypeInt) {
		if n, ok := c.(*NumVal); ok {
			_, err := n.AsInt64()
			switch err {
			case errConstOutOfRange:
				return nil, err
			case errConstNotInt:
			default:
				panic(fmt.Sprintf("unexpected error %v", err))
			}
		}
	}

	natural := avail[0]
	return c.ResolveAsType(ctx, natural)
}

func naturalConstantType(c Constant) Type {
	return c.AvailableTypes()[0]
}

// canConstantBecome returns whether the provided Constant can become resolved
// as the provided type.
func canConstantBecome(c Constant, typ Type) bool {
	avail := c.AvailableTypes()
	for _, availTyp := range avail {
		if availTyp.Equivalent(typ) {
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
func shouldConstantBecome(c Constant, typ Type) bool {
	if num, ok := c.(*NumVal); ok {
		if UnwrapType(typ) == TypeInt && num.Kind() == constant.Float {
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
	_, err := expr.AsInt64()
	return err == nil
}

// ShouldBeInt64 checks if the value naturally is an int64:
//  1   = yes
//  1.0 = no
//  1.1 = no
//  123...overflow...456 = no
//
// Currently unused so commented out, but useful even just for
// its documentation value.
func (expr *NumVal) ShouldBeInt64() bool {
	return expr.Kind() == constant.Int && expr.canBeInt64()
}

// These errors are statically allocated, because they are returned in the
// common path of AsInt64.
var errConstNotInt = errors.New("cannot represent numeric constant as an int")
var errConstOutOfRange = errors.New("numeric constant out of int64 range")

// AsInt64 returns the value as a 64-bit integer if possible, or returns an
// error if not possible. The method will set expr.resInt to the value of
// this int64 if it is successful, avoiding the need to call the method again.
func (expr *NumVal) AsInt64() (int64, error) {
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

var (
	intLikeTypes     = []Type{TypeInt, TypeOid}
	decimalLikeTypes = []Type{TypeDecimal, TypeFloat}

	numValAvailInteger             = append(intLikeTypes, decimalLikeTypes...)
	numValAvailDecimalNoFraction   = append(decimalLikeTypes, intLikeTypes...)
	numValAvailDecimalWithFraction = decimalLikeTypes
)

// AvailableTypes implements the Constant interface.
func (expr *NumVal) AvailableTypes() []Type {
	switch {
	case expr.canBeInt64():
		if expr.Kind() == constant.Int {
			return numValAvailInteger
		}
		return numValAvailDecimalNoFraction
	default:
		return numValAvailDecimalWithFraction
	}
}

// ResolveAsType implements the Constant interface.
func (expr *NumVal) ResolveAsType(ctx *SemaContext, typ Type) (Datum, error) {
	switch typ {
	case TypeInt:
		// We may have already set expr.resInt in AsInt64.
		if expr.resInt == 0 {
			if _, err := expr.AsInt64(); err != nil {
				return nil, err
			}
		}
		return &expr.resInt, nil
	case TypeFloat:
		f, _ := constant.Float64Val(expr.Value)
		expr.resFloat = DFloat(f)
		return &expr.resFloat, nil
	case TypeDecimal:
		dd := &expr.resDecimal
		s := expr.OrigString
		if s == "" {
			// TODO(nvanbenschoten): We should propagate width through constant folding so that we
			// can control precision on folded values as well.
			s = expr.ExactString()
		}
		if idx := strings.IndexRune(s, '/'); idx != -1 {
			// Handle constant.ratVal, which will return a rational string
			// like 6/7. If only we could call big.Rat.FloatString() on it...
			num, den := s[:idx], s[idx+1:]
			if err := dd.SetString(num); err != nil {
				return nil, errors.Wrapf(err, "could not evaluate numerator of %v as Datum type DDecimal "+
					"from string %q", expr, num)
			}
			// TODO(nvanbenschoten): Should we try to avoid this allocation?
			denDec, err := ParseDDecimal(den)
			if err != nil {
				return nil, errors.Wrapf(err, "could not evaluate denominator %v as Datum type DDecimal "+
					"from string %q", expr, den)
			}
			if _, err := DecimalCtx.Quo(&dd.Decimal, &dd.Decimal, &denDec.Decimal); err != nil {
				return nil, err
			}
		} else {
			if err := dd.SetString(s); err != nil {
				return nil, errors.Wrapf(err, "could not evaluate %v as Datum type DDecimal from "+
					"string %q", expr, s)
			}
		}
		return dd, nil
	case TypeOid,
		TypeRegClass,
		TypeRegNamespace,
		TypeRegProc,
		TypeRegProcedure,
		TypeRegType:

		d, err := expr.ResolveAsType(ctx, TypeInt)
		if err != nil {
			return nil, err
		}
		oid := NewDOid(*d.(*DInt))
		oid.kind = oidTypeToColType(typ)
		return oid, nil
	default:
		return nil, fmt.Errorf("could not resolve %T %v into a %T", expr, expr, typ)
	}
}

// commonConstantType returns the most constrained type which is mutually
// resolvable between a set of provided constants. It returns false if constants
// are not all of the same kind, and therefore share no common type.
//
// The function takes a slice of indexedExprs, but expects all indexedExprs
// to wrap a Constant. The reason it does no take a slice of Constants instead
// is to avoid forcing callers to allocate separate slices of Constant.
func commonConstantType(vals []indexedExpr) (Type, bool) {
	switch vals[0].e.(Constant).(type) {
	case *NumVal:
		for _, val := range vals[1:] {
			if _, ok := val.e.(Constant).(*NumVal); !ok {
				return nil, false
			}
		}
		return commonNumericConstantType(vals), true
	case *StrVal:
		for _, val := range vals[1:] {
			if _, ok := val.e.(Constant).(*StrVal); !ok {
				return nil, false
			}
		}
		return commonStringConstantType(vals), true
	default:
		panic(fmt.Sprintf("unexpected Constant type %T", vals[0].e))
	}
}

// commonNumericConstantType returns the best type which is mutually
// resolvable between a set of provided numeric constants. Here, "best"
// is defined as the smallest numeric data type that all constants can
// become without losing information.
//
// The function takes a slice of indexedExprs, but expects all indexedExprs
// to wrap a *NumVal. The reason it does no take a slice of *NumVals instead
// is to avoid forcing callers to allocate separate slices of *NumVals.
func commonNumericConstantType(vals []indexedExpr) Type {
	for _, c := range vals {
		if !shouldConstantBecome(c.e.(*NumVal), TypeInt) {
			return TypeDecimal
		}
	}
	return TypeInt
}

// commonStringConstantType returns the best type which is shared
// between a set of provided string constants. Here, "best" is defined as
// the most specific string-like type that applies to all constants. This
// suffers from the same limitation as StrVal.AvailableTypes.
//
// The function takes a slice of indexedExprs, but expects all indexedExprs
// to wrap a *StrVal. The reason it does no take a slice of *StrVals instead
// is to avoid forcing callers to allocate separate slices of *StrVals.
func commonStringConstantType(vals []indexedExpr) Type {
	for _, c := range vals {
		if c.e.(*StrVal).bytesEsc {
			return TypeBytes
		}
	}
	return TypeString
}

// StrVal represents a constant string value.
type StrVal struct {
	// We could embed a constant.Value here (like NumVal) and use the stringVal implementation,
	// but that would have extra overhead without much of a benefit. However, it would make
	// constant folding (below) a little more straightforward.
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

var (
	strValAvailAllParsable = []Type{
		TypeString,
		TypeBytes,
		TypeBool,
		TypeDate,
		TypeTimestamp,
		TypeTimestampTZ,
		TypeInterval,
	}
	strValAvailBytesString = []Type{TypeBytes, TypeString}
	strValAvailBytes       = []Type{TypeBytes}
)

// AvailableTypes implements the Constant interface.
//
// To fully take advantage of literal type inference, this method would
// determine exactly which types are available for a given string. This would
// entail attempting to parse the literal string as a date, a timestamp, an
// interval, etc. and having more fine-grained results than strValAvailAllParsable.
// However, this is not feasible in practice because of the associated parsing
// overhead.
//
// Conservative approaches like checking the string's length have been investigated
// to reduce ambiguity and improve type inference in some cases. When doing so, the
// length of the string literal was compared against all valid date and timestamp
// formats to quickly gain limited insight into whether parsing the string as the
// respective datum types could succeed. The hope was to eliminate impossibilities
// and constrain the returned type sets as much as possible. Unfortunately, two issues
// were found with this approach:
// - date and timestamp formats do not always imply a fixed-length valid input. For
//   instance, timestamp formats that take fractional seconds can successfully parse
//   inputs of varied length.
// - the set of date and timestamp formats are not disjoint, which means that ambiguity
//   can not be eliminated when inferring the type of string literals that use these
//   shared formats.
// While these limitations still permitted improved type inference in many cases, they
// resulted in behavior that was ultimately incomplete, resulted in unpredictable levels
// of inference, and occasionally failed to eliminate ambiguity. Further heuristics could
// have been applied to improve the accuracy of the inference, like checking that all
// or some characters were digits, but it would not have circumvented the fundamental
// issues here. Fully parsing the literal into each type would be the only way to
// concretely avoid the issue of unpredictable inference behavior.
func (expr *StrVal) AvailableTypes() []Type {
	if !expr.bytesEsc {
		return strValAvailAllParsable
	}
	if utf8.ValidString(expr.s) {
		return strValAvailBytesString
	}
	return strValAvailBytes
}

// ResolveAsType implements the Constant interface.
func (expr *StrVal) ResolveAsType(ctx *SemaContext, typ Type) (Datum, error) {
	switch typ {
	case TypeString:
		expr.resString = DString(expr.s)
		return &expr.resString, nil
	case TypeName:
		expr.resString = DString(expr.s)
		return NewDNameFromDString(&expr.resString), nil
	case TypeBytes:
		expr.resBytes = DBytes(expr.s)
		return &expr.resBytes, nil
	case TypeBool:
		return ParseDBool(expr.s)
	case TypeDate:
		return ParseDDate(expr.s, ctx.getLocation())
	case TypeTimestamp:
		return ParseDTimestamp(expr.s, time.Microsecond)
	case TypeTimestampTZ:
		return ParseDTimestampTZ(expr.s, ctx.getLocation(), time.Microsecond)
	case TypeInterval:
		return ParseDInterval(expr.s)
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
	Div:   token.QUO,
}
var binaryOpToTokenIntOnly = map[BinaryOperator]token.Token{
	FloorDiv: token.QUO_ASSIGN,
	Mod:      token.REM,
	Bitand:   token.AND,
	Bitor:    token.OR,
	Bitxor:   token.XOR,
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
		switch cv := t.Expr.(type) {
		case *NumVal, *StrVal:
			return cv
		}
	case *UnaryExpr:
		switch cv := t.Expr.(type) {
		case *NumVal:
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
		switch l := t.Left.(type) {
		case *NumVal:
			if r, ok := t.Right.(*NumVal); ok {
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
						if rInt64, err := r.AsInt64(); err == nil && rInt64 >= 0 {
							return &NumVal{Value: constant.Shift(lInt, token, uint(rInt64))}
						}
					}
				}
			}
		case *StrVal:
			if r, ok := t.Right.(*StrVal); ok {
				switch t.Operator {
				case Concat:
					// When folding string-like constants, if either was byte-escaped,
					// the result is also considered byte escaped.
					return &StrVal{s: l.s + r.s, bytesEsc: l.bytesEsc || r.bytesEsc}
				}
			}
		}
	case *ComparisonExpr:
		switch l := t.Left.(type) {
		case *NumVal:
			if r, ok := t.Right.(*NumVal); ok {
				if token, ok := comparisonOpToToken[t.Operator]; ok {
					return MakeDBool(DBool(constant.Compare(l.Value, token, r.Value)))
				}
			}
		case *StrVal:
			// ComparisonExpr folding for String-like constants is not significantly different
			// from constant evalutation during normalization (because both should be exact,
			// unlike numeric comparisons). Still, folding these comparisons when possible here
			// can reduce the amount of work performed during type checking, can reduce necessary
			// allocations, and maintains symmetry with numeric constants.
			if r, ok := t.Right.(*StrVal); ok {
				switch t.Operator {
				case EQ:
					return MakeDBool(DBool(l.s == r.s))
				case NE:
					return MakeDBool(DBool(l.s != r.s))
				case LT:
					return MakeDBool(DBool(l.s < r.s))
				case LE:
					return MakeDBool(DBool(l.s <= r.s))
				case GT:
					return MakeDBool(DBool(l.s > r.s))
				case GE:
					return MakeDBool(DBool(l.s >= r.s))
				}
			}
		}
	}
	return expr
}

// foldConstantLiterals folds all constant literals using exact arithmetic.
//
// TODO(nvanbenschoten): Can this visitor be preallocated (like normalizeVisitor)?
// TODO(nvanbenschoten): Investigate normalizing associative operations to group
//     constants together and permit further numeric constant folding.
func foldConstantLiterals(expr Expr) (Expr, error) {
	v := constantFolderVisitor{}
	expr, _ = WalkExpr(v, expr)
	return expr, nil
}
