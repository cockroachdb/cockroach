// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package tree

import (
	"go/constant"
	"go/token"
	"math"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// Constant is an constant literal expression which may be resolved to more than one type.
type Constant interface {
	Expr
	// AvailableTypes returns the ordered set of types that the Constant is able to
	// be resolved into. The order of the type slice provides a notion of precedence,
	// with the first element in the ordering being the Constant's "natural type".
	AvailableTypes() []*types.T
	// DesirableTypes returns the ordered set of types that the constant would
	// prefer to be resolved into. As in AvailableTypes, the order of the returned
	// type slice provides a notion of precedence, with the first element in the
	// ordering being the Constant's "natural type." The function is meant to be
	// differentiated from AvailableTypes in that it will exclude certain types
	// that are possible, but not desirable.
	//
	// An example of this is a floating point numeric constant without a value
	// past the decimal point. It is possible to resolve this constant as a
	// decimal, but it is not desirable.
	DesirableTypes() []*types.T
	// ResolveAsType resolves the Constant as the Datum type specified, or returns an
	// error if the Constant could not be resolved as that type. The method should only
	// be passed a type returned from AvailableTypes and should never be called more than
	// once for a given Constant.
	ResolveAsType(*SemaContext, *types.T) (Datum, error)
}

var _ Constant = &NumVal{}
var _ Constant = &StrVal{}

func isConstant(expr Expr) bool {
	_, ok := expr.(Constant)
	return ok
}

func typeCheckConstant(c Constant, ctx *SemaContext, desired *types.T) (ret TypedExpr, err error) {
	avail := c.AvailableTypes()
	if desired.Family() != types.AnyFamily {
		for _, typ := range avail {
			if desired.Equivalent(typ) {
				return c.ResolveAsType(ctx, desired)
			}
		}
	}

	// If a numeric constant will be promoted to a DECIMAL because it was out
	// of range of an INT, but an INT is desired, throw an error here so that
	// the error message specifically mentions the overflow.
	if desired.Family() == types.IntFamily {
		if n, ok := c.(*NumVal); ok {
			_, err := n.AsInt64()
			switch err {
			case errConstOutOfRange64:
				return nil, err
			case errConstNotInt:
			default:
				return nil, errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error")
			}
		}
	}

	natural := avail[0]
	return c.ResolveAsType(ctx, natural)
}

func naturalConstantType(c Constant) *types.T {
	return c.AvailableTypes()[0]
}

// canConstantBecome returns whether the provided Constant can become resolved
// as the provided type.
func canConstantBecome(c Constant, typ *types.T) bool {
	avail := c.AvailableTypes()
	for _, availTyp := range avail {
		if availTyp.Equivalent(typ) {
			return true
		}
	}
	return false
}

// NumVal represents a constant numeric value.
type NumVal struct {
	constant.Value
	// Negative is the sign bit to add to any interpretation of the
	// Value or OrigString fields.
	Negative bool

	// We preserve the "original" string representation (before
	// folding). This should remain sign-less.
	OrigString string

	// The following fields are used to avoid allocating Datums on type resolution.
	resInt     DInt
	resFloat   DFloat
	resDecimal DDecimal
}

// Format implements the NodeFormatter interface.
func (expr *NumVal) Format(ctx *FmtCtx) {
	s := expr.OrigString
	if s == "" {
		s = expr.Value.String()
	}
	if expr.Negative {
		ctx.WriteByte('-')
	}
	ctx.WriteString(s)
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
func (expr *NumVal) ShouldBeInt64() bool {
	return expr.Kind() == constant.Int && expr.canBeInt64()
}

// These errors are statically allocated, because they are returned in the
// common path of AsInt64.
var errConstNotInt = pgerror.New(pgerror.CodeNumericValueOutOfRangeError, "cannot represent numeric constant as an int")
var errConstOutOfRange64 = pgerror.New(pgerror.CodeNumericValueOutOfRangeError, "numeric constant out of int64 range")
var errConstOutOfRange32 = pgerror.New(pgerror.CodeNumericValueOutOfRangeError, "numeric constant out of int32 range")

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
		return 0, errConstOutOfRange64
	}
	expr.resInt = DInt(i)
	return i, nil
}

// AsInt32 returns the value as 32-bit integer if possible, or returns
// an error if not possible. The method will set expr.resInt to the
// value of this int32 if it is successful, avoiding the need to call
// the method again.
func (expr *NumVal) AsInt32() (int32, error) {
	intVal, ok := expr.asConstantInt()
	if !ok {
		return 0, errConstNotInt
	}
	i, exact := constant.Int64Val(intVal)
	if !exact {
		return 0, errConstOutOfRange32
	}
	if i > math.MaxInt32 || i < math.MinInt32 {
		return 0, errConstOutOfRange32
	}
	expr.resInt = DInt(i)
	return int32(i), nil
}

// asConstantInt returns the value as an constant.Int if possible, along
// with a flag indicating whether the conversion was possible.
// The result contains the proper sign as per expr.Negative.
func (expr *NumVal) asConstantInt() (constant.Value, bool) {
	v := expr.Value
	if expr.Negative {
		v = constant.UnaryOp(token.SUB, v, 0)
	}
	intVal := constant.ToInt(v)
	if intVal.Kind() == constant.Int {
		return intVal, true
	}
	return nil, false
}

var (
	intLikeTypes     = []*types.T{types.Int, types.Oid}
	decimalLikeTypes = []*types.T{types.Decimal, types.Float}

	// NumValAvailInteger is the set of available integer types.
	NumValAvailInteger = append(intLikeTypes, decimalLikeTypes...)
	// NumValAvailDecimalNoFraction is the set of available integral numeric types.
	NumValAvailDecimalNoFraction = append(decimalLikeTypes, intLikeTypes...)
	// NumValAvailDecimalWithFraction is the set of available fractional numeric types.
	NumValAvailDecimalWithFraction = decimalLikeTypes
)

// AvailableTypes implements the Constant interface.
func (expr *NumVal) AvailableTypes() []*types.T {
	switch {
	case expr.canBeInt64():
		if expr.Kind() == constant.Int {
			return NumValAvailInteger
		}
		return NumValAvailDecimalNoFraction
	default:
		return NumValAvailDecimalWithFraction
	}
}

// DesirableTypes implements the Constant interface.
func (expr *NumVal) DesirableTypes() []*types.T {
	if expr.ShouldBeInt64() {
		return NumValAvailInteger
	}
	return NumValAvailDecimalWithFraction
}

// ResolveAsType implements the Constant interface.
func (expr *NumVal) ResolveAsType(ctx *SemaContext, typ *types.T) (Datum, error) {
	switch typ.Family() {
	case types.IntFamily:
		// We may have already set expr.resInt in AsInt64.
		if expr.resInt == 0 {
			if _, err := expr.AsInt64(); err != nil {
				return nil, err
			}
		}
		return &expr.resInt, nil
	case types.FloatFamily:
		f, _ := constant.Float64Val(expr.Value)
		if expr.Negative {
			f = -f
		}
		expr.resFloat = DFloat(f)
		return &expr.resFloat, nil
	case types.DecimalFamily:
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
				return nil, pgerror.Wrapf(err, pgerror.CodeSyntaxError,
					"could not evaluate numerator of %v as Datum type DDecimal from string %q",
					expr, num)
			}
			// TODO(nvanbenschoten): Should we try to avoid this allocation?
			denDec, err := ParseDDecimal(den)
			if err != nil {
				return nil, pgerror.Wrapf(err, pgerror.CodeSyntaxError,
					"could not evaluate denominator %v as Datum type DDecimal from string %q",
					expr, den)
			}
			if cond, err := DecimalCtx.Quo(&dd.Decimal, &dd.Decimal, &denDec.Decimal); err != nil {
				if cond.DivisionByZero() {
					return nil, ErrDivByZero
				}
				return nil, err
			}
		} else {
			if err := dd.SetString(s); err != nil {
				return nil, pgerror.Wrapf(err, pgerror.CodeSyntaxError,
					"could not evaluate %v as Datum type DDecimal from string %q", expr, s)
			}
		}
		if !dd.IsZero() {
			// Negative zero does not exist for DECIMAL, in that case we
			// ignore the sign.
			dd.Negative = expr.Negative
		}
		return dd, nil
	case types.OidFamily:
		d, err := expr.ResolveAsType(ctx, types.Int)
		if err != nil {
			return nil, err
		}
		oid := NewDOid(*d.(*DInt))
		oid.semanticType = typ
		return oid, nil
	default:
		return nil, errors.AssertionFailedf("could not resolve %T %v into a %T", expr, expr, typ)
	}
}

func intersectTypeSlices(xs, ys []*types.T) (out []*types.T) {
	for _, x := range xs {
		for _, y := range ys {
			if x == y {
				out = append(out, x)
			}
		}
	}
	return out
}

// commonConstantType returns the most constrained type which is mutually
// resolvable between a set of provided constants.
//
// The function takes a slice of Exprs and indexes, but expects all the indexed
// Exprs to wrap a Constant. The reason it does no take a slice of Constants
// instead is to avoid forcing callers to allocate separate slices of Constant.
func commonConstantType(vals []Expr, idxs []int) (*types.T, bool) {
	var candidates []*types.T

	for _, i := range idxs {
		availableTypes := vals[i].(Constant).DesirableTypes()
		if candidates == nil {
			candidates = availableTypes
		} else {
			candidates = intersectTypeSlices(candidates, availableTypes)
		}
	}

	if len(candidates) > 0 {
		return candidates[0], true
	}
	return nil, false
}

// StrVal represents a constant string value.
type StrVal struct {
	// We could embed a constant.Value here (like NumVal) and use the stringVal implementation,
	// but that would have extra overhead without much of a benefit. However, it would make
	// constant folding (below) a little more straightforward.
	s string

	// scannedAsBytes is true iff the input syntax was using b'...' or
	// x'....'. If false, the string is guaranteed to be a valid UTF-8
	// sequence.
	scannedAsBytes bool

	// The following fields are used to avoid allocating Datums on type resolution.
	resString DString
	resBytes  DBytes
}

// NewStrVal constructs a StrVal instance. This is used during
// parsing when interpreting a token of type SCONST, i.e. *not* using
// the b'...' or x'...' syntax.
func NewStrVal(s string) *StrVal {
	return &StrVal{s: s}
}

// NewBytesStrVal constructs a StrVal instance suitable as byte array.
// This is used during parsing when interpreting a token of type BCONST,
// i.e. using the b'...' or x'...' syntax.
func NewBytesStrVal(s string) *StrVal {
	return &StrVal{s: s, scannedAsBytes: true}
}

// RawString retrieves the underlying string of the StrVal.
func (expr *StrVal) RawString() string {
	return expr.s
}

// Format implements the NodeFormatter interface.
func (expr *StrVal) Format(ctx *FmtCtx) {
	buf, f := &ctx.Buffer, ctx.flags
	if expr.scannedAsBytes {
		lex.EncodeSQLBytes(buf, expr.s)
	} else {
		lex.EncodeSQLStringWithFlags(buf, expr.s, f.EncodeFlags())
	}
}

var (
	// StrValAvailAllParsable is the set of parsable string types.
	StrValAvailAllParsable = []*types.T{
		types.String,
		types.Bytes,
		types.Bool,
		types.Int,
		types.Float,
		types.Decimal,
		types.Date,
		types.Time,
		types.Timestamp,
		types.TimestampTZ,
		types.Interval,
		types.Uuid,
		types.INet,
		types.Jsonb,
		types.VarBit,
	}
	// StrValAvailBytes is the set of types convertible to byte array.
	StrValAvailBytes = []*types.T{types.Bytes, types.Uuid, types.String}
)

// AvailableTypes implements the Constant interface.
//
// To fully take advantage of literal type inference, this method would
// determine exactly which types are available for a given string. This would
// entail attempting to parse the literal string as a date, a timestamp, an
// interval, etc. and having more fine-grained results than StrValAvailAllParsable.
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
func (expr *StrVal) AvailableTypes() []*types.T {
	if expr.scannedAsBytes {
		return StrValAvailBytes
	}
	return StrValAvailAllParsable
}

// DesirableTypes implements the Constant interface.
func (expr *StrVal) DesirableTypes() []*types.T {
	return expr.AvailableTypes()
}

// ResolveAsType implements the Constant interface.
func (expr *StrVal) ResolveAsType(ctx *SemaContext, typ *types.T) (Datum, error) {
	if expr.scannedAsBytes {
		// We're looking at typing a byte literal constant into some value type.
		switch typ.Family() {
		case types.BytesFamily:
			expr.resBytes = DBytes(expr.s)
			return &expr.resBytes, nil
		case types.UuidFamily:
			return ParseDUuidFromBytes([]byte(expr.s))
		case types.StringFamily:
			expr.resString = DString(expr.s)
			return &expr.resString, nil
		}
		return nil, errors.AssertionFailedf("attempt to type byte array literal to %T", typ)
	}

	// Typing a string literal constant into some value type.
	switch typ.Family() {
	case types.StringFamily:
		if typ.Oid() == oid.T_name {
			expr.resString = DString(expr.s)
			return NewDNameFromDString(&expr.resString), nil
		}
		expr.resString = DString(expr.s)
		return &expr.resString, nil
	case types.BytesFamily:
		return ParseDByte(expr.s)
	}

	datum, err := parseStringAs(typ, expr.s, ctx)
	if datum == nil && err == nil {
		return nil, errors.AssertionFailedf("could not resolve %T %v into a %T", expr, expr, typ)
	}
	return datum, err
}

type constantFolderVisitor struct{}

var _ Visitor = constantFolderVisitor{}

func (constantFolderVisitor) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	return true, expr
}

var unaryOpToToken = map[UnaryOperator]token.Token{
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
			if tok, ok := unaryOpToToken[t.Operator]; ok {
				switch tok {
				case token.ADD:
					return cv
				case token.SUB:
					// We always coerce -0 to 0 everywhere else, so this can be a passthrough.
					if cv.Value.Kind() == constant.Float && constant.Compare(cv.Value, token.EQL, constant.MakeFloat64(0)) {
						return cv
					}
				}
				return &NumVal{Value: constant.UnaryOp(tok, cv.Value, 0)}
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
				// Explicitly ignore shift operators so the expression is evaluated as a
				// non-const. This is because 1 << 63 as a 64-bit int (which is a negative
				// number due to 2s complement) is different than 1 << 63 as constant,
				// which is positive.
			}
		case *StrVal:
			if r, ok := t.Right.(*StrVal); ok {
				switch t.Operator {
				case Concat:
					// When folding string-like constants, if either was a byte
					// array literal, the result is also a byte literal.
					return &StrVal{s: l.s + r.s, scannedAsBytes: l.scannedAsBytes || r.scannedAsBytes}
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

// FoldConstantLiterals folds all constant literals using exact arithmetic.
//
// TODO(nvanbenschoten): Can this visitor be preallocated (like normalizeVisitor)?
// TODO(nvanbenschoten): Investigate normalizing associative operations to group
//     constants together and permit further numeric constant folding.
func FoldConstantLiterals(expr Expr) (Expr, error) {
	v := constantFolderVisitor{}
	expr, _ = WalkExpr(v, expr)
	return expr, nil
}
