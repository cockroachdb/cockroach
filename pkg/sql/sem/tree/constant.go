// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"context"
	"go/constant"
	"go/token"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
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
	// ResolveAsType resolves the Constant as the specified type, or returns an
	// error if the Constant could not be resolved as that type. The method should
	// only be passed a type returned from AvailableTypes and should never be
	// called more than once for a given Constant.
	//
	// The returned expression is either a Datum or a CastExpr wrapping a Datum;
	// the latter is necessary for cases where the result would depend on the
	// context (like the timezone or the current time).
	ResolveAsType(context.Context, *SemaContext, *types.T) (TypedExpr, error)
}

var _ Constant = &NumVal{}
var _ Constant = &StrVal{}

func isConstant(expr Expr) bool {
	_, ok := expr.(Constant)
	return ok
}

func typeCheckConstant(
	ctx context.Context, semaCtx *SemaContext, c Constant, desired *types.T,
) (ret TypedExpr, err error) {
	avail := c.AvailableTypes()
	if !desired.IsAmbiguous() {
		for _, typ := range avail {
			if desired.Equivalent(typ) {
				return c.ResolveAsType(ctx, semaCtx, desired)
			}
		}
	}

	// If a numeric constant will be promoted to a DECIMAL because it was out
	// of range of an INT, but an INT is desired, throw an error here so that
	// the error message specifically mentions the overflow.
	if desired.Family() == types.IntFamily {
		if n, ok := c.(*NumVal); ok {
			_, err := n.AsInt64()
			switch {
			case errors.Is(err, errConstOutOfRange64):
				return nil, err
			case errors.Is(err, errConstNotInt):
			default:
				return nil, errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error")
			}
		}
	}

	natural := avail[0]
	return c.ResolveAsType(ctx, semaCtx, natural)
}

func naturalConstantType(c Constant) *types.T {
	return c.AvailableTypes()[0]
}

// canConstantBecome returns whether the provided Constant can become resolved
// as a type that is Equivalent to the given type.
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
	// value is the constant number, without any sign information.
	value constant.Value
	// negative is the sign bit to add to any interpretation of the
	// value or origString fields.
	negative bool
	// origString is the "original" string representation (before
	// folding). This should remain sign-less.
	origString string

	// The following fields are used to avoid allocating Datums on type resolution.
	resInt     DInt
	resFloat   DFloat
	resDecimal DDecimal
}

var _ Constant = &NumVal{}

// NewNumVal constructs a new NumVal instance. This is used during parsing and
// in tests.
func NewNumVal(value constant.Value, origString string, negative bool) *NumVal {
	return &NumVal{value: value, origString: origString, negative: negative}
}

// Kind implements the constant.Value interface.
func (expr *NumVal) Kind() constant.Kind {
	return expr.value.Kind()
}

// ExactString implements the constant.Value interface.
func (expr *NumVal) ExactString() string {
	return expr.value.ExactString()
}

// OrigString returns the origString field.
func (expr *NumVal) OrigString() string {
	return expr.origString
}

// SetNegative sets the negative field to true. The parser calls this when it
// identifies a negative constant.
func (expr *NumVal) SetNegative() {
	expr.negative = true
}

// Negate sets the negative field to the opposite of its current value. The
// parser calls this to simplify unary negation expressions.
func (expr *NumVal) Negate() {
	expr.negative = !expr.negative
}

// Format implements the NodeFormatter interface.
func (expr *NumVal) Format(ctx *FmtCtx) {
	s := expr.origString
	if s == "" {
		s = expr.value.String()
	}
	if expr.negative {
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
var errConstNotInt = pgerror.New(pgcode.NumericValueOutOfRange, "cannot represent numeric constant as an int")
var errConstOutOfRange64 = pgerror.New(pgcode.NumericValueOutOfRange, "numeric constant out of int64 range")
var errConstOutOfRange32 = pgerror.New(pgcode.NumericValueOutOfRange, "numeric constant out of int32 range")

// AsInt64 returns the value as a 64-bit integer if possible, or returns an
// error if not possible. The method will set expr.resInt to the value of
// this int64 if it is successful, avoiding the need to call the method again.
func (expr *NumVal) AsInt64() (int64, error) {
	intVal, ok := expr.AsConstantInt()
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
	intVal, ok := expr.AsConstantInt()
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

// AsConstantValue returns the value as a constant numerical value, with the proper sign
// as given by expr.negative.
func (expr *NumVal) AsConstantValue() constant.Value {
	v := expr.value
	if expr.negative {
		v = constant.UnaryOp(token.SUB, v, 0)
	}
	return v
}

// AsConstantInt returns the value as an constant.Int if possible, along
// with a flag indicating whether the conversion was possible.
// The result contains the proper sign as per expr.negative.
func (expr *NumVal) AsConstantInt() (constant.Value, bool) {
	v := expr.AsConstantValue()
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
func (expr *NumVal) ResolveAsType(
	ctx context.Context, semaCtx *SemaContext, typ *types.T,
) (TypedExpr, error) {
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
		f, _ := constant.Float64Val(expr.value)
		if expr.negative {
			f = -f
		}
		expr.resFloat = DFloat(f)
		return &expr.resFloat, nil
	case types.DecimalFamily:
		dd := &expr.resDecimal
		s := expr.origString
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
				return nil, pgerror.Wrapf(err, pgcode.Syntax,
					"could not evaluate numerator of %v as Datum type DDecimal from string %q",
					expr, num)
			}
			// TODO(nvanbenschoten): Should we try to avoid this allocation?
			denDec, err := ParseDDecimal(den)
			if err != nil {
				return nil, pgerror.Wrapf(err, pgcode.Syntax,
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
				return nil, pgerror.Wrapf(err, pgcode.Syntax,
					"could not evaluate %v as Datum type DDecimal from string %q", expr, s)
			}
		}
		if !dd.IsZero() {
			// Negative zero does not exist for DECIMAL, in that case we ignore the
			// sign. Otherwise XOR the signs of the expr and the decimal value
			// contained in the expr, since the negative may have been folded into the
			// inner decimal.
			dd.Negative = dd.Negative != expr.negative
		}
		return dd, nil
	case types.OidFamily:
		d, err := expr.ResolveAsType(ctx, semaCtx, types.Int)
		if err != nil {
			return nil, err
		}
		oid := NewDOid(*d.(*DInt))
		return oid, nil
	default:
		return nil, errors.AssertionFailedf("could not resolve %T %v into a %T", expr, expr, typ)
	}
}

// intersectTypeSlices returns a slice of all the types that are in both of the
// input slices that have the same OID.
func intersectTypeSlices(xs, ys []*types.T) (out []*types.T) {
	seen := make(map[oid.Oid]struct{})
	for _, x := range xs {
		for _, y := range ys {
			_, ok := seen[x.Oid()]
			if x.Oid() == y.Oid() && !ok {
				out = append(out, x)
			}
		}
		seen[x.Oid()] = struct{}{}
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
		// Note: String is deliberately first, to make sure that "string" is the
		// default type that raw strings get parsed into, without any casts or type
		// assertions.
		types.String,
		types.Bytes,
		types.Bool,
		types.Int,
		types.Float,
		types.Decimal,
		types.Date,
		types.StringArray,
		types.IntArray,
		types.FloatArray,
		types.DecimalArray,
		types.BoolArray,
		types.Box2D,
		types.Geography,
		types.Geometry,
		types.Time,
		types.TimeTZ,
		types.Timestamp,
		types.TimestampTZ,
		types.Interval,
		types.Uuid,
		types.DateArray,
		types.TimeArray,
		types.TimeTZArray,
		types.TimestampArray,
		types.TimestampTZArray,
		types.IntervalArray,
		types.UUIDArray,
		types.INet,
		types.Jsonb,
		types.VarBit,
		types.AnyEnum,
		types.INetArray,
		types.VarBitArray,
	}
	// StrValAvailBytes is the set of types convertible to byte array.
	StrValAvailBytes = []*types.T{types.Bytes, types.Uuid, types.String, types.AnyEnum}
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
func (expr *StrVal) ResolveAsType(
	ctx context.Context, semaCtx *SemaContext, typ *types.T,
) (TypedExpr, error) {
	if expr.scannedAsBytes {
		// We're looking at typing a byte literal constant into some value type.
		switch typ.Family() {
		case types.BytesFamily:
			expr.resBytes = DBytes(expr.s)
			return &expr.resBytes, nil
		case types.EnumFamily:
			return MakeDEnumFromPhysicalRepresentation(typ, []byte(expr.s))
		case types.UuidFamily:
			return ParseDUuidFromBytes([]byte(expr.s))
		case types.StringFamily:
			expr.resString = DString(adjustStringValueToType(typ, expr.s))
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
		expr.resString = DString(adjustStringValueToType(typ, expr.s))
		return &expr.resString, nil

	case types.BytesFamily:
		return ParseDByte(expr.s)

	default:
		val, dependsOnContext, err := ParseAndRequireString(typ, expr.s, dummyParseTimeContext{})
		if err != nil {
			return nil, err
		}
		if !dependsOnContext {
			return val, nil
		}
		// Interpreting a string as one of these types may depend on the timezone or
		// the current time; the value won't be safe to reuse later. So in this case
		// we return a CastExpr and let the conversion happen at evaluation time. We
		// still want to error out if the conversion is not possible though (this is
		// used when resolving overloads).
		expr.resString = DString(expr.s)
		c := NewTypedCastExpr(&expr.resString, typ)
		return c.TypeCheck(ctx, semaCtx, typ)
	}
}

// dummyParseTimeContext is a ParseTimeContext when used for parsing timestamps
// during type-checking. Note that results that depend on the context are not
// retained in the AST.
type dummyParseTimeContext struct{}

var _ ParseTimeContext = dummyParseTimeContext{}

// We can return any time, but not the zero value - it causes an error when
// parsing "yesterday".
var dummyTime = time.Date(2000, time.January, 2, 3, 4, 5, 0, time.UTC)

func (dummyParseTimeContext) GetRelativeParseTime() time.Time {
	return dummyTime
}
