// Copyright 2015 The Cockroach Authors.
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
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/stringencoding"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/tsearch"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq/oid"
	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

var (
	constDBoolTrue  DBool = true
	constDBoolFalse DBool = false

	// DBoolTrue is a pointer to the DBool(true) value and can be used in
	// comparisons against Datum types.
	DBoolTrue = &constDBoolTrue
	// DBoolFalse is a pointer to the DBool(false) value and can be used in
	// comparisons against Datum types.
	DBoolFalse = &constDBoolFalse

	// DNull is the NULL Datum.
	DNull Datum = dNull{}

	// DZero is the zero-valued integer Datum.
	DZero = NewDInt(0)

	// DTimeMaxTimeRegex is a compiled regex for parsing the 24:00 time value.
	DTimeMaxTimeRegex = regexp.MustCompile(`^([0-9-]*(\s|T))?\s*24:00(:00(.0+)?)?\s*$`)

	// The maximum timestamp Golang can represents is represented as UNIX
	// time timeutil.Unix(-9223372028715321601, 0).
	// However, this causes errors as we cannot reliably sort as we use
	// UNIX time in the key encoding, and 9223372036854775807 > -9223372028715321601
	// but timeutil.Unix(9223372036854775807, 0) < timeutil.Unix(-9223372028715321601, 0).
	//
	// To be compatible with pgwire, we only support the published min/max for
	// postgres 4714 BC (JULIAN = 0) - 4713 in their docs - and 294276 AD.

	// MaxSupportedTime is the maximum time we support parsing.
	MaxSupportedTime = timeutil.Unix(9224318016000-1, 999999000) // 294276-12-31 23:59:59.999999
	// MinSupportedTime is the minimum time we support parsing.
	MinSupportedTime = timeutil.Unix(-210866803200, 0) // 4714-11-24 00:00:00+00 BC
)

// CompareContext represents the dependencies used to evaluate comparisons
// between datums.
type CompareContext interface {

	// UnwrapDatum will unwrap the OIDs and potentially the placeholders.
	UnwrapDatum(d Datum) Datum
	GetLocation() *time.Location
	GetRelativeParseTime() time.Time

	// MustGetPlaceholderValue is used to compare Datum
	MustGetPlaceholderValue(p *Placeholder) Datum
}

// Datum represents a SQL value.
type Datum interface {
	TypedExpr

	// AmbiguousFormat indicates whether the result of formatting this Datum can
	// be interpreted into more than one type. Used with
	// fmtFlags.disambiguateDatumTypes.
	AmbiguousFormat() bool

	// Compare returns -1 if the receiver is less than other, 0 if receiver is
	// equal to other and +1 if receiver is greater than other.
	// TODO(rafi): Migrate all usages of this to CompareError, then delete this.
	Compare(ctx CompareContext, other Datum) int

	// CompareError is the same as Compare, but it returns an error instead of
	// panicking.
	CompareError(ctx CompareContext, other Datum) (int, error)

	// Prev returns the previous datum and true, if one exists, or nil and false.
	// The previous datum satisfies the following definition: if the receiver is
	// "b" and the returned datum is "a", then for every compatible datum "x", it
	// holds that "x < b" is true if and only if "x <= a" is true.
	//
	// The return value is undefined if IsMin(_ *EvalContext) returns true.
	//
	// TODO(#12022): for DTuple, the contract is actually that "x < b" (SQL order,
	// where NULL < x is unknown for all x) is true only if "x <= a"
	// (.Compare/encoding order, where NULL <= x is true for all x) is true. This
	// is okay for now: the returned datum is used only to construct a span, which
	// uses .Compare/encoding order and is guaranteed to be large enough by this
	// weaker contract. The original filter expression is left in place to catch
	// false positives.
	Prev(ctx CompareContext) (Datum, bool)

	// IsMin returns true if the datum is equal to the minimum value the datum
	// type can hold.
	IsMin(ctx CompareContext) bool

	// Next returns the next datum and true, if one exists, or nil and false
	// otherwise. The next datum satisfies the following definition: if the
	// receiver is "a" and the returned datum is "b", then for every compatible
	// datum "x", it holds that "x > a" is true if and only if "x >= b" is true.
	//
	// The return value is undefined if IsMax(_ *EvalContext) returns true.
	//
	// TODO(#12022): for DTuple, the contract is actually that "x > a" (SQL order,
	// where x > NULL is unknown for all x) is true only if "x >= b"
	// (.Compare/encoding order, where x >= NULL is true for all x) is true. This
	// is okay for now: the returned datum is used only to construct a span, which
	// uses .Compare/encoding order and is guaranteed to be large enough by this
	// weaker contract. The original filter expression is left in place to catch
	// false positives.
	Next(ctx CompareContext) (Datum, bool)

	// IsMax returns true if the datum is equal to the maximum value the datum
	// type can hold.
	IsMax(ctx CompareContext) bool

	// Max returns the upper value and true, if one exists, otherwise
	// nil and false. Used By Prev().
	Max(ctx CompareContext) (Datum, bool)

	// Min returns the lower value, if one exists, otherwise nil and
	// false. Used by Next().
	Min(ctx CompareContext) (Datum, bool)

	// Size returns a lower bound on the total size of the receiver in bytes,
	// including memory that is pointed at (even if shared between Datum
	// instances) but excluding allocation overhead.
	//
	// It holds for every Datum d that d.Size().
	Size() uintptr
}

// Datums is a slice of Datum values.
type Datums []Datum

// Len returns the number of Datum values.
func (d Datums) Len() int { return len(d) }

// Format implements the NodeFormatter interface.
func (d *Datums) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	for i, v := range *d {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(v)
	}
	ctx.WriteByte(')')
}

// Compare does a lexicographical comparison and returns -1 if the receiver
// is less than other, 0 if receiver is equal to other and +1 if receiver is
// greater than other.
func (d Datums) Compare(evalCtx CompareContext, other Datums) int {
	if len(d) == 0 {
		panic(errors.AssertionFailedf("empty Datums being compared to other"))
	}

	for i := range d {
		if i >= len(other) {
			return 1
		}

		compareDatum := d[i].Compare(evalCtx, other[i])
		if compareDatum != 0 {
			return compareDatum
		}
	}

	if len(d) < len(other) {
		return -1
	}
	return 0
}

// IsDistinctFrom checks to see if two datums are distinct from each other. Any
// change in value is considered distinct, however, a NULL value is NOT
// considered distinct from another NULL value.
func (d Datums) IsDistinctFrom(evalCtx CompareContext, other Datums) bool {
	if len(d) != len(other) {
		return true
	}
	for i, val := range d {
		if val == DNull {
			if other[i] != DNull {
				return true
			}
		} else {
			if val.Compare(evalCtx, other[i]) != 0 {
				return true
			}
		}
	}
	return false
}

// CompositeDatum is a Datum that may require composite encoding in
// indexes. Any Datum implementing this interface must also add itself to
// colinfo.HasCompositeKeyEncoding.
type CompositeDatum interface {
	Datum
	// IsComposite returns true if this datum is not round-tripable in a key
	// encoding.
	IsComposite() bool
}

// DBool is the boolean Datum.
type DBool bool

// MakeDBool converts its argument to a *DBool, returning either DBoolTrue or
// DBoolFalse.
func MakeDBool(d DBool) *DBool {
	if d {
		return DBoolTrue
	}
	return DBoolFalse
}

// MustBeDBool attempts to retrieve a DBool from an Expr, panicking if the
// assertion fails.
func MustBeDBool(e Expr) DBool {
	b, ok := AsDBool(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DBool, found %T", e))
	}
	return b
}

// AsDBool attempts to retrieve a *DBool from an Expr, returning a *DBool and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions.
func AsDBool(e Expr) (DBool, bool) {
	switch t := e.(type) {
	case *DBool:
		return *t, true
	}
	return false, false
}

// MakeParseError returns a parse error using the provided string and type. An
// optional error can be provided, which will be appended to the end of the
// error string.
func MakeParseError(s string, typ *types.T, err error) error {
	if err != nil {
		return pgerror.Wrapf(err, pgcode.InvalidTextRepresentation,
			"could not parse %q as type %s", s, typ)
	}
	return pgerror.Newf(pgcode.InvalidTextRepresentation,
		"could not parse %q as type %s", s, typ)
}

func makeUnsupportedComparisonMessage(d1, d2 Datum) error {
	return pgerror.Newf(pgcode.DatatypeMismatch,
		"unsupported comparison: %s to %s",
		errors.Safe(d1.ResolvedType()),
		errors.Safe(d2.ResolvedType()),
	)
}

func isCaseInsensitivePrefix(prefix, s string) bool {
	if len(prefix) > len(s) {
		return false
	}
	return strings.EqualFold(prefix, s[:len(prefix)])
}

// ParseBool parses and returns the boolean value represented by the provided
// string, or an error if parsing is unsuccessful.
// See https://github.com/postgres/postgres/blob/90627cf98a8e7d0531789391fd798c9bfcc3bc1a/src/backend/utils/adt/bool.c#L36
func ParseBool(s string) (bool, error) {
	s = strings.TrimSpace(s)
	if len(s) >= 1 {
		switch s[0] {
		case 't', 'T':
			if isCaseInsensitivePrefix(s, "true") {
				return true, nil
			}
		case 'f', 'F':
			if isCaseInsensitivePrefix(s, "false") {
				return false, nil
			}
		case 'y', 'Y':
			if isCaseInsensitivePrefix(s, "yes") {
				return true, nil
			}
		case 'n', 'N':
			if isCaseInsensitivePrefix(s, "no") {
				return false, nil
			}
		case '1':
			if s == "1" {
				return true, nil
			}
		case '0':
			if s == "0" {
				return false, nil
			}
		case 'o', 'O':
			// Just 'o' is ambiguous between 'on' and 'off'.
			if len(s) > 1 {
				if isCaseInsensitivePrefix(s, "on") {
					return true, nil
				}
				if isCaseInsensitivePrefix(s, "off") {
					return false, nil
				}
			}
		}
	}
	return false, MakeParseError(s, types.Bool, pgerror.New(pgcode.InvalidTextRepresentation, "invalid bool value"))
}

// ParseDBool parses and returns the *DBool Datum value represented by the provided
// string, or an error if parsing is unsuccessful.
// See https://github.com/postgres/postgres/blob/90627cf98a8e7d0531789391fd798c9bfcc3bc1a/src/backend/utils/adt/bool.c#L36
func ParseDBool(s string) (*DBool, error) {
	v, err := ParseBool(s)
	if err != nil {
		return nil, err
	}
	if v {
		return DBoolTrue, nil
	}
	return DBoolFalse, nil
}

// ParseDByte parses a string representation of hex encoded binary
// data. It supports both the hex format, with "\x" followed by a
// string of hexadecimal digits (the "\x" prefix occurs just once at
// the beginning), and the escaped format, which supports "\\" and
// octal escapes.
func ParseDByte(s string) (*DBytes, error) {
	res, err := lex.DecodeRawBytesToByteArrayAuto([]byte(s))
	if err != nil {
		return nil, MakeParseError(s, types.Bytes, err)
	}
	return NewDBytes(DBytes(res)), nil
}

// ParseDUuidFromString parses and returns the *DUuid Datum value represented
// by the provided input string, or an error.
func ParseDUuidFromString(s string) (*DUuid, error) {
	uv, err := uuid.FromString(s)
	if err != nil {
		return nil, MakeParseError(s, types.Uuid, err)
	}
	return NewDUuid(DUuid{uv}), nil
}

// ParseDUuidFromBytes parses and returns the *DUuid Datum value represented
// by the provided input bytes, or an error.
func ParseDUuidFromBytes(b []byte) (*DUuid, error) {
	uv, err := uuid.FromBytes(b)
	if err != nil {
		return nil, MakeParseError(string(b), types.Uuid, err)
	}
	return NewDUuid(DUuid{uv}), nil
}

// ParseDIPAddrFromINetString parses and returns the *DIPAddr Datum value
// represented by the provided input INet string, or an error.
func ParseDIPAddrFromINetString(s string) (*DIPAddr, error) {
	var d DIPAddr
	err := ipaddr.ParseINet(s, &d.IPAddr)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// GetBool gets DBool or an error (also treats NULL as false, not an error).
func GetBool(d Datum) (DBool, error) {
	if v, ok := d.(*DBool); ok {
		return *v, nil
	}
	if d == DNull {
		return DBool(false), nil
	}
	return false, errors.AssertionFailedf("cannot convert %s to type %s", d.ResolvedType(), types.Bool)
}

// ResolvedType implements the TypedExpr interface.
func (*DBool) ResolvedType() *types.T {
	return types.Bool
}

// Compare implements the Datum interface.
func (d *DBool) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DBool) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DBool)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	res := CompareBools(bool(*d), bool(*v))
	return res, nil
}

// CompareBools compares the input bools according to the SQL comparison rules.
func CompareBools(d, v bool) int {
	if !d && v {
		return -1
	}
	if d && !v {
		return 1
	}
	return 0
}

// Prev implements the Datum interface.
func (*DBool) Prev(ctx CompareContext) (Datum, bool) {
	return DBoolFalse, true
}

// Next implements the Datum interface.
func (*DBool) Next(ctx CompareContext) (Datum, bool) {
	return DBoolTrue, true
}

// IsMax implements the Datum interface.
func (d *DBool) IsMax(ctx CompareContext) bool {
	return bool(*d)
}

// IsMin implements the Datum interface.
func (d *DBool) IsMin(ctx CompareContext) bool {
	return !bool(*d)
}

// Min implements the Datum interface.
func (d *DBool) Min(ctx CompareContext) (Datum, bool) {
	return DBoolFalse, true
}

// Max implements the Datum interface.
func (d *DBool) Max(ctx CompareContext) (Datum, bool) {
	return DBoolTrue, true
}

// AmbiguousFormat implements the Datum interface.
func (*DBool) AmbiguousFormat() bool { return false }

// PgwireFormatBool returns a single byte representing a boolean according to
// pgwire encoding.
func PgwireFormatBool(d bool) byte {
	if d {
		return 't'
	}
	return 'f'
}

// Format implements the NodeFormatter interface.
func (d *DBool) Format(ctx *FmtCtx) {
	if ctx.HasFlags(fmtPgwireFormat) {
		ctx.WriteByte(PgwireFormatBool(bool(*d)))
		return
	}
	ctx.WriteString(strconv.FormatBool(bool(*d)))
}

// Size implements the Datum interface.
func (d *DBool) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DBitArray is the BIT/VARBIT Datum.
type DBitArray struct {
	bitarray.BitArray
}

// ParseDBitArray parses a string representation of binary digits.
func ParseDBitArray(s string) (*DBitArray, error) {
	var a DBitArray
	var err error
	a.BitArray, err = bitarray.Parse(s)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// NewDBitArray returns a DBitArray.
func NewDBitArray(bitLen uint) *DBitArray {
	a := MakeDBitArray(bitLen)
	return &a
}

// MakeDBitArray returns a DBitArray.
func MakeDBitArray(bitLen uint) DBitArray {
	return DBitArray{BitArray: bitarray.MakeZeroBitArray(bitLen)}
}

// MustBeDBitArray attempts to retrieve a DBitArray from an Expr, panicking if the
// assertion fails.
func MustBeDBitArray(e Expr) *DBitArray {
	b, ok := AsDBitArray(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DBitArray, found %T", e))
	}
	return b
}

// AsDBitArray attempts to retrieve a *DBitArray from an Expr, returning a *DBitArray and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions.
func AsDBitArray(e Expr) (*DBitArray, bool) {
	switch t := e.(type) {
	case *DBitArray:
		return t, true
	}
	return nil, false
}

var errCannotCastNegativeIntToBitArray = pgerror.Newf(pgcode.CannotCoerce,
	"cannot cast negative integer to bit varying with unbounded width")

// NewDBitArrayFromInt creates a bit array from the specified integer
// at the specified width.
// If the width is zero, only positive integers can be converted.
// If the width is nonzero, the value is truncated to that width.
// Negative values are encoded using two's complement.
func NewDBitArrayFromInt(i int64, width uint) (*DBitArray, error) {
	if width == 0 && i < 0 {
		return nil, errCannotCastNegativeIntToBitArray
	}
	return &DBitArray{
		BitArray: bitarray.MakeBitArrayFromInt64(width, i, 64),
	}, nil
}

// AsDInt computes the integer value of the given bit array.
// The value is assumed to be encoded using two's complement.
// The result is truncated to the given integer number of bits,
// if specified.
// The given width must be 64 or smaller. The results are undefined
// if n is greater than 64.
func (d *DBitArray) AsDInt(n uint) *DInt {
	if n == 0 {
		n = 64
	}
	return NewDInt(DInt(d.BitArray.AsInt64(n)))
}

// ResolvedType implements the TypedExpr interface.
func (*DBitArray) ResolvedType() *types.T {
	return types.VarBit
}

// Compare implements the Datum interface.
func (d *DBitArray) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DBitArray) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DBitArray)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	res := bitarray.Compare(d.BitArray, v.BitArray)
	return res, nil
}

// Prev implements the Datum interface.
func (d *DBitArray) Prev(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DBitArray) Next(ctx CompareContext) (Datum, bool) {
	a := bitarray.Next(d.BitArray)
	return &DBitArray{BitArray: a}, true
}

// IsMax implements the Datum interface.
func (d *DBitArray) IsMax(ctx CompareContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DBitArray) IsMin(ctx CompareContext) bool {
	return d.BitArray.IsEmpty()
}

var bitArrayZero = NewDBitArray(0)

// Min implements the Datum interface.
func (d *DBitArray) Min(ctx CompareContext) (Datum, bool) {
	return bitArrayZero, true
}

// Max implements the Datum interface.
func (d *DBitArray) Max(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// AmbiguousFormat implements the Datum interface.
func (*DBitArray) AmbiguousFormat() bool { return false }

// Format implements the NodeFormatter interface.
func (d *DBitArray) Format(ctx *FmtCtx) {
	f := ctx.flags
	if f.HasFlags(fmtPgwireFormat) {
		d.BitArray.Format(&ctx.Buffer)
	} else {
		withQuotes := !f.HasFlags(FmtFlags(lexbase.EncBareStrings))
		if withQuotes {
			ctx.WriteString("B'")
		}
		d.BitArray.Format(&ctx.Buffer)
		if withQuotes {
			ctx.WriteByte('\'')
		}
	}
}

// Size implements the Datum interface.
func (d *DBitArray) Size() uintptr {
	return d.BitArray.Sizeof()
}

// DInt is the int Datum.
type DInt int64

// NewDInt is a helper routine to create a *DInt initialized from its argument.
func NewDInt(d DInt) *DInt {
	return &d
}

// ParseDInt parses and returns the *DInt Datum value represented by the provided
// string, or an error if parsing is unsuccessful.
func ParseDInt(s string) (*DInt, error) {
	i, err := strconv.ParseInt(s, 0, 64)
	if err != nil {
		return nil, MakeParseError(s, types.Int, err)
	}
	return NewDInt(DInt(i)), nil
}

// AsDInt attempts to retrieve a DInt from an Expr, returning a DInt and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DInt wrapped by a
// *DOidWrapper is possible.
func AsDInt(e Expr) (DInt, bool) {
	switch t := e.(type) {
	case *DInt:
		return *t, true
	case *DOidWrapper:
		return AsDInt(t.Wrapped)
	}
	return 0, false
}

// MustBeDInt attempts to retrieve a DInt from an Expr, panicking if the
// assertion fails.
func MustBeDInt(e Expr) DInt {
	i, ok := AsDInt(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DInt, found %T", e))
	}
	return i
}

// ResolvedType implements the TypedExpr interface.
func (*DInt) ResolvedType() *types.T {
	return types.Int
}

// Compare implements the Datum interface.
func (d *DInt) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DInt) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	thisInt := *d
	var v DInt
	switch t := ctx.UnwrapDatum(other).(type) {
	case *DInt:
		v = *t
	case *DFloat, *DDecimal:
		res, err := t.CompareError(ctx, d)
		if err != nil {
			return 0, err
		}
		return -res, nil
	case *DOid:
		// OIDs are always unsigned 32-bit integers. Some languages, like Java,
		// compare OIDs to signed 32-bit integers, so we implement the comparison
		// by converting to a uint32 first. This matches Postgres behavior.
		o, err := IntToOid(thisInt)
		if err != nil {
			return 0, err
		}
		thisInt = DInt(o.Oid)
		v = DInt(t.Oid)
	default:
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	if thisInt < v {
		return -1, nil
	}
	if thisInt > v {
		return 1, nil
	}
	return 0, nil
}

// Prev implements the Datum interface.
func (d *DInt) Prev(ctx CompareContext) (Datum, bool) {
	return NewDInt(*d - 1), true
}

// Next implements the Datum interface.
func (d *DInt) Next(ctx CompareContext) (Datum, bool) {
	return NewDInt(*d + 1), true
}

// IsMax implements the Datum interface.
func (d *DInt) IsMax(ctx CompareContext) bool {
	return *d == math.MaxInt64
}

// IsMin implements the Datum interface.
func (d *DInt) IsMin(ctx CompareContext) bool {
	return *d == math.MinInt64
}

var dMaxInt = NewDInt(math.MaxInt64)
var dMinInt = NewDInt(math.MinInt64)

// Max implements the Datum interface.
func (d *DInt) Max(ctx CompareContext) (Datum, bool) {
	return dMaxInt, true
}

// Min implements the Datum interface.
func (d *DInt) Min(ctx CompareContext) (Datum, bool) {
	return dMinInt, true
}

// AmbiguousFormat implements the Datum interface.
func (*DInt) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DInt) Format(ctx *FmtCtx) {
	// If the number is negative, we need to use parens or the `:::INT` type hint
	// will take precedence over the negation sign.
	disambiguate := ctx.flags.HasFlags(fmtDisambiguateDatumTypes)
	parsable := ctx.flags.HasFlags(FmtParsableNumerics)
	needParens := (disambiguate || parsable) && *d < 0
	if needParens {
		ctx.WriteByte('(')
	}
	ctx.WriteString(strconv.FormatInt(int64(*d), 10))
	if needParens {
		ctx.WriteByte(')')
	}
}

// Size implements the Datum interface.
func (d *DInt) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DFloat is the float Datum.
type DFloat float64

// MustBeDFloat attempts to retrieve a DFloat from an Expr, panicking if the
// assertion fails.
func MustBeDFloat(e Expr) DFloat {
	switch t := e.(type) {
	case *DFloat:
		return *t
	}
	panic(errors.AssertionFailedf("expected *DFloat, found %T", e))
}

// NewDFloat is a helper routine to create a *DFloat initialized from its
// argument.
func NewDFloat(d DFloat) *DFloat {
	return &d
}

// ParseDFloat parses and returns the *DFloat Datum value represented by the provided
// string, or an error if parsing is unsuccessful.
func ParseDFloat(s string) (*DFloat, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, MakeParseError(s, types.Float, err)
	}
	return NewDFloat(DFloat(f)), nil
}

// ResolvedType implements the TypedExpr interface.
func (*DFloat) ResolvedType() *types.T {
	return types.Float
}

// Compare implements the Datum interface.
func (d *DFloat) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DFloat) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	var v DFloat
	switch t := ctx.UnwrapDatum(other).(type) {
	case *DFloat:
		v = *t
	case *DInt:
		v = DFloat(MustBeDInt(t))
	case *DDecimal:
		res, err := t.CompareError(ctx, d)
		if err != nil {
			return 0, err
		}
		return -res, nil
	default:
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	if *d < v {
		return -1, nil
	}
	if *d > v {
		return 1, nil
	}
	// NaN sorts before non-NaN (#10109).
	if *d == v {
		return 0, nil
	}
	if math.IsNaN(float64(*d)) {
		if math.IsNaN(float64(v)) {
			return 0, nil
		}
		return -1, nil
	}
	return 1, nil
}

// Prev implements the Datum interface.
func (d *DFloat) Prev(ctx CompareContext) (Datum, bool) {
	f := float64(*d)
	if math.IsNaN(f) {
		return nil, false
	}
	if f == math.Inf(-1) {
		return DNaNFloat, true
	}
	return NewDFloat(DFloat(math.Nextafter(f, math.Inf(-1)))), true
}

// Next implements the Datum interface.
func (d *DFloat) Next(ctx CompareContext) (Datum, bool) {
	f := float64(*d)
	if math.IsNaN(f) {
		return DNegInfFloat, true
	}
	if f == math.Inf(+1) {
		return nil, false
	}
	return NewDFloat(DFloat(math.Nextafter(f, math.Inf(+1)))), true
}

var (
	// DZeroFloat is the DFloat for zero.
	DZeroFloat = NewDFloat(0)
	// DPosInfFloat is the DFloat for positive infinity.
	DPosInfFloat = NewDFloat(DFloat(math.Inf(+1)))
	// DNegInfFloat is the DFloat for negative infinity.
	DNegInfFloat = NewDFloat(DFloat(math.Inf(-1)))
	// DNaNFloat is the DFloat for NaN.
	DNaNFloat = NewDFloat(DFloat(math.NaN()))
)

// IsMax implements the Datum interface.
func (d *DFloat) IsMax(ctx CompareContext) bool {
	return *d == *DPosInfFloat
}

// IsMin implements the Datum interface.
func (d *DFloat) IsMin(ctx CompareContext) bool {
	return math.IsNaN(float64(*d))
}

// Max implements the Datum interface.
func (d *DFloat) Max(ctx CompareContext) (Datum, bool) {
	return DPosInfFloat, true
}

// Min implements the Datum interface.
func (d *DFloat) Min(ctx CompareContext) (Datum, bool) {
	return DNaNFloat, true
}

// AmbiguousFormat implements the Datum interface.
func (*DFloat) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DFloat) Format(ctx *FmtCtx) {
	fl := float64(*d)

	disambiguate := ctx.flags.HasFlags(fmtDisambiguateDatumTypes)
	parsable := ctx.flags.HasFlags(FmtParsableNumerics)
	quote := parsable && (math.IsNaN(fl) || math.IsInf(fl, 0))
	// We need to use Signbit here and not just fl < 0 because of -0.
	needParens := !quote && (disambiguate || parsable) && math.Signbit(fl)
	// If the number is negative, we need to use parens or the `:::INT` type hint
	// will take precedence over the negation sign.
	if quote {
		ctx.WriteByte('\'')
	} else if needParens {
		ctx.WriteByte('(')
	}
	if _, frac := math.Modf(fl); frac == 0 && -1000000 < *d && *d < 1000000 {
		// d is a small whole number. Ensure it is printed using a decimal point.
		ctx.Printf("%.1f", fl)
	} else {
		ctx.Printf("%g", fl)
	}
	if quote {
		ctx.WriteByte('\'')
	} else if needParens {
		ctx.WriteByte(')')
	}
}

// Size implements the Datum interface.
func (d *DFloat) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// IsComposite implements the CompositeDatum interface.
func (d *DFloat) IsComposite() bool {
	// -0 is composite.
	return math.Float64bits(float64(*d)) == 1<<63
}

// DDecimal is the decimal Datum.
type DDecimal struct {
	apd.Decimal
}

// MustBeDDecimal attempts to retrieve a DDecimal from an Expr, panicking if the
// assertion fails.
func MustBeDDecimal(e Expr) DDecimal {
	switch t := e.(type) {
	case *DDecimal:
		return *t
	}
	panic(errors.AssertionFailedf("expected *DDecimal, found %T", e))
}

// ParseDDecimal parses and returns the *DDecimal Datum value represented by the
// provided string, or an error if parsing is unsuccessful.
func ParseDDecimal(s string) (*DDecimal, error) {
	dd := &DDecimal{}
	err := dd.SetString(s)
	return dd, err
}

// SetString sets d to s. Any non-standard NaN values are converted to a
// normal NaN. Any negative zero is converted to positive.
func (d *DDecimal) SetString(s string) error {
	// ExactCtx should be able to handle any decimal, but if there is any rounding
	// or other inexact conversion, it will result in an error.
	//_, res, err := HighPrecisionCtx.SetString(&d.Decimal, s)
	_, res, err := ExactCtx.SetString(&d.Decimal, s)
	if res != 0 || err != nil {
		return MakeParseError(s, types.Decimal, err)
	}
	switch d.Form {
	case apd.NaNSignaling:
		d.Form = apd.NaN
		d.Negative = false
	case apd.NaN:
		d.Negative = false
	case apd.Finite:
		if d.IsZero() && d.Negative {
			d.Negative = false
		}
	}
	return nil
}

// ResolvedType implements the TypedExpr interface.
func (*DDecimal) ResolvedType() *types.T {
	return types.Decimal
}

// Compare implements the Datum interface.
func (d *DDecimal) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DDecimal) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	var v apd.Decimal
	switch t := ctx.UnwrapDatum(other).(type) {
	case *DDecimal:
		v.Set(&t.Decimal)
	case *DInt:
		v.SetInt64(int64(*t))
	case *DFloat:
		if _, err := v.SetFloat64(float64(*t)); err != nil {
			panic(errors.NewAssertionErrorWithWrappedErrf(err, "decimal compare, unexpected error"))
		}
	default:
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	res := CompareDecimals(&d.Decimal, &v)
	return res, nil
}

// CompareDecimals compares 2 apd.Decimals according to the SQL comparison
// rules, making sure that NaNs sort first.
func CompareDecimals(d *apd.Decimal, v *apd.Decimal) int {
	// NaNs sort first in SQL.
	if dn, vn := d.Form == apd.NaN, v.Form == apd.NaN; dn && !vn {
		return -1
	} else if !dn && vn {
		return 1
	} else if dn && vn {
		return 0
	}
	return d.Cmp(v)
}

// Prev implements the Datum interface.
func (d *DDecimal) Prev(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DDecimal) Next(ctx CompareContext) (Datum, bool) {
	return nil, false
}

var dZeroDecimal = &DDecimal{Decimal: apd.Decimal{}}
var dPosInfDecimal = &DDecimal{Decimal: apd.Decimal{Form: apd.Infinite, Negative: false}}
var dNaNDecimal = &DDecimal{Decimal: apd.Decimal{Form: apd.NaN}}

// IsMax implements the Datum interface.
func (d *DDecimal) IsMax(ctx CompareContext) bool {
	return d.Form == apd.Infinite && !d.Negative
}

// IsMin implements the Datum interface.
func (d *DDecimal) IsMin(ctx CompareContext) bool {
	return d.Form == apd.NaN
}

// Max implements the Datum interface.
func (d *DDecimal) Max(ctx CompareContext) (Datum, bool) {
	return dPosInfDecimal, true
}

// Min implements the Datum interface.
func (d *DDecimal) Min(ctx CompareContext) (Datum, bool) {
	return dNaNDecimal, true
}

// AmbiguousFormat implements the Datum interface.
func (*DDecimal) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DDecimal) Format(ctx *FmtCtx) {
	// If the number is negative, we need to use parens or the `:::INT` type hint
	// will take precedence over the negation sign.
	disambiguate := ctx.flags.HasFlags(fmtDisambiguateDatumTypes)
	parsable := ctx.flags.HasFlags(FmtParsableNumerics)
	quote := parsable && d.Decimal.Form != apd.Finite
	needParens := !quote && (disambiguate || parsable) && d.Negative
	if needParens {
		ctx.WriteByte('(')
	}
	if quote {
		ctx.WriteByte('\'')
	}
	ctx.WriteString(d.Decimal.String())
	if quote {
		ctx.WriteByte('\'')
	}
	if needParens {
		ctx.WriteByte(')')
	}
}

// Size implements the Datum interface.
func (d *DDecimal) Size() uintptr {
	return d.Decimal.Size()
}

var (
	decimalNegativeZero = &apd.Decimal{Negative: true}
	bigTen              = apd.NewBigInt(10)
)

// IsComposite implements the CompositeDatum interface.
func (d *DDecimal) IsComposite() bool {
	// -0 is composite.
	if d.Decimal.CmpTotal(decimalNegativeZero) == 0 {
		return true
	}

	// Check if d is divisible by 10.
	var r apd.BigInt
	r.Rem(&d.Decimal.Coeff, bigTen)
	return r.Sign() == 0
}

// DString is the string Datum.
type DString string

// NewDString is a helper routine to create a *DString initialized from its
// argument.
func NewDString(d string) *DString {
	r := DString(d)
	return &r
}

// AsDString attempts to retrieve a DString from an Expr, returning a DString and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DString wrapped by a
// *DOidWrapper is possible.
func AsDString(e Expr) (DString, bool) {
	switch t := e.(type) {
	case *DString:
		return *t, true
	case *DOidWrapper:
		return AsDString(t.Wrapped)
	}
	return "", false
}

// MustBeDString attempts to retrieve a DString from an Expr, panicking if the
// assertion fails.
func MustBeDString(e Expr) DString {
	i, ok := AsDString(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DString, found %T", e))
	}
	return i
}

// MustBeDStringOrDNull attempts to retrieve a DString or DNull from an Expr, panicking if the
// assertion fails.
func MustBeDStringOrDNull(e Expr) DString {
	i, ok := AsDString(e)
	if !ok && e != DNull {
		panic(errors.AssertionFailedf("expected *DString or DNull, found %T", e))
	}
	return i
}

// ResolvedType implements the TypedExpr interface.
func (*DString) ResolvedType() *types.T {
	return types.String
}

// Compare implements the Datum interface.
func (d *DString) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DString) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DString)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	if *d < *v {
		return -1, nil
	}
	if *d > *v {
		return 1, nil
	}
	return 0, nil
}

// Prev implements the Datum interface.
func (d *DString) Prev(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DString) Next(ctx CompareContext) (Datum, bool) {
	return NewDString(string(encoding.BytesNext([]byte(*d)))), true
}

// IsMax implements the Datum interface.
func (*DString) IsMax(ctx CompareContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DString) IsMin(ctx CompareContext) bool {
	return len(*d) == 0
}

var dEmptyString = NewDString("")

// Min implements the Datum interface.
func (d *DString) Min(ctx CompareContext) (Datum, bool) {
	return dEmptyString, true
}

// Max implements the Datum interface.
func (d *DString) Max(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// AmbiguousFormat implements the Datum interface.
func (*DString) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DString) Format(ctx *FmtCtx) {
	buf, f := &ctx.Buffer, ctx.flags
	if f.HasFlags(fmtRawStrings) {
		buf.WriteString(string(*d))
	} else {
		lexbase.EncodeSQLStringWithFlags(buf, string(*d), f.EncodeFlags())
	}
}

// Size implements the Datum interface.
func (d *DString) Size() uintptr {
	return unsafe.Sizeof(*d) + uintptr(len(*d))
}

// DCollatedString is the Datum for strings with a locale. The struct members
// are intended to be immutable.
type DCollatedString struct {
	Contents string
	Locale   string
	// Key is the collation key.
	Key []byte
}

// CollationEnvironment stores the state needed by NewDCollatedString to
// construct collation keys efficiently.
type CollationEnvironment struct {
	cache  map[string]collationEnvironmentCacheEntry
	buffer *collate.Buffer
}

type collationEnvironmentCacheEntry struct {
	// locale is interned.
	locale string
	// collator is an expensive factory.
	collator *collate.Collator
}

func (env *CollationEnvironment) getCacheEntry(
	locale string,
) (collationEnvironmentCacheEntry, error) {
	entry, ok := env.cache[locale]
	if !ok {
		if env.cache == nil {
			env.cache = make(map[string]collationEnvironmentCacheEntry)
		}
		tag, err := language.Parse(locale)
		if err != nil {
			err = errors.NewAssertionErrorWithWrappedErrf(err, "failed to parse locale %q", locale)
			return collationEnvironmentCacheEntry{}, err
		}

		entry = collationEnvironmentCacheEntry{locale, collate.New(tag)}
		env.cache[locale] = entry
	}
	return entry, nil
}

// NewDCollatedString is a helper routine to create a *DCollatedString. Panics
// if locale is invalid. Not safe for concurrent use.
func NewDCollatedString(
	contents string, locale string, env *CollationEnvironment,
) (*DCollatedString, error) {
	entry, err := env.getCacheEntry(locale)
	if err != nil {
		return nil, err
	}
	if env.buffer == nil {
		env.buffer = &collate.Buffer{}
	}
	key := entry.collator.KeyFromString(env.buffer, contents)
	d := DCollatedString{contents, entry.locale, make([]byte, len(key))}
	copy(d.Key, key)
	env.buffer.Reset()
	return &d, nil
}

// AmbiguousFormat implements the Datum interface.
func (*DCollatedString) AmbiguousFormat() bool { return false }

// Format implements the NodeFormatter interface.
func (d *DCollatedString) Format(ctx *FmtCtx) {
	lexbase.EncodeSQLString(&ctx.Buffer, d.Contents)
	ctx.WriteString(" COLLATE ")
	lex.EncodeLocaleName(&ctx.Buffer, d.Locale)
}

// ResolvedType implements the TypedExpr interface.
func (d *DCollatedString) ResolvedType() *types.T {
	return types.MakeCollatedString(types.String, d.Locale)
}

// Compare implements the Datum interface.
func (d *DCollatedString) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DCollatedString) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DCollatedString)
	if !ok || !d.ResolvedType().Equivalent(other.ResolvedType()) {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	res := bytes.Compare(d.Key, v.Key)
	return res, nil
}

// Prev implements the Datum interface.
func (d *DCollatedString) Prev(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DCollatedString) Next(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (*DCollatedString) IsMax(ctx CompareContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DCollatedString) IsMin(ctx CompareContext) bool {
	return d.Contents == ""
}

// Min implements the Datum interface.
func (d *DCollatedString) Min(ctx CompareContext) (Datum, bool) {
	return &DCollatedString{"", d.Locale, nil}, true
}

// Max implements the Datum interface.
func (d *DCollatedString) Max(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Size implements the Datum interface.
func (d *DCollatedString) Size() uintptr {
	return unsafe.Sizeof(*d) + uintptr(len(d.Contents)) + uintptr(len(d.Locale)) + uintptr(len(d.Key))
}

// IsComposite implements the CompositeDatum interface.
func (d *DCollatedString) IsComposite() bool {
	return true
}

// DBytes is the bytes Datum. The underlying type is a string because we want
// the immutability, but this may contain arbitrary bytes.
type DBytes string

// NewDBytes is a helper routine to create a *DBytes initialized from its
// argument.
func NewDBytes(d DBytes) *DBytes {
	return &d
}

// MustBeDBytes attempts to convert an Expr into a DBytes, panicking if unsuccessful.
func MustBeDBytes(e Expr) DBytes {
	i, ok := AsDBytes(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DBytes, found %T", e))
	}
	return i
}

// AsDBytes attempts to convert an Expr into a DBytes, returning a flag indicating
// whether it was successful.
func AsDBytes(e Expr) (DBytes, bool) {
	switch t := e.(type) {
	case *DBytes:
		return *t, true
	}
	return "", false
}

// ResolvedType implements the TypedExpr interface.
func (*DBytes) ResolvedType() *types.T {
	return types.Bytes
}

// Compare implements the Datum interface.
func (d *DBytes) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DBytes) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DBytes)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	if *d < *v {
		return -1, nil
	}
	if *d > *v {
		return 1, nil
	}
	return 0, nil
}

// Prev implements the Datum interface.
func (d *DBytes) Prev(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DBytes) Next(ctx CompareContext) (Datum, bool) {
	return NewDBytes(DBytes(encoding.BytesNext([]byte(*d)))), true
}

// IsMax implements the Datum interface.
func (*DBytes) IsMax(ctx CompareContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DBytes) IsMin(ctx CompareContext) bool {
	return len(*d) == 0
}

var dEmptyBytes = NewDBytes(DBytes(""))

// Min implements the Datum interface.
func (d *DBytes) Min(ctx CompareContext) (Datum, bool) {
	return dEmptyBytes, true
}

// Max implements the Datum interface.
func (d *DBytes) Max(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// AmbiguousFormat implements the Datum interface.
func (*DBytes) AmbiguousFormat() bool { return true }

func writeAsHexString(ctx *FmtCtx, b string) {
	for i := 0; i < len(b); i++ {
		ctx.Write(stringencoding.RawHexMap[b[i]])
	}
}

// Format implements the NodeFormatter interface.
func (d *DBytes) Format(ctx *FmtCtx) {
	f := ctx.flags
	if f.HasFlags(fmtPgwireFormat) {
		ctx.WriteString(`"\\x`)
		writeAsHexString(ctx, string(*d))
		ctx.WriteString(`"`)
	} else if f.HasFlags(fmtFormatByteLiterals) {
		ctx.WriteByte('x')
		ctx.WriteByte('\'')
		_, _ = hex.NewEncoder(ctx).Write([]byte(*d))
		ctx.WriteByte('\'')
	} else {
		withQuotes := !f.HasFlags(FmtFlags(lexbase.EncBareStrings))
		if withQuotes {
			ctx.WriteByte('\'')
		}
		ctx.WriteString("\\x")
		writeAsHexString(ctx, string(*d))
		if withQuotes {
			ctx.WriteByte('\'')
		}
	}
}

// Size implements the Datum interface.
func (d *DBytes) Size() uintptr {
	return unsafe.Sizeof(*d) + uintptr(len(*d))
}

// DEncodedKey is a special Datum of types.EncodedKey type, used to pass through
// encoded key data. It is similar to DBytes, except when it comes to
// encoding/decoding. It is currently used to pass around inverted index keys,
// which do not fully encode an object.
type DEncodedKey string

// NewDEncodedKey is a helper routine to create a *DEncodedKey initialized from its
// argument.
func NewDEncodedKey(d DEncodedKey) *DEncodedKey {
	return &d
}

// ResolvedType implements the TypedExpr interface.
func (*DEncodedKey) ResolvedType() *types.T {
	return types.EncodedKey
}

// Compare implements the Datum interface.
func (d *DEncodedKey) Compare(ctx CompareContext, other Datum) int {
	panic(errors.AssertionFailedf("not implemented"))
}

// CompareError implements the Datum interface.
func (d *DEncodedKey) CompareError(ctx CompareContext, other Datum) (int, error) {
	panic(errors.AssertionFailedf("not implemented"))
}

// Prev implements the Datum interface.
func (d *DEncodedKey) Prev(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DEncodedKey) Next(ctx CompareContext) (Datum, bool) {
	return nil, true
}

// IsMax implements the Datum interface.
func (*DEncodedKey) IsMax(ctx CompareContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DEncodedKey) IsMin(ctx CompareContext) bool {
	return false
}

// Min implements the Datum interface.
func (d *DEncodedKey) Min(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Max implements the Datum interface.
func (d *DEncodedKey) Max(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// AmbiguousFormat implements the Datum interface.
func (*DEncodedKey) AmbiguousFormat() bool {
	panic(errors.AssertionFailedf("not implemented"))
}

// Format implements the NodeFormatter interface.
func (d *DEncodedKey) Format(ctx *FmtCtx) {
	(*DBytes)(d).Format(ctx)
}

// Size implements the Datum interface.
func (d *DEncodedKey) Size() uintptr {
	return unsafe.Sizeof(*d) + uintptr(len(*d))
}

// DUuid is the UUID Datum.
type DUuid struct {
	uuid.UUID
}

// NewDUuid is a helper routine to create a *DUuid initialized from its
// argument.
func NewDUuid(d DUuid) *DUuid {
	return &d
}

// AsDUuid attempts to retrieve a DUuid from an Expr, returning a DUuid and
// a flag signifying whether the assertion was successful.
func AsDUuid(e Expr) (DUuid, bool) {
	switch t := e.(type) {
	case *DUuid:
		return *t, true
	}
	return DUuid{}, false
}

// MustBeDUuid attempts to retrieve a DUuid from an Expr, panicking if the
// assertion fails.
func MustBeDUuid(e Expr) DUuid {
	i, ok := AsDUuid(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DUuid, found %T", e))
	}
	return i
}

// ResolvedType implements the TypedExpr interface.
func (*DUuid) ResolvedType() *types.T {
	return types.Uuid
}

// Compare implements the Datum interface.
func (d *DUuid) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DUuid) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DUuid)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	res := bytes.Compare(d.GetBytes(), v.GetBytes())
	return res, nil
}

func (d *DUuid) equal(other *DUuid) bool {
	return bytes.Equal(d.GetBytes(), other.GetBytes())
}

// Prev implements the Datum interface.
func (d *DUuid) Prev(ctx CompareContext) (Datum, bool) {
	i := d.ToUint128()
	u := uuid.FromUint128(i.Sub(1))
	return NewDUuid(DUuid{u}), true
}

// Next implements the Datum interface.
func (d *DUuid) Next(ctx CompareContext) (Datum, bool) {
	i := d.ToUint128()
	u := uuid.FromUint128(i.Add(1))
	return NewDUuid(DUuid{u}), true
}

// IsMax implements the Datum interface.
func (d *DUuid) IsMax(ctx CompareContext) bool {
	return d.equal(DMaxUUID)
}

// IsMin implements the Datum interface.
func (d *DUuid) IsMin(ctx CompareContext) bool {
	return d.equal(DMinUUID)
}

// DMinUUID is the min UUID.
var DMinUUID = NewDUuid(DUuid{uuid.UUID{}})

// DMaxUUID is the max UUID.
var DMaxUUID = NewDUuid(DUuid{uuid.UUID{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}})

// Min implements the Datum interface.
func (*DUuid) Min(ctx CompareContext) (Datum, bool) {
	return DMinUUID, true
}

// Max implements the Datum interface.
func (*DUuid) Max(ctx CompareContext) (Datum, bool) {
	return DMaxUUID, true
}

// AmbiguousFormat implements the Datum interface.
func (*DUuid) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DUuid) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lexbase.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}

	buf := ctx.scratch[:uuid.RFC4122StrSize]
	d.UUID.StringBytes(buf)
	ctx.Write(buf)

	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DUuid) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DIPAddr is the IPAddr Datum.
type DIPAddr struct {
	ipaddr.IPAddr
}

// NewDIPAddr is a helper routine to create a *DIPAddr initialized from its
// argument.
func NewDIPAddr(d DIPAddr) *DIPAddr {
	return &d
}

// AsDIPAddr attempts to retrieve a *DIPAddr from an Expr, returning a *DIPAddr and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DIPAddr wrapped by a
// *DOidWrapper is possible.
func AsDIPAddr(e Expr) (DIPAddr, bool) {
	switch t := e.(type) {
	case *DIPAddr:
		return *t, true
	case *DOidWrapper:
		return AsDIPAddr(t.Wrapped)
	}
	return DIPAddr{}, false
}

// MustBeDIPAddr attempts to retrieve a DIPAddr from an Expr, panicking if the
// assertion fails.
func MustBeDIPAddr(e Expr) DIPAddr {
	i, ok := AsDIPAddr(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DIPAddr, found %T", e))
	}
	return i
}

// ResolvedType implements the TypedExpr interface.
func (*DIPAddr) ResolvedType() *types.T {
	return types.INet
}

// Compare implements the Datum interface.
func (d *DIPAddr) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DIPAddr) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DIPAddr)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}

	res := d.IPAddr.Compare(&v.IPAddr)
	return res, nil
}

func (d DIPAddr) equal(other *DIPAddr) bool {
	return d.IPAddr.Equal(&other.IPAddr)
}

// Prev implements the Datum interface.
func (d *DIPAddr) Prev(ctx CompareContext) (Datum, bool) {
	// We will do one of the following to get the Prev IPAddr:
	//	- Decrement IP address if we won't underflow the IP.
	//	- Decrement mask and set the IP to max in family if we will underflow.
	//	- Jump down from IPv6 to IPv4 if we will underflow both IP and mask.
	if d.Family == ipaddr.IPv6family && d.Addr.Equal(dIPv6min) {
		if d.Mask == 0 {
			// Jump down IP family.
			return dMaxIPv4Addr, true
		}
		// Decrease mask size, wrap IPv6 IP address.
		return NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv6family, Addr: dIPv6max, Mask: d.Mask - 1}}), true
	} else if d.Family == ipaddr.IPv4family && d.Addr.Equal(dIPv4min) {
		// Decrease mask size, wrap IPv4 IP address.
		return NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv4family, Addr: dIPv4max, Mask: d.Mask - 1}}), true
	}
	// Decrement IP address.
	return NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: d.Family, Addr: d.Addr.Sub(1), Mask: d.Mask}}), true
}

// Next implements the Datum interface.
func (d *DIPAddr) Next(ctx CompareContext) (Datum, bool) {
	// We will do one of a few things to get the Next IP address:
	//	- Increment IP address if we won't overflow the IP.
	//	- Increment mask and set the IP to min in family if we will overflow.
	//	- Jump up from IPv4 to IPv6 if we will overflow both IP and mask.
	if d.Family == ipaddr.IPv4family && d.Addr.Equal(dIPv4max) {
		if d.Mask == 32 {
			// Jump up IP family.
			return dMinIPv6Addr, true
		}
		// Increase mask size, wrap IPv4 IP address.
		return NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv4family, Addr: dIPv4min, Mask: d.Mask + 1}}), true
	} else if d.Family == ipaddr.IPv6family && d.Addr.Equal(dIPv6max) {
		// Increase mask size, wrap IPv6 IP address.
		return NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv6family, Addr: dIPv6min, Mask: d.Mask + 1}}), true
	}
	// Increment IP address.
	return NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: d.Family, Addr: d.Addr.Add(1), Mask: d.Mask}}), true
}

// IsMax implements the Datum interface.
func (d *DIPAddr) IsMax(ctx CompareContext) bool {
	return d.equal(DMaxIPAddr)
}

// IsMin implements the Datum interface.
func (d *DIPAddr) IsMin(ctx CompareContext) bool {
	return d.equal(DMinIPAddr)
}

// dIPv4 and dIPv6 min and maxes use ParseIP because the actual byte constant is
// no equal to solely zeros or ones. For IPv4 there is a 0xffff prefix. Without
// this prefix this makes IP arithmetic invalid.
var dIPv4min = ipaddr.Addr(uint128.FromBytes([]byte(ipaddr.ParseIP("0.0.0.0"))))
var dIPv4max = ipaddr.Addr(uint128.FromBytes([]byte(ipaddr.ParseIP("255.255.255.255"))))
var dIPv6min = ipaddr.Addr(uint128.FromBytes([]byte(ipaddr.ParseIP("::"))))
var dIPv6max = ipaddr.Addr(uint128.FromBytes([]byte(ipaddr.ParseIP("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"))))

// dMaxIPv4Addr and dMinIPv6Addr are used as global constants to prevent extra
// heap extra allocation
var dMaxIPv4Addr = NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv4family, Addr: dIPv4max, Mask: 32}})
var dMinIPv6Addr = NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv6family, Addr: dIPv6min, Mask: 0}})

// DMinIPAddr is the min DIPAddr.
var DMinIPAddr = NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv4family, Addr: dIPv4min, Mask: 0}})

// DMaxIPAddr is the max DIPaddr.
var DMaxIPAddr = NewDIPAddr(DIPAddr{ipaddr.IPAddr{Family: ipaddr.IPv6family, Addr: dIPv6max, Mask: 128}})

// Min implements the Datum interface.
func (*DIPAddr) Min(ctx CompareContext) (Datum, bool) {
	return DMinIPAddr, true
}

// Max implements the Datum interface.
func (*DIPAddr) Max(ctx CompareContext) (Datum, bool) {
	return DMaxIPAddr, true
}

// AmbiguousFormat implements the Datum interface.
func (*DIPAddr) AmbiguousFormat() bool {
	return true
}

// Format implements the NodeFormatter interface.
func (d *DIPAddr) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lexbase.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	ctx.WriteString(d.IPAddr.String())
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DIPAddr) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DDate is the date Datum represented as the number of days after
// the Unix epoch.
type DDate struct {
	pgdate.Date
}

// NewDDate is a helper routine to create a *DDate initialized from its
// argument.
func NewDDate(d pgdate.Date) *DDate {
	return &DDate{Date: d}
}

// MakeDDate makes a DDate from a pgdate.Date.
func MakeDDate(d pgdate.Date) DDate {
	return DDate{Date: d}
}

// NewDDateFromTime constructs a *DDate from a time.Time.
func NewDDateFromTime(t time.Time) (*DDate, error) {
	d, err := pgdate.MakeDateFromTime(t)
	return NewDDate(d), err
}

// ParseContext provides the information necessary for
// parsing dates.
// A nil value is generally acceptable and will result in
// reasonable defaults being applied.
type ParseContext interface {
	// GetRelativeParseTime returns the transaction time in the session's
	// timezone (i.e. now()). This is used to calculate relative dates,
	// like "tomorrow", and also provides a default time.Location for
	// parsed times.
	GetRelativeParseTime() time.Time
	// GetCollationEnv returns the collation environment.
	GetCollationEnv() *CollationEnvironment
	// GetIntervalStyle returns the interval style in the session.
	GetIntervalStyle() duration.IntervalStyle
	// GetDateStyle returns the date style in the session.
	GetDateStyle() pgdate.DateStyle
	// GetParseHelper returns a helper to optmize date parsing.
	GetDateHelper() *pgdate.ParseHelper
}

var _ ParseContext = &simpleParseContext{}

// NewParseContextOption is an option to NewParseContext.
type NewParseContextOption func(ret *simpleParseContext)

// NewParseContextOptionDateStyle sets the DateStyle for the context.
func NewParseContextOptionDateStyle(dateStyle pgdate.DateStyle) NewParseContextOption {
	return func(ret *simpleParseContext) {
		ret.DateStyle = dateStyle
	}
}

// NewParseContext constructs a ParseContext that returns
// the given values.
func NewParseContext(relativeParseTime time.Time, opts ...NewParseContextOption) ParseContext {
	ret := &simpleParseContext{
		RelativeParseTime: relativeParseTime,
	}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

type simpleParseContext struct {
	RelativeParseTime    time.Time
	CollationEnvironment CollationEnvironment
	DateStyle            pgdate.DateStyle
	IntervalStyle        duration.IntervalStyle
	dateHelper           pgdate.ParseHelper
}

// GetRelativeParseTime implements ParseContext.
func (ctx *simpleParseContext) GetRelativeParseTime() time.Time {
	return ctx.RelativeParseTime
}

// GetCollationEnv implements ParseContext.
func (ctx *simpleParseContext) GetCollationEnv() *CollationEnvironment {
	return &ctx.CollationEnvironment
}

// GetIntervalStyle implements ParseContext.
func (ctx *simpleParseContext) GetIntervalStyle() duration.IntervalStyle {
	return ctx.IntervalStyle
}

// GetDateStyle implements ParseContext.
func (ctx *simpleParseContext) GetDateStyle() pgdate.DateStyle {
	return ctx.DateStyle
}

// GetDateHelper implements ParseTimeContext.
func (ctx *simpleParseContext) GetDateHelper() *pgdate.ParseHelper {
	return &ctx.dateHelper
}

// relativeParseTime chooses a reasonable "now" value for
// performing date parsing.
func relativeParseTime(ctx ParseContext) time.Time {
	if ctx == nil {
		return timeutil.Now()
	}
	return ctx.GetRelativeParseTime()
}

func dateStyle(ctx ParseContext) pgdate.DateStyle {
	if ctx == nil {
		return pgdate.DefaultDateStyle()
	}
	return ctx.GetDateStyle()
}

func intervalStyle(ctx ParseContext) duration.IntervalStyle {
	if ctx == nil {
		return duration.IntervalStyle_POSTGRES
	}
	return ctx.GetIntervalStyle()
}

func dateParseHelper(ctx ParseContext) *pgdate.ParseHelper {
	if ctx == nil {
		return nil
	}
	return ctx.GetDateHelper()
}

// ParseDDate parses and returns the *DDate Datum value represented by the provided
// string in the provided location, or an error if parsing is unsuccessful.
//
// The dependsOnContext return value indicates if we had to consult the
// ParseContext (either for the time or the local timezone).
func ParseDDate(ctx ParseContext, s string) (_ *DDate, dependsOnContext bool, _ error) {
	now := relativeParseTime(ctx)
	t, dependsOnContext, err := pgdate.ParseDate(now, dateStyle(ctx), s, dateParseHelper(ctx))
	return NewDDate(t), dependsOnContext, err
}

// AsDDate attempts to retrieve a DDate from an Expr, returning a DDate and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DDate wrapped by a
// *DOidWrapper is possible.
func AsDDate(e Expr) (DDate, bool) {
	switch t := e.(type) {
	case *DDate:
		return *t, true
	case *DOidWrapper:
		return AsDDate(t.Wrapped)
	}
	return DDate{}, false
}

// MustBeDDate attempts to retrieve a DDate from an Expr, panicking if the
// assertion fails.
func MustBeDDate(e Expr) DDate {
	t, ok := AsDDate(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DDate, found %T", e))
	}
	return t
}

// ResolvedType implements the TypedExpr interface.
func (*DDate) ResolvedType() *types.T {
	return types.Date
}

// Compare implements the Datum interface.
func (d *DDate) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DDate) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	var v DDate
	switch t := ctx.UnwrapDatum(other).(type) {
	case *DDate:
		v = *t
	case *DTimestamp, *DTimestampTZ:
		return compareTimestamps(ctx, d, other)
	default:
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	res := d.Date.Compare(v.Date)
	return res, nil
}

var (
	epochDate, _ = pgdate.MakeDateFromPGEpoch(0)
	dEpochDate   = NewDDate(epochDate)
	dMaxDate     = NewDDate(pgdate.PosInfDate)
	dMinDate     = NewDDate(pgdate.NegInfDate)
	dLowDate     = NewDDate(pgdate.LowDate)
	dHighDate    = NewDDate(pgdate.HighDate)
)

// Prev implements the Datum interface.
func (d *DDate) Prev(ctx CompareContext) (Datum, bool) {
	switch d.Date {
	case pgdate.PosInfDate:
		return dHighDate, true
	case pgdate.LowDate:
		return dMinDate, true
	case pgdate.NegInfDate:
		return nil, false
	}
	n, err := d.AddDays(-1)
	if err != nil {
		return nil, false
	}
	return NewDDate(n), true
}

// Next implements the Datum interface.
func (d *DDate) Next(ctx CompareContext) (Datum, bool) {
	switch d.Date {
	case pgdate.NegInfDate:
		return dLowDate, true
	case pgdate.HighDate:
		return dMaxDate, true
	case pgdate.PosInfDate:
		return nil, false
	}
	n, err := d.AddDays(1)
	if err != nil {
		return nil, false
	}
	return NewDDate(n), true
}

// IsMax implements the Datum interface.
func (d *DDate) IsMax(ctx CompareContext) bool {
	return d.PGEpochDays() == pgdate.PosInfDate.PGEpochDays()
}

// IsMin implements the Datum interface.
func (d *DDate) IsMin(ctx CompareContext) bool {
	return d.PGEpochDays() == pgdate.NegInfDate.PGEpochDays()
}

// Max implements the Datum interface.
func (d *DDate) Max(ctx CompareContext) (Datum, bool) {
	return dMaxDate, true
}

// Min implements the Datum interface.
func (d *DDate) Min(ctx CompareContext) (Datum, bool) {
	return dMinDate, true
}

// AmbiguousFormat implements the Datum interface.
func (*DDate) AmbiguousFormat() bool { return true }

// FormatDate writes d into ctx according to the format flags.
func FormatDate(d pgdate.Date, ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lexbase.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	d.Format(&ctx.Buffer)
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Format implements the NodeFormatter interface.
func (d *DDate) Format(ctx *FmtCtx) {
	FormatDate(d.Date, ctx)
}

// Size implements the Datum interface.
func (d *DDate) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DTime is the time Datum.
type DTime timeofday.TimeOfDay

// MakeDTime creates a DTime from a TimeOfDay.
func MakeDTime(t timeofday.TimeOfDay) *DTime {
	d := DTime(t)
	return &d
}

// ParseDTime parses and returns the *DTime Datum value represented by the
// provided string, or an error if parsing is unsuccessful.
//
// The dependsOnContext return value indicates if we had to consult the
// ParseContext (either for the time or the local timezone).
func ParseDTime(
	ctx ParseContext, s string, precision time.Duration,
) (_ *DTime, dependsOnContext bool, _ error) {
	now := relativeParseTime(ctx)

	// Special case on 24:00 and 24:00:00 as the parser
	// does not handle these correctly.
	if DTimeMaxTimeRegex.MatchString(s) {
		return MakeDTime(timeofday.Time2400), false, nil
	}

	s = timeutil.ReplaceLibPQTimePrefix(s)

	t, dependsOnContext, err := pgdate.ParseTimeWithoutTimezone(now, dateStyle(ctx), s)
	if err != nil {
		return nil, false, MakeParseError(s, types.Time, err)
	}
	return MakeDTime(timeofday.FromTime(t).Round(precision)), dependsOnContext, nil
}

// ResolvedType implements the TypedExpr interface.
func (*DTime) ResolvedType() *types.T {
	return types.Time
}

// Compare implements the Datum interface.
func (d *DTime) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DTime) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	return compareTimestamps(ctx, d, other)
}

// Prev implements the Datum interface.
func (d *DTime) Prev(ctx CompareContext) (Datum, bool) {
	if d.IsMin(ctx) {
		return nil, false
	}
	prev := *d - 1
	return &prev, true
}

// Round returns a new DTime to the specified precision.
func (d *DTime) Round(precision time.Duration) *DTime {
	return MakeDTime(timeofday.TimeOfDay(*d).Round(precision))
}

// Next implements the Datum interface.
func (d *DTime) Next(ctx CompareContext) (Datum, bool) {
	if d.IsMax(ctx) {
		return nil, false
	}
	next := *d + 1
	return &next, true
}

var dTimeMin = MakeDTime(timeofday.Min)
var dTimeMax = MakeDTime(timeofday.Max)

// IsMax implements the Datum interface.
func (d *DTime) IsMax(ctx CompareContext) bool {
	return *d == *dTimeMax
}

// IsMin implements the Datum interface.
func (d *DTime) IsMin(ctx CompareContext) bool {
	return *d == *dTimeMin
}

// Max implements the Datum interface.
func (d *DTime) Max(ctx CompareContext) (Datum, bool) {
	return dTimeMax, true
}

// Min implements the Datum interface.
func (d *DTime) Min(ctx CompareContext) (Datum, bool) {
	return dTimeMin, true
}

// AmbiguousFormat implements the Datum interface.
func (*DTime) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DTime) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lexbase.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	ctx.Write(timeofday.TimeOfDay(*d).AppendFormat(ctx.scratch[:0]))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DTime) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DTimeTZ is the time with time zone Datum.
type DTimeTZ struct {
	timetz.TimeTZ
}

var (
	dZeroTimeTZ = NewDTimeTZFromOffset(timeofday.Min, 0)
	// DMinTimeTZ is the min TimeTZ.
	DMinTimeTZ = NewDTimeTZFromOffset(timeofday.Min, timetz.MinTimeTZOffsetSecs)
	// DMaxTimeTZ is the max TimeTZ.
	DMaxTimeTZ = NewDTimeTZFromOffset(timeofday.Max, timetz.MaxTimeTZOffsetSecs)
)

// NewDTimeTZ creates a DTimeTZ from a timetz.TimeTZ.
func NewDTimeTZ(t timetz.TimeTZ) *DTimeTZ {
	return &DTimeTZ{t}
}

// NewDTimeTZFromTime creates a DTimeTZ from time.Time.
func NewDTimeTZFromTime(t time.Time) *DTimeTZ {
	return &DTimeTZ{timetz.MakeTimeTZFromTime(t)}
}

// NewDTimeTZFromOffset creates a DTimeTZ from a TimeOfDay and offset.
func NewDTimeTZFromOffset(t timeofday.TimeOfDay, offsetSecs int32) *DTimeTZ {
	return &DTimeTZ{timetz.MakeTimeTZ(t, offsetSecs)}
}

// NewDTimeTZFromLocation creates a DTimeTZ from a TimeOfDay and time.Location.
func NewDTimeTZFromLocation(t timeofday.TimeOfDay, loc *time.Location) *DTimeTZ {
	return &DTimeTZ{timetz.MakeTimeTZFromLocation(t, loc)}
}

// ParseDTimeTZ parses and returns the *DTime Datum value represented by the
// provided string, or an error if parsing is unsuccessful.
//
// The dependsOnContext return value indicates if we had to consult the
// ParseContext (either for the time or the local timezone).
func ParseDTimeTZ(
	ctx ParseContext, s string, precision time.Duration,
) (_ *DTimeTZ, dependsOnContext bool, _ error) {
	now := relativeParseTime(ctx)
	d, dependsOnContext, err := timetz.ParseTimeTZ(now, dateStyle(ctx), s, precision)
	if err != nil {
		return nil, false, err
	}
	return NewDTimeTZ(d), dependsOnContext, nil
}

// ResolvedType implements the TypedExpr interface.
func (*DTimeTZ) ResolvedType() *types.T {
	return types.TimeTZ
}

// Compare implements the Datum interface.
func (d *DTimeTZ) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DTimeTZ) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	return compareTimestamps(ctx, d, other)
}

// Prev implements the Datum interface.
func (d *DTimeTZ) Prev(ctx CompareContext) (Datum, bool) {
	if d.IsMin(ctx) {
		return nil, false
	}
	// In the common case, the absolute time doesn't change, we simply decrement
	// the offset by one second and increment the time of day by one second. Once
	// we hit the minimum offset for the current absolute time, then we decrement
	// the absolute time by one microsecond and wrap around to the highest offset
	// for the new absolute time. This aligns with how Before and After are
	// defined for TimeTZ.
	var newTimeOfDay timeofday.TimeOfDay
	var newOffsetSecs int32
	if d.OffsetSecs == timetz.MinTimeTZOffsetSecs ||
		d.TimeOfDay+duration.MicrosPerSec > timeofday.Max {
		newTimeOfDay = d.TimeOfDay - 1
		shiftSeconds := int32((newTimeOfDay - timeofday.Min) / duration.MicrosPerSec)
		if d.OffsetSecs+shiftSeconds > timetz.MaxTimeTZOffsetSecs {
			shiftSeconds = timetz.MaxTimeTZOffsetSecs - d.OffsetSecs
		}
		newOffsetSecs = d.OffsetSecs + shiftSeconds
		newTimeOfDay -= timeofday.TimeOfDay(shiftSeconds) * duration.MicrosPerSec
	} else {
		newTimeOfDay = d.TimeOfDay + duration.MicrosPerSec
		newOffsetSecs = d.OffsetSecs - 1
	}
	return NewDTimeTZFromOffset(newTimeOfDay, newOffsetSecs), true
}

// Next implements the Datum interface.
func (d *DTimeTZ) Next(ctx CompareContext) (Datum, bool) {
	if d.IsMax(ctx) {
		return nil, false
	}
	// In the common case, the absolute time doesn't change, we simply increment
	// the offset by one second and decrement the time of day by one second. Once
	// we hit the maximum offset for the current absolute time, then we increment
	// the absolute time by one microsecond and wrap around to the lowest offset
	// for the new absolute time. This aligns with how Before and After are
	// defined for TimeTZ.
	var newTimeOfDay timeofday.TimeOfDay
	var newOffsetSecs int32
	if d.OffsetSecs == timetz.MaxTimeTZOffsetSecs ||
		d.TimeOfDay-duration.MicrosPerSec < timeofday.Min {
		newTimeOfDay = d.TimeOfDay + 1
		shiftSeconds := int32((timeofday.Max - newTimeOfDay) / duration.MicrosPerSec)
		if d.OffsetSecs-shiftSeconds < timetz.MinTimeTZOffsetSecs {
			shiftSeconds = d.OffsetSecs - timetz.MinTimeTZOffsetSecs
		}
		newOffsetSecs = d.OffsetSecs - shiftSeconds
		newTimeOfDay += timeofday.TimeOfDay(shiftSeconds) * duration.MicrosPerSec
	} else {
		newTimeOfDay = d.TimeOfDay - duration.MicrosPerSec
		newOffsetSecs = d.OffsetSecs + 1
	}
	return NewDTimeTZFromOffset(newTimeOfDay, newOffsetSecs), true
}

// IsMax implements the Datum interface.
func (d *DTimeTZ) IsMax(ctx CompareContext) bool {
	return d.TimeOfDay == DMaxTimeTZ.TimeOfDay && d.OffsetSecs == timetz.MaxTimeTZOffsetSecs
}

// IsMin implements the Datum interface.
func (d *DTimeTZ) IsMin(ctx CompareContext) bool {
	return d.TimeOfDay == DMinTimeTZ.TimeOfDay && d.OffsetSecs == timetz.MinTimeTZOffsetSecs
}

// Max implements the Datum interface.
func (d *DTimeTZ) Max(ctx CompareContext) (Datum, bool) {
	return DMaxTimeTZ, true
}

// Round returns a new DTimeTZ to the specified precision.
func (d *DTimeTZ) Round(precision time.Duration) *DTimeTZ {
	return NewDTimeTZ(d.TimeTZ.Round(precision))
}

// Min implements the Datum interface.
func (d *DTimeTZ) Min(ctx CompareContext) (Datum, bool) {
	return DMinTimeTZ, true
}

// AmbiguousFormat implements the Datum interface.
func (*DTimeTZ) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DTimeTZ) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lexbase.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	ctx.Write(d.TimeTZ.AppendFormat(ctx.scratch[:0]))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DTimeTZ) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// NewTimestampExceedsBoundsError returns a new "exceeds supported timestamp
// bounds" error for the given timestamp, with the correct pgcode.
func NewTimestampExceedsBoundsError(t time.Time) error {
	return pgerror.Newf(
		pgcode.InvalidTimeZoneDisplacementValue,
		"timestamp %q exceeds supported timestamp bounds",
		t.Format(time.RFC3339),
	)
}

// DTimestamp is the timestamp Datum.
type DTimestamp struct {
	// Time always has UTC location.
	time.Time
}

// MakeDTimestamp creates a DTimestamp with specified precision.
func MakeDTimestamp(t time.Time, precision time.Duration) (*DTimestamp, error) {
	ret := t.Round(precision)
	if ret.After(MaxSupportedTime) || ret.Before(MinSupportedTime) {
		return nil, NewTimestampExceedsBoundsError(ret)
	}
	return &DTimestamp{Time: ret}, nil
}

// MustMakeDTimestamp wraps MakeDTimestamp but panics if there is an error.
// This is intended for testing applications only.
func MustMakeDTimestamp(t time.Time, precision time.Duration) *DTimestamp {
	ret, err := MakeDTimestamp(t, precision)
	if err != nil {
		panic(err)
	}
	return ret
}

// DZeroTimestamp is the zero-valued DTimestamp.
var DZeroTimestamp = &DTimestamp{}

// time.Time formats.
const (
	// timestampTZOutputFormat is used to output all TimestampTZs.
	// Note the second offset is missing here -- this is to maintain
	// backward compatibility with casting timestamptz to strings.
	timestampTZOutputFormat = "2006-01-02 15:04:05.999999-07:00"
	// timestampOutputFormat is used to output all Timestamps.
	timestampOutputFormat = "2006-01-02 15:04:05.999999"
)

// ParseDTimestamp parses and returns the *DTimestamp Datum value represented by
// the provided string in UTC, or an error if parsing is unsuccessful.
//
// The dependsOnContext return value indicates if we had to consult the
// ParseContext (either for the time or the local timezone).
func ParseDTimestamp(
	ctx ParseContext, s string, precision time.Duration,
) (_ *DTimestamp, dependsOnContext bool, _ error) {
	now := relativeParseTime(ctx)
	t, dependsOnContext, err := pgdate.ParseTimestampWithoutTimezone(now, dateStyle(ctx), s)
	if err != nil {
		return nil, false, err
	}
	d, err := MakeDTimestamp(t, precision)
	return d, dependsOnContext, err
}

// AsDTimestamp attempts to retrieve a DTimestamp from an Expr, returning a DTimestamp and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DTimestamp wrapped by a
// *DOidWrapper is possible.
func AsDTimestamp(e Expr) (DTimestamp, bool) {
	switch t := e.(type) {
	case *DTimestamp:
		return *t, true
	case *DOidWrapper:
		return AsDTimestamp(t.Wrapped)
	}
	return DTimestamp{}, false
}

// MustBeDTimestamp attempts to retrieve a DTimestamp from an Expr, panicking if the
// assertion fails.
func MustBeDTimestamp(e Expr) DTimestamp {
	t, ok := AsDTimestamp(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DTimestamp, found %T", e))
	}
	return t
}

// Round returns a new DTimestamp to the specified precision.
func (d *DTimestamp) Round(precision time.Duration) (*DTimestamp, error) {
	return MakeDTimestamp(d.Time, precision)
}

// ResolvedType implements the TypedExpr interface.
func (*DTimestamp) ResolvedType() *types.T {
	return types.Timestamp
}

// TimeFromDatumForComparison gets the time from a datum object to use
// strictly for comparison usage.
func TimeFromDatumForComparison(ctx CompareContext, d Datum) (time.Time, error) {
	d = ctx.UnwrapDatum(d)
	switch t := d.(type) {
	case *DDate:
		ts, err := MakeDTimestampTZFromDate(ctx.GetLocation(), t)
		if err != nil {
			return time.Time{}, err
		}
		return ts.Time, nil
	case *DTimestampTZ:
		return t.Time, nil
	case *DTimestamp:
		// Normalize to the timezone of the context.
		_, zoneOffset := t.Time.In(ctx.GetLocation()).Zone()
		ts := t.Time.In(ctx.GetLocation()).Add(-time.Duration(zoneOffset) * time.Second)
		return ts, nil
	case *DTime:
		// Normalize to the timezone of the context.
		toTime := timeofday.TimeOfDay(*t).ToTime()
		_, zoneOffsetSecs := toTime.In(ctx.GetLocation()).Zone()
		return toTime.In(ctx.GetLocation()).Add(-time.Duration(zoneOffsetSecs) * time.Second), nil
	case *DTimeTZ:
		return t.ToTime(), nil
	default:
		return time.Time{}, errors.AssertionFailedf("unexpected type: %v", t.ResolvedType())
	}
}

type infiniteDateComparison int

const (
	// Note: the order of the constants here is important.
	negativeInfinity infiniteDateComparison = iota
	finite
	positiveInfinity
)

func checkInfiniteDate(ctx CompareContext, d Datum) infiniteDateComparison {
	if _, isDate := d.(*DDate); isDate {
		if d.IsMax(ctx) {
			return positiveInfinity
		}
		if d.IsMin(ctx) {
			return negativeInfinity
		}
	}
	return finite
}

// compareTimestamps takes in two time-related datums and compares them as
// timestamps while paying attention to time zones if needed. It returns -1, 0,
// or +1 for "less", "equal", and "greater", respectively.
//
// Datums are allowed to be one of DDate, DTimestamp, DTimestampTZ, DTime,
// DTimeTZ. For all other datum types it will panic; also, comparing two DDates
// is not supported.
func compareTimestamps(ctx CompareContext, l Datum, r Datum) (int, error) {
	leftInf := checkInfiniteDate(ctx, l)
	rightInf := checkInfiniteDate(ctx, r)
	if leftInf != finite || rightInf != finite {
		// At least one of the datums is an infinite date.
		if leftInf != finite && rightInf != finite {
			// Both datums cannot be infinite dates at the same time because we
			// wouldn't use this method.
			return 0, errors.AssertionFailedf("unexpectedly two infinite dates in compareTimestamps")
		}
		// Exactly one of the datums is an infinite date and another is a finite
		// datums (not necessarily a date). We can just subtract the returned
		// values to get the desired result for comparison.
		return int(leftInf - rightInf), nil
	}
	lTime, lErr := TimeFromDatumForComparison(ctx, l)
	rTime, rErr := TimeFromDatumForComparison(ctx, r)
	if lErr != nil || rErr != nil {
		return 0, makeUnsupportedComparisonMessage(l, r)
	}
	if lTime.Before(rTime) {
		return -1, nil
	}
	if rTime.Before(lTime) {
		return 1, nil
	}

	// If either side is a TimeTZ, then we must compare timezones before
	// when comparing. If comparing a non-TimeTZ value, and the times are
	// equal, then we must compare relative to the current zone we are at.
	//
	// This is a special quirk of TimeTZ and does not apply to TimestampTZ,
	// as TimestampTZ does not store a timezone offset and is based on
	// the current zone.
	_, leftIsTimeTZ := l.(*DTimeTZ)
	_, rightIsTimeTZ := r.(*DTimeTZ)

	// If neither side is TimeTZ, this is always equal at this point.
	if !leftIsTimeTZ && !rightIsTimeTZ {
		return 0, nil
	}

	_, zoneOffset := ctx.GetRelativeParseTime().Zone()
	lOffset := int32(-zoneOffset)
	rOffset := int32(-zoneOffset)

	if leftIsTimeTZ {
		lOffset = l.(*DTimeTZ).OffsetSecs
	}
	if rightIsTimeTZ {
		rOffset = r.(*DTimeTZ).OffsetSecs
	}

	if lOffset > rOffset {
		return 1, nil
	}
	if lOffset < rOffset {
		return -1, nil
	}
	return 0, nil
}

// Compare implements the Datum interface.
func (d *DTimestamp) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DTimestamp) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	return compareTimestamps(ctx, d, other)
}

// Prev implements the Datum interface.
func (d *DTimestamp) Prev(ctx CompareContext) (Datum, bool) {
	if d.IsMin(ctx) {
		return nil, false
	}
	return &DTimestamp{Time: d.Add(-time.Microsecond)}, true
}

// Next implements the Datum interface.
func (d *DTimestamp) Next(ctx CompareContext) (Datum, bool) {
	if d.IsMax(ctx) {
		return nil, false
	}
	return &DTimestamp{Time: d.Add(time.Microsecond)}, true
}

// IsMax implements the Datum interface.
func (d *DTimestamp) IsMax(ctx CompareContext) bool {
	return d.Equal(MaxSupportedTime)
}

// IsMin implements the Datum interface.
func (d *DTimestamp) IsMin(ctx CompareContext) bool {
	return d.Equal(MinSupportedTime)
}

// Min implements the Datum interface.
func (d *DTimestamp) Min(ctx CompareContext) (Datum, bool) {
	return &DTimestamp{Time: MinSupportedTime}, true
}

// Max implements the Datum interface.
func (d *DTimestamp) Max(ctx CompareContext) (Datum, bool) {
	return &DTimestamp{Time: MaxSupportedTime}, true
}

// AmbiguousFormat implements the Datum interface.
func (*DTimestamp) AmbiguousFormat() bool { return true }

// FormatTimestamp outputs a timestamp in the UTC timezone.
func FormatTimestamp(t time.Time) string {
	return t.UTC().Format(timestampOutputFormat)
}

// Format implements the NodeFormatter interface.
func (d *DTimestamp) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lexbase.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	ctx.WriteString(FormatTimestamp(d.Time))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DTimestamp) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DTimestampTZ is the timestamp Datum that is rendered with session offset.
type DTimestampTZ struct {
	time.Time
}

// MakeDTimestampTZ creates a DTimestampTZ with specified precision.
func MakeDTimestampTZ(t time.Time, precision time.Duration) (*DTimestampTZ, error) {
	ret := t.Round(precision)
	if ret.After(MaxSupportedTime) || ret.Before(MinSupportedTime) {
		return nil, NewTimestampExceedsBoundsError(ret)
	}
	return &DTimestampTZ{Time: ret}, nil
}

// MustMakeDTimestampTZ wraps MakeDTimestampTZ but panics if there is an error.
// This is intended for testing applications only.
func MustMakeDTimestampTZ(t time.Time, precision time.Duration) *DTimestampTZ {
	ret, err := MakeDTimestampTZ(t, precision)
	if err != nil {
		panic(err)
	}
	return ret
}

// MakeDTimestampTZFromDate creates a DTimestampTZ from a DDate.
// This will be equivalent to the midnight of the given zone.
func MakeDTimestampTZFromDate(loc *time.Location, d *DDate) (*DTimestampTZ, error) {
	t, err := d.ToTime()
	if err != nil {
		return nil, err
	}
	// Normalize to the correct zone.
	t = t.In(loc)
	_, offset := t.Zone()
	return MakeDTimestampTZ(t.Add(time.Duration(-offset)*time.Second), time.Microsecond)
}

// ParseDTimestampTZ parses and returns the *DTimestampTZ Datum value represented by
// the provided string in the provided location, or an error if parsing is unsuccessful.
//
// The dependsOnContext return value indicates if we had to consult the
// ParseContext (either for the time or the local timezone).
func ParseDTimestampTZ(
	ctx ParseContext, s string, precision time.Duration,
) (_ *DTimestampTZ, dependsOnContext bool, _ error) {
	now := relativeParseTime(ctx)
	t, dependsOnContext, err := pgdate.ParseTimestamp(now, dateStyle(ctx), s)
	if err != nil {
		return nil, false, err
	}
	// Always normalize time to the current location.
	d, err := MakeDTimestampTZ(t, precision)
	return d, dependsOnContext, err
}

// DZeroTimestampTZ is the zero-valued DTimestampTZ.
var DZeroTimestampTZ = &DTimestampTZ{}

// AsDTimestampTZ attempts to retrieve a DTimestampTZ from an Expr, returning a
// DTimestampTZ and a flag signifying whether the assertion was successful. The
// function should be used instead of direct type assertions wherever a
// *DTimestamp wrapped by a *DOidWrapper is possible.
func AsDTimestampTZ(e Expr) (DTimestampTZ, bool) {
	switch t := e.(type) {
	case *DTimestampTZ:
		return *t, true
	case *DOidWrapper:
		return AsDTimestampTZ(t.Wrapped)
	}
	return DTimestampTZ{}, false
}

// MustBeDTimestampTZ attempts to retrieve a DTimestampTZ from an Expr,
// panicking if the assertion fails.
func MustBeDTimestampTZ(e Expr) DTimestampTZ {
	t, ok := AsDTimestampTZ(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DTimestampTZ, found %T", e))
	}
	return t
}

// Round returns a new DTimestampTZ to the specified precision.
func (d *DTimestampTZ) Round(precision time.Duration) (*DTimestampTZ, error) {
	return MakeDTimestampTZ(d.Time, precision)
}

// ResolvedType implements the TypedExpr interface.
func (*DTimestampTZ) ResolvedType() *types.T {
	return types.TimestampTZ
}

// Compare implements the Datum interface.
func (d *DTimestampTZ) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DTimestampTZ) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	return compareTimestamps(ctx, d, other)
}

// Prev implements the Datum interface.
func (d *DTimestampTZ) Prev(ctx CompareContext) (Datum, bool) {
	if d.IsMin(ctx) {
		return nil, false
	}
	return &DTimestampTZ{Time: d.Add(-time.Microsecond)}, true
}

// Next implements the Datum interface.
func (d *DTimestampTZ) Next(ctx CompareContext) (Datum, bool) {
	if d.IsMax(ctx) {
		return nil, false
	}
	return &DTimestampTZ{Time: d.Add(time.Microsecond)}, true
}

// IsMax implements the Datum interface.
func (d *DTimestampTZ) IsMax(ctx CompareContext) bool {
	return d.Equal(MaxSupportedTime)
}

// IsMin implements the Datum interface.
func (d *DTimestampTZ) IsMin(ctx CompareContext) bool {
	return d.Equal(MinSupportedTime)
}

// Min implements the Datum interface.
func (d *DTimestampTZ) Min(ctx CompareContext) (Datum, bool) {
	return &DTimestampTZ{Time: MinSupportedTime}, true
}

// Max implements the Datum interface.
func (d *DTimestampTZ) Max(ctx CompareContext) (Datum, bool) {
	return &DTimestampTZ{Time: MaxSupportedTime}, true
}

// AmbiguousFormat implements the Datum interface.
func (*DTimestampTZ) AmbiguousFormat() bool { return true }

// FormatTimestampTZ formats the given timestamp with timezone into the provided
// buffer.
func FormatTimestampTZ(t time.Time, buf *bytes.Buffer) {
	buf.WriteString(t.Format(timestampTZOutputFormat))
	_, offsetSecs := t.Zone()
	// Only output remaining seconds offsets if it is available.
	// This is to maintain backward compatibility with older CRDB versions,
	// where we only output HH:MM.
	if secondOffset := offsetSecs % 60; secondOffset != 0 {
		if secondOffset < 0 {
			secondOffset = 60 + secondOffset
		}
		buf.WriteByte(':')
		buf.WriteString(fmt.Sprintf("%02d", secondOffset))
	}
}

// Format implements the NodeFormatter interface.
func (d *DTimestampTZ) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lexbase.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	FormatTimestampTZ(d.Time, &ctx.Buffer)
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DTimestampTZ) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// EvalAtTimeZone evaluates this TimestampTZ as if it were in the supplied
// location, returning a timestamp without a timezone.
func (d *DTimestampTZ) EvalAtTimeZone(loc *time.Location) (*DTimestamp, error) {
	_, locOffset := d.Time.In(loc).Zone()
	t := d.Time.UTC().Add(time.Duration(locOffset) * time.Second).UTC()
	return MakeDTimestamp(t, time.Microsecond)
}

// DInterval is the interval Datum.
type DInterval struct {
	duration.Duration
}

// AsDInterval attempts to retrieve a DInterval from an Expr, panicking if the
// assertion fails.
func AsDInterval(e Expr) (*DInterval, bool) {
	switch t := e.(type) {
	case *DInterval:
		return t, true
	}
	return nil, false
}

// MustBeDInterval attempts to retrieve a DInterval from an Expr, panicking if the
// assertion fails.
func MustBeDInterval(e Expr) *DInterval {
	t, ok := AsDInterval(e)
	if ok {
		return t
	}
	panic(errors.AssertionFailedf("expected *DInterval, found %T", e))
}

// NewDInterval creates a new DInterval.
func NewDInterval(d duration.Duration, itm types.IntervalTypeMetadata) *DInterval {
	truncateInterval(&d, itm)
	return &DInterval{Duration: d}
}

// ParseDInterval parses and returns the *DInterval Datum value represented by the provided
// string, or an error if parsing is unsuccessful.
func ParseDInterval(style duration.IntervalStyle, s string) (*DInterval, error) {
	return ParseDIntervalWithTypeMetadata(style, s, types.DefaultIntervalTypeMetadata)
}

// truncateInterval truncates the input interval downward to the nearest
// interval quantity specified by the DurationField input.
// If precision is set for seconds, this will instead round at the second layer.
func truncateInterval(d *duration.Duration, itm types.IntervalTypeMetadata) {
	switch itm.DurationField.DurationType {
	case types.IntervalDurationType_YEAR:
		d.Months = d.Months - d.Months%12
		d.Days = 0
		d.SetNanos(0)
	case types.IntervalDurationType_MONTH:
		d.Days = 0
		d.SetNanos(0)
	case types.IntervalDurationType_DAY:
		d.SetNanos(0)
	case types.IntervalDurationType_HOUR:
		d.SetNanos(d.Nanos() - d.Nanos()%time.Hour.Nanoseconds())
	case types.IntervalDurationType_MINUTE:
		d.SetNanos(d.Nanos() - d.Nanos()%time.Minute.Nanoseconds())
	case types.IntervalDurationType_SECOND, types.IntervalDurationType_UNSET:
		if itm.PrecisionIsSet || itm.Precision > 0 {
			prec := TimeFamilyPrecisionToRoundDuration(itm.Precision)
			d.SetNanos(time.Duration(d.Nanos()).Round(prec).Nanoseconds())
		}
	}
}

// ParseDIntervalWithTypeMetadata is like ParseDInterval, but it also takes a
// types.IntervalTypeMetadata that both specifies the units for unitless, numeric intervals
// and also specifies the precision of the interval.
func ParseDIntervalWithTypeMetadata(
	style duration.IntervalStyle, s string, itm types.IntervalTypeMetadata,
) (*DInterval, error) {
	d, err := ParseIntervalWithTypeMetadata(style, s, itm)
	if err != nil {
		return nil, err
	}
	return &DInterval{Duration: d}, nil
}

// ParseIntervalWithTypeMetadata is the same as ParseDIntervalWithTypeMetadata
// but returns a duration.Duration.
func ParseIntervalWithTypeMetadata(
	style duration.IntervalStyle, s string, itm types.IntervalTypeMetadata,
) (duration.Duration, error) {
	d, err := duration.ParseInterval(style, s, itm)
	if err != nil {
		return d, err
	}
	truncateInterval(&d, itm)
	return d, nil
}

// ResolvedType implements the TypedExpr interface.
func (*DInterval) ResolvedType() *types.T {
	return types.Interval
}

// Compare implements the Datum interface.
func (d *DInterval) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DInterval) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DInterval)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	res := d.Duration.Compare(v.Duration)
	return res, nil
}

// Prev implements the Datum interface.
func (d *DInterval) Prev(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DInterval) Next(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (d *DInterval) IsMax(ctx CompareContext) bool {
	return d.Duration == dMaxInterval.Duration
}

// IsMin implements the Datum interface.
func (d *DInterval) IsMin(ctx CompareContext) bool {
	return d.Duration == dMinInterval.Duration
}

var (
	dZeroInterval = &DInterval{}
	dMaxInterval  = &DInterval{duration.MakeDuration(math.MaxInt64, math.MaxInt64, math.MaxInt64)}
	dMinInterval  = &DInterval{duration.MakeDuration(math.MinInt64, math.MinInt64, math.MinInt64)}
)

// Max implements the Datum interface.
func (d *DInterval) Max(ctx CompareContext) (Datum, bool) {
	return dMaxInterval, true
}

// Min implements the Datum interface.
func (d *DInterval) Min(ctx CompareContext) (Datum, bool) {
	return dMinInterval, true
}

// ValueAsISO8601String returns the interval as an ISO 8601 Duration string (e.g. "P1Y2MT6S").
func (d *DInterval) ValueAsISO8601String() string {
	return d.Duration.ISO8601String()
}

// AmbiguousFormat implements the Datum interface.
func (*DInterval) AmbiguousFormat() bool { return true }

// FormatDuration writes d into ctx according to the format flags.
func FormatDuration(d duration.Duration, ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lexbase.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	d.FormatWithStyle(&ctx.Buffer, ctx.dataConversionConfig.IntervalStyle)
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Format implements the NodeFormatter interface.
func (d *DInterval) Format(ctx *FmtCtx) {
	FormatDuration(d.Duration, ctx)
}

// Size implements the Datum interface.
func (d *DInterval) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DGeography is the Geometry Datum.
type DGeography struct {
	geo.Geography
}

// NewDGeography returns a new Geography Datum.
func NewDGeography(g geo.Geography) *DGeography {
	return &DGeography{Geography: g}
}

// AsDGeography attempts to retrieve a *DGeography from an Expr, returning a
// *DGeography and a flag signifying whether the assertion was successful. The
// function should be used instead of direct type assertions wherever a
// *DGeography wrapped by a *DOidWrapper is possible.
func AsDGeography(e Expr) (*DGeography, bool) {
	switch t := e.(type) {
	case *DGeography:
		return t, true
	case *DOidWrapper:
		return AsDGeography(t.Wrapped)
	}
	return nil, false
}

// MustBeDGeography attempts to retrieve a *DGeography from an Expr, panicking
// if the assertion fails.
func MustBeDGeography(e Expr) *DGeography {
	i, ok := AsDGeography(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DGeography, found %T", e))
	}
	return i
}

// ParseDGeography attempts to pass `str` as a Geography type.
func ParseDGeography(str string) (*DGeography, error) {
	g, err := geo.ParseGeography(str)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse geography")
	}
	return &DGeography{Geography: g}, nil
}

// ResolvedType implements the TypedExpr interface.
func (*DGeography) ResolvedType() *types.T {
	return types.Geography
}

// Compare implements the Datum interface.
func (d *DGeography) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DGeography) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DGeography)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	res := d.Geography.Compare(v.Geography)
	return res, nil
}

// Prev implements the Datum interface.
func (d *DGeography) Prev(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DGeography) Next(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (d *DGeography) IsMax(ctx CompareContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DGeography) IsMin(ctx CompareContext) bool {
	return false
}

// Max implements the Datum interface.
func (d *DGeography) Max(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Min implements the Datum interface.
func (d *DGeography) Min(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// AmbiguousFormat implements the Datum interface.
func (*DGeography) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DGeography) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lexbase.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	ctx.WriteString(d.Geography.EWKBHex())
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DGeography) Size() uintptr {
	return d.Geography.SpatialObjectRef().MemSize()
}

// DGeometry is the Geometry Datum.
type DGeometry struct {
	geo.Geometry
}

// NewDGeometry returns a new Geometry Datum.
func NewDGeometry(g geo.Geometry) *DGeometry {
	return &DGeometry{Geometry: g}
}

// AsDGeometry attempts to retrieve a *DGeometry from an Expr, returning a
// *DGeometry and a flag signifying whether the assertion was successful. The
// function should be used instead of direct type assertions wherever a
// *DGeometry wrapped by a *DOidWrapper is possible.
func AsDGeometry(e Expr) (*DGeometry, bool) {
	switch t := e.(type) {
	case *DGeometry:
		return t, true
	case *DOidWrapper:
		return AsDGeometry(t.Wrapped)
	}
	return nil, false
}

// MustBeDGeometry attempts to retrieve a *DGeometry from an Expr, panicking
// if the assertion fails.
func MustBeDGeometry(e Expr) *DGeometry {
	i, ok := AsDGeometry(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DGeometry, found %T", e))
	}
	return i
}

// ParseDGeometry attempts to pass `str` as a Geometry type.
func ParseDGeometry(str string) (*DGeometry, error) {
	g, err := geo.ParseGeometry(str)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse geometry")
	}
	return &DGeometry{Geometry: g}, nil
}

// ResolvedType implements the TypedExpr interface.
func (*DGeometry) ResolvedType() *types.T {
	return types.Geometry
}

// Compare implements the Datum interface.
func (d *DGeometry) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DGeometry) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DGeometry)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	res := d.Geometry.Compare(v.Geometry)
	return res, nil
}

// Prev implements the Datum interface.
func (d *DGeometry) Prev(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DGeometry) Next(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (d *DGeometry) IsMax(ctx CompareContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DGeometry) IsMin(ctx CompareContext) bool {
	return false
}

// Max implements the Datum interface.
func (d *DGeometry) Max(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Min implements the Datum interface.
func (d *DGeometry) Min(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// AmbiguousFormat implements the Datum interface.
func (*DGeometry) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DGeometry) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lexbase.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	ctx.WriteString(d.Geometry.EWKBHex())
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DGeometry) Size() uintptr {
	return d.Geometry.SpatialObjectRef().MemSize()
}

// DBox2D is the Datum representation of the Box2D type.
type DBox2D struct {
	geo.CartesianBoundingBox
}

// NewDBox2D returns a new Box2D Datum.
func NewDBox2D(b geo.CartesianBoundingBox) *DBox2D {
	return &DBox2D{CartesianBoundingBox: b}
}

// ParseDBox2D attempts to pass `str` as a Box2D type.
func ParseDBox2D(str string) (*DBox2D, error) {
	b, err := geo.ParseCartesianBoundingBox(str)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse geometry")
	}
	return &DBox2D{CartesianBoundingBox: b}, nil
}

// AsDBox2D attempts to retrieve a *DBox2D from an Expr, returning a
// *DBox2D and a flag signifying whether the assertion was successful. The
// function should be used instead of direct type assertions wherever a
// *DBox2D wrapped by a *DOidWrapper is possible.
func AsDBox2D(e Expr) (*DBox2D, bool) {
	switch t := e.(type) {
	case *DBox2D:
		return t, true
	case *DOidWrapper:
		return AsDBox2D(t.Wrapped)
	}
	return nil, false
}

// MustBeDBox2D attempts to retrieve a *DBox2D from an Expr, panicking
// if the assertion fails.
func MustBeDBox2D(e Expr) *DBox2D {
	i, ok := AsDBox2D(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DBox2D, found %T", e))
	}
	return i
}

// ResolvedType implements the TypedExpr interface.
func (*DBox2D) ResolvedType() *types.T {
	return types.Box2D
}

// Compare implements the Datum interface.
func (d *DBox2D) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DBox2D) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DBox2D)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	res := d.CartesianBoundingBox.Compare(&v.CartesianBoundingBox)
	return res, nil
}

// Prev implements the Datum interface.
func (d *DBox2D) Prev(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DBox2D) Next(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (d *DBox2D) IsMax(ctx CompareContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DBox2D) IsMin(ctx CompareContext) bool {
	return false
}

// Max implements the Datum interface.
func (d *DBox2D) Max(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Min implements the Datum interface.
func (d *DBox2D) Min(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// AmbiguousFormat implements the Datum interface.
func (*DBox2D) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DBox2D) Format(ctx *FmtCtx) {
	f := ctx.flags
	bareStrings := f.HasFlags(FmtFlags(lexbase.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	ctx.Write(d.CartesianBoundingBox.AppendFormat(ctx.scratch[:0]))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// Size implements the Datum interface.
func (d *DBox2D) Size() uintptr {
	return unsafe.Sizeof(*d) + unsafe.Sizeof(d.CartesianBoundingBox)
}

// DJSON is the JSON Datum.
type DJSON struct{ json.JSON }

// NewDJSON is a helper routine to create a DJSON initialized from its argument.
func NewDJSON(j json.JSON) *DJSON {
	return &DJSON{j}
}

// ParseDJSON takes a string of JSON and returns a DJSON value.
func ParseDJSON(s string) (Datum, error) {
	j, err := json.ParseJSON(s)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.Syntax, "could not parse JSON")
	}
	return NewDJSON(j), nil
}

// MakeDJSON returns a JSON value given a Go-style representation of JSON.
// * JSON null is Go `nil`,
// * JSON true is Go `true`,
// * JSON false is Go `false`,
// * JSON numbers are json.Number | int | int64 | float64,
// * JSON string is a Go string,
// * JSON array is a Go []interface{},
// * JSON object is a Go map[string]interface{}.
func MakeDJSON(d interface{}) (Datum, error) {
	j, err := json.MakeJSON(d)
	if err != nil {
		return nil, err
	}
	return &DJSON{j}, nil
}

var dNullJSON = NewDJSON(json.NullJSONValue)

// AsDJSON attempts to retrieve a *DJSON from an Expr, returning a *DJSON and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DJSON wrapped by a
// *DOidWrapper is possible.
func AsDJSON(e Expr) (*DJSON, bool) {
	switch t := e.(type) {
	case *DJSON:
		return t, true
	case *DOidWrapper:
		return AsDJSON(t.Wrapped)
	}
	return nil, false
}

// MustBeDJSON attempts to retrieve a DJSON from an Expr, panicking if the
// assertion fails.
func MustBeDJSON(e Expr) DJSON {
	i, ok := AsDJSON(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DJSON, found %T", e))
	}
	return *i
}

// AsJSON converts a datum into our standard json representation.
func AsJSON(
	d Datum, dcc sessiondatapb.DataConversionConfig, loc *time.Location,
) (json.JSON, error) {
	d = UnwrapDOidWrapper(d)
	switch t := d.(type) {
	case *DBool:
		return json.FromBool(bool(*t)), nil
	case *DInt:
		return json.FromInt(int(*t)), nil
	case *DFloat:
		return json.FromFloat64(float64(*t))
	case *DDecimal:
		return json.FromDecimal(t.Decimal), nil
	case *DString:
		return json.FromString(string(*t)), nil
	case *DCollatedString:
		return json.FromString(t.Contents), nil
	case *DEnum:
		return json.FromString(t.LogicalRep), nil
	case *DJSON:
		return t.JSON, nil
	case *DArray:
		builder := json.NewArrayBuilder(t.Len())
		for _, e := range t.Array {
			j, err := AsJSON(e, dcc, loc)
			if err != nil {
				return nil, err
			}
			builder.Add(j)
		}
		return builder.Build(), nil
	case *DTuple:
		builder := json.NewObjectBuilder(len(t.D))
		// We need to make sure that t.typ is initialized before getting the tuple
		// labels (it is valid for t.typ be left uninitialized when instantiating a
		// DTuple).
		t.maybePopulateType()
		labels := t.typ.TupleLabels()
		for i, e := range t.D {
			j, err := AsJSON(e, dcc, loc)
			if err != nil {
				return nil, err
			}
			var key string
			if i >= len(labels) {
				key = fmt.Sprintf("f%d", i+1)
			} else {
				key = labels[i]
			}
			builder.Add(key, j)
		}
		return builder.Build(), nil
	case *DTimestampTZ:
		// Our normal timestamp-formatting code uses a variation on RFC 3339,
		// without the T separator. This causes some compatibility problems
		// with certain JSON consumers, so we'll use an alternate formatting
		// path here to maintain consistency with PostgreSQL.
		return json.FromString(formatTime(t.Time.In(loc), time.RFC3339Nano)), nil
	case *DTimestamp:
		// This is RFC3339Nano, but without the TZ fields.
		return json.FromString(formatTime(t.UTC(), "2006-01-02T15:04:05.999999999")), nil
	case *DDate, *DUuid, *DOid, *DInterval, *DBytes, *DIPAddr, *DTime, *DTimeTZ, *DBitArray, *DBox2D,
		*DTSVector, *DTSQuery:
		return json.FromString(AsStringWithFlags(t, FmtBareStrings, FmtDataConversionConfig(dcc))), nil
	case *DGeometry:
		return json.FromSpatialObject(t.Geometry.SpatialObject(), geo.DefaultGeoJSONDecimalDigits)
	case *DGeography:
		return json.FromSpatialObject(t.Geography.SpatialObject(), geo.DefaultGeoJSONDecimalDigits)
	case *DVoid:
		return json.FromString(AsStringWithFlags(t, fmtRawStrings)), nil
	default:
		if d == DNull {
			return json.NullJSONValue, nil
		}

		return nil, errors.AssertionFailedf("unexpected type %T for AsJSON", d)
	}
}

// formatTime formats time with specified layout.
// TODO(yuzefovich): consider using this function in more places.
func formatTime(t time.Time, layout string) string {
	// We only need FmtCtx to access its buffer so
	// that we get 0 amortized allocations.
	ctx := NewFmtCtx(FmtSimple)
	ctx.Write(t.AppendFormat(ctx.scratch[:0], layout))
	return ctx.CloseAndGetString()
}

// ResolvedType implements the TypedExpr interface.
func (*DJSON) ResolvedType() *types.T {
	return types.Jsonb
}

// Compare implements the Datum interface.
func (d *DJSON) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DJSON) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DJSON)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	c, err := d.JSON.Compare(v.JSON)
	if err != nil {
		return 0, err
	}
	return c, nil
}

// Prev implements the Datum interface.
func (d *DJSON) Prev(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DJSON) Next(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (d *DJSON) IsMax(ctx CompareContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DJSON) IsMin(ctx CompareContext) bool {
	return d.JSON == json.NullJSONValue
}

// Max implements the Datum interface.
func (d *DJSON) Max(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Min implements the Datum interface.
func (d *DJSON) Min(ctx CompareContext) (Datum, bool) {
	return &DJSON{json.NullJSONValue}, true
}

// AmbiguousFormat implements the Datum interface.
func (*DJSON) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DJSON) Format(ctx *FmtCtx) {
	// TODO(justin): ideally the JSON string encoder should know it needs to
	// escape things to be inside SQL strings in order to avoid this allocation.
	s := d.JSON.String()
	if ctx.flags.HasFlags(fmtRawStrings) {
		ctx.WriteString(s)
	} else {
		// TODO(knz): This seems incorrect,
		// see https://github.com/cockroachdb/cockroach/issues/60673
		lexbase.EncodeSQLStringWithFlags(&ctx.Buffer, s, ctx.flags.EncodeFlags())
	}
}

// Size implements the Datum interface.
// TODO(justin): is this a frequently-called method? Should we be caching the computed size?
func (d *DJSON) Size() uintptr {
	return unsafe.Sizeof(*d) + d.JSON.Size()
}

// DTSQuery is the tsquery Datum.
type DTSQuery struct {
	tsearch.TSQuery
}

// Format implements the NodeFormatter interface.
func (d *DTSQuery) Format(ctx *FmtCtx) {
	bareStrings := ctx.HasFlags(FmtFlags(lexbase.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	str := d.TSQuery.String()
	if !bareStrings {
		str = strings.ReplaceAll(str, `'`, `''`)
	}
	ctx.WriteString(str)
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// ResolvedType implements the TypedExpr interface.
func (d *DTSQuery) ResolvedType() *types.T {
	return types.TSQuery
}

// AmbiguousFormat implements the Datum interface.
func (d *DTSQuery) AmbiguousFormat() bool { return true }

// Compare implements the Datum interface.
func (d *DTSQuery) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DTSQuery) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DTSQuery)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	l, r := d.String(), v.String()
	if l < r {
		return -1, nil
	} else if l > r {
		return 1, nil
	}
	return 0, nil
}

// Prev implements the Datum interface.
func (d *DTSQuery) Prev(_ CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DTSQuery) Next(_ CompareContext) (Datum, bool) {
	return nil, false
}

// IsMin implements the Datum interface.
func (d *DTSQuery) IsMin(_ CompareContext) bool {
	return len(d.String()) == 0
}

// IsMax implements the Datum interface.
func (d *DTSQuery) IsMax(ctx CompareContext) bool {
	return false
}

// Max implements the Datum interface.
func (d *DTSQuery) Max(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Min implements the Datum interface.
func (d *DTSQuery) Min(ctx CompareContext) (Datum, bool) {
	return &DTSQuery{}, false
}

// Size implements the Datum interface.
func (d *DTSQuery) Size() uintptr {
	return uintptr(len(d.TSQuery.String()))
}

// AsDTSQuery attempts to retrieve a DTSQuery from an Expr, returning a
// DTSQuery and a flag signifying whether the assertion was successful. The
// function should be used instead of direct type assertions wherever a
// *DTSQuery wrapped by a *DOidWrapper is possible.
func AsDTSQuery(e Expr) (*DTSQuery, bool) {
	switch t := e.(type) {
	case *DTSQuery:
		return t, true
	case *DOidWrapper:
		return AsDTSQuery(t.Wrapped)
	}
	return nil, false
}

// MustBeDTSQuery attempts to retrieve a DTSQuery from an Expr, panicking if the
// assertion fails.
func MustBeDTSQuery(e Expr) *DTSQuery {
	v, ok := AsDTSQuery(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DTSQuery, found %T", e))
	}
	return v
}

// NewDTSQuery is a helper routine to create a DTSQuery initialized from its
// argument.
func NewDTSQuery(q tsearch.TSQuery) *DTSQuery {
	return &DTSQuery{TSQuery: q}
}

// ParseDTSQuery takes a string of TSQuery and returns a DTSQuery value.
func ParseDTSQuery(s string) (Datum, error) {
	v, err := tsearch.ParseTSQuery(s)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.Syntax, "could not parse tsquery")
	}
	return NewDTSQuery(v), nil
}

// DTSVector is the tsvector Datum.
type DTSVector struct {
	tsearch.TSVector
}

// Format implements the NodeFormatter interface.
func (d *DTSVector) Format(ctx *FmtCtx) {
	bareStrings := ctx.HasFlags(FmtFlags(lexbase.EncBareStrings))
	if !bareStrings {
		ctx.WriteByte('\'')
	}
	str := d.TSVector.String()
	if !bareStrings {
		str = strings.ReplaceAll(str, `'`, `''`)
	}
	ctx.WriteString(str)
	if !bareStrings {
		ctx.WriteByte('\'')
	}
}

// ResolvedType implements the TypedExpr interface.
func (d *DTSVector) ResolvedType() *types.T {
	return types.TSVector
}

// AmbiguousFormat implements the Datum interface.
func (d *DTSVector) AmbiguousFormat() bool { return true }

// Compare implements the Datum interface.
func (d *DTSVector) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DTSVector) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DTSVector)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	l, r := d.String(), v.String()
	if l < r {
		return -1, nil
	} else if l > r {
		return 1, nil
	}
	return 0, nil
}

// Prev implements the Datum interface.
func (d *DTSVector) Prev(_ CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DTSVector) Next(_ CompareContext) (Datum, bool) {
	return nil, false
}

// IsMin implements the Datum interface.
func (d *DTSVector) IsMin(_ CompareContext) bool {
	return len(d.String()) == 0
}

// IsMax implements the Datum interface.
func (d *DTSVector) IsMax(_ CompareContext) bool {
	return false
}

// Max implements the Datum interface.
func (d *DTSVector) Max(_ CompareContext) (Datum, bool) {
	return nil, false
}

// Min implements the Datum interface.
func (d *DTSVector) Min(_ CompareContext) (Datum, bool) {
	return &DTSVector{}, false
}

// Size implements the Datum interface.
func (d *DTSVector) Size() uintptr {
	return uintptr(len(d.TSVector.String()))
}

// AsDTSVector attempts to retrieve a DTSVector from an Expr, returning a
// DTSVector and a flag signifying whether the assertion was successful. The
// function should be used instead of direct type assertions wherever a
// *DTSVector wrapped by a *DOidWrapper is possible.
func AsDTSVector(e Expr) (*DTSVector, bool) {
	switch t := e.(type) {
	case *DTSVector:
		return t, true
	case *DOidWrapper:
		return AsDTSVector(t.Wrapped)
	}
	return nil, false
}

// MustBeDTSVector attempts to retrieve a DTSVector from an Expr, panicking if the
// assertion fails.
func MustBeDTSVector(e Expr) *DTSVector {
	v, ok := AsDTSVector(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DTSVector, found %T", e))
	}
	return v
}

// NewDTSVector is a helper routine to create a DTSVector initialized from its
// argument.
func NewDTSVector(v tsearch.TSVector) *DTSVector {
	return &DTSVector{TSVector: v}
}

// ParseDTSVector takes a string of TSVector and returns a DTSVector value.
func ParseDTSVector(s string) (Datum, error) {
	v, err := tsearch.ParseTSVector(s)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.Syntax, "could not parse tsvector")
	}
	return NewDTSVector(v), nil
}

// DTuple is the tuple Datum.
type DTuple struct {
	D Datums

	// sorted indicates that the values in D are pre-sorted.
	// This is used to accelerate IN comparisons.
	sorted bool

	// typ is the tuple's type.
	//
	// The Types sub-field can be initially uninitialized, and is then
	// populated upon first invocation of ResolvedTypes(). If
	// initialized it must have the same arity as D.
	//
	// The Labels sub-field can be left nil. If populated, it must have
	// the same arity as D.
	typ *types.T
}

// NewDTuple creates a *DTuple with the provided datums. When creating a new
// DTuple with Datums that are known to be sorted in ascending order, chain
// this call with DTuple.SetSorted.
func NewDTuple(typ *types.T, d ...Datum) *DTuple {
	return &DTuple{D: d, typ: typ}
}

// NewDTupleWithLen creates a *DTuple with the provided length.
func NewDTupleWithLen(typ *types.T, l int) *DTuple {
	return &DTuple{D: make(Datums, l), typ: typ}
}

// MakeDTuple creates a DTuple with the provided datums. See NewDTuple.
func MakeDTuple(typ *types.T, d ...Datum) DTuple {
	return DTuple{D: d, typ: typ}
}

// AsDTuple attempts to retrieve a *DTuple from an Expr, returning a *DTuple and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DTuple wrapped by a
// *DOidWrapper is possible.
func AsDTuple(e Expr) (*DTuple, bool) {
	switch t := e.(type) {
	case *DTuple:
		return t, true
	case *DOidWrapper:
		return AsDTuple(t.Wrapped)
	}
	return nil, false
}

// MustBeDTuple attempts to retrieve a *DTuple from an Expr, panicking if the
// assertion fails.
func MustBeDTuple(e Expr) *DTuple {
	i, ok := AsDTuple(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DTuple, found %T", e))
	}
	return i
}

// maybePopulateType populates the tuple's type if it hasn't yet been
// populated.
func (d *DTuple) maybePopulateType() {
	if d.typ == nil {
		contents := make([]*types.T, len(d.D))
		for i, v := range d.D {
			contents[i] = v.ResolvedType()
		}
		d.typ = types.MakeTuple(contents)
	}
}

// ResolvedType implements the TypedExpr interface.
func (d *DTuple) ResolvedType() *types.T {
	d.maybePopulateType()
	return d.typ
}

// Compare implements the Datum interface.
func (d *DTuple) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DTuple) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DTuple)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	n := len(d.D)
	if n > len(v.D) {
		n = len(v.D)
	}
	for i := 0; i < n; i++ {
		c, err := d.D[i].CompareError(ctx, v.D[i])
		if err != nil {
			return 0, errors.WithDetailf(err, "type mismatch at record column %d", redact.SafeInt(i+1))
		}
		if c != 0 {
			return c, nil
		}
	}
	if len(d.D) < len(v.D) {
		return -1, nil
	}
	if len(d.D) > len(v.D) {
		return 1, nil
	}
	return 0, nil
}

// Prev implements the Datum interface.
func (d *DTuple) Prev(ctx CompareContext) (Datum, bool) {
	// Note: (a:decimal, b:int, c:int) has a prev value; that's (a, b,
	// c-1). With an exception if c is MinInt64, in which case the prev
	// value is (a, b-1, max(_ *EvalContext)). However, (a:int, b:decimal) does not
	// have a prev value, because decimal doesn't have one.
	//
	// In general, a tuple has a prev value if and only if it ends with
	// zero or more values that are a minimum and a maximum value of the
	// same type exists, and the first element before that has a prev
	// value.
	res := NewDTupleWithLen(d.typ, len(d.D))
	copy(res.D, d.D)
	for i := len(res.D) - 1; i >= 0; i-- {
		if !res.D[i].IsMin(ctx) {
			prevVal, ok := res.D[i].Prev(ctx)
			if !ok {
				return nil, false
			}
			res.D[i] = prevVal
			break
		}
		maxVal, ok := res.D[i].Max(ctx)
		if !ok {
			return nil, false
		}
		res.D[i] = maxVal
	}
	return res, true
}

// Next implements the Datum interface.
func (d *DTuple) Next(ctx CompareContext) (Datum, bool) {
	// Note: (a:decimal, b:int, c:int) has a next value; that's (a, b,
	// c+1). With an exception if c is MaxInt64, in which case the next
	// value is (a, b+1, min(_ *EvalContext)). However, (a:int, b:decimal) does not
	// have a next value, because decimal doesn't have one.
	//
	// In general, a tuple has a next value if and only if it ends with
	// zero or more values that are a maximum and a minimum value of the
	// same type exists, and the first element before that has a next
	// value.
	res := NewDTupleWithLen(d.typ, len(d.D))
	copy(res.D, d.D)
	for i := len(res.D) - 1; i >= 0; i-- {
		if !res.D[i].IsMax(ctx) {
			nextVal, ok := res.D[i].Next(ctx)
			if !ok {
				return nil, false
			}
			res.D[i] = nextVal
			break
		}
		// TODO(#12022): temporary workaround; see the interface comment.
		res.D[i] = DNull
	}
	return res, true
}

// Max implements the Datum interface.
func (d *DTuple) Max(ctx CompareContext) (Datum, bool) {
	res := NewDTupleWithLen(d.typ, len(d.D))
	for i, v := range d.D {
		m, ok := v.Max(ctx)
		if !ok {
			return nil, false
		}
		res.D[i] = m
	}
	return res, true
}

// Min implements the Datum interface.
func (d *DTuple) Min(ctx CompareContext) (Datum, bool) {
	res := NewDTupleWithLen(d.typ, len(d.D))
	for i, v := range d.D {
		m, ok := v.Min(ctx)
		if !ok {
			return nil, false
		}
		res.D[i] = m
	}
	return res, true
}

// IsMax implements the Datum interface.
func (d *DTuple) IsMax(ctx CompareContext) bool {
	for _, v := range d.D {
		if !v.IsMax(ctx) {
			return false
		}
	}
	return true
}

// IsMin implements the Datum interface.
func (d *DTuple) IsMin(ctx CompareContext) bool {
	for _, v := range d.D {
		if !v.IsMin(ctx) {
			return false
		}
	}
	return true
}

// AmbiguousFormat implements the Datum interface.
func (*DTuple) AmbiguousFormat() bool { return false }

// Format implements the NodeFormatter interface.
func (d *DTuple) Format(ctx *FmtCtx) {
	if ctx.HasFlags(fmtPgwireFormat) {
		d.pgwireFormat(ctx)
		return
	}

	typ := d.ResolvedType()
	tupleContents := typ.TupleContents()
	showLabels := len(typ.TupleLabels()) > 0
	if showLabels {
		ctx.WriteByte('(')
	}
	ctx.WriteByte('(')
	comma := ""
	parsable := ctx.HasFlags(FmtParsable)
	for i, v := range d.D {
		ctx.WriteString(comma)
		ctx.FormatNode(v)
		if parsable && (v == DNull) && len(tupleContents) > i {
			// If Tuple has types.Unknown for this slot, then we can't determine
			// the column type to write this annotation. Somebody else will provide
			// an error message in this case, if necessary, so just skip the
			// annotation and continue.
			if tupleContents[i].Family() != types.UnknownFamily {
				nullType := tupleContents[i]
				if ctx.HasFlags(fmtDisambiguateDatumTypes) {
					ctx.WriteString(":::")
					ctx.FormatTypeReference(nullType)
				} else {
					ctx.WriteString("::")
					ctx.WriteString(nullType.SQLString())
				}
			}
		}
		comma = ", "
	}
	if len(d.D) == 1 {
		// Ensure the pretty-printed 1-value tuple is not ambiguous with
		// the equivalent value enclosed in grouping parentheses.
		ctx.WriteByte(',')
	}
	ctx.WriteByte(')')
	if showLabels {
		ctx.WriteString(" AS ")
		comma := ""
		for i := range typ.TupleLabels() {
			ctx.WriteString(comma)
			ctx.FormatNode((*Name)(&typ.TupleLabels()[i]))
			comma = ", "
		}
		ctx.WriteByte(')')
	}
}

// Sorted returns true if the tuple is known to be sorted (and contains no
// NULLs).
func (d *DTuple) Sorted() bool {
	return d.sorted
}

// SetSorted sets the sorted flag on the DTuple. This should be used when a
// DTuple is known to be sorted based on the datums added to it.
func (d *DTuple) SetSorted() *DTuple {
	if d.ContainsNull() {
		// A DTuple that contains a NULL (see ContainsNull) cannot be marked as sorted.
		return d
	}
	d.sorted = true
	return d
}

// AssertSorted asserts that the DTuple is sorted.
func (d *DTuple) AssertSorted() {
	if !d.sorted {
		panic(errors.AssertionFailedf("expected sorted tuple, found %#v", d))
	}
}

// SearchSorted searches the tuple for the target Datum, returning an int with
// the same contract as sort.Search and a boolean flag signifying whether the datum
// was found. It assumes that the DTuple is sorted and panics if it is not.
//
// The target Datum cannot be NULL or a DTuple that contains NULLs (we cannot
// binary search in this case; for example `(1, NULL) IN ((1, 2), ..)` needs to
// be
func (d *DTuple) SearchSorted(ctx CompareContext, target Datum) (int, bool) {
	d.AssertSorted()
	if target == DNull {
		panic(errors.AssertionFailedf("NULL target (d: %s)", d))
	}
	if t, ok := target.(*DTuple); ok && t.ContainsNull() {
		panic(errors.AssertionFailedf("target containing NULLs: %#v (d: %s)", target, d))
	}
	i := sort.Search(len(d.D), func(i int) bool {
		return d.D[i].Compare(ctx, target) >= 0
	})
	found := i < len(d.D) && d.D[i].Compare(ctx, target) == 0
	return i, found
}

// Normalize sorts and uniques the datum tuple.
func (d *DTuple) Normalize(ctx CompareContext) {
	d.sort(ctx)
	d.makeUnique(ctx)
}

func (d *DTuple) sort(ctx CompareContext) {
	if !d.sorted {
		lessFn := func(i, j int) bool {
			return d.D[i].Compare(ctx, d.D[j]) < 0
		}

		// It is possible for the tuple to be sorted even though the sorted flag
		// is not true. So before we perform the sort we check that it is not
		// already sorted.
		if !sort.SliceIsSorted(d.D, lessFn) {
			sort.Slice(d.D, lessFn)
		}
		d.SetSorted()
	}
}

func (d *DTuple) makeUnique(ctx CompareContext) {
	n := 0
	for i := 0; i < len(d.D); i++ {
		if n == 0 || d.D[n-1].Compare(ctx, d.D[i]) < 0 {
			d.D[n] = d.D[i]
			n++
		}
	}
	d.D = d.D[:n]
}

// Size implements the Datum interface.
func (d *DTuple) Size() uintptr {
	sz := unsafe.Sizeof(*d)
	for _, e := range d.D {
		dsz := e.Size()
		sz += dsz
	}
	return sz
}

// ContainsNull returns true if the tuple contains NULL, possibly nested inside
// other tuples. For example, all the following tuples contain NULL:
//
//	(1, 2, NULL)
//	((1, 1), (2, NULL))
//	(((1, 1), (2, 2)), ((3, 3), (4, NULL)))
func (d *DTuple) ContainsNull() bool {
	for _, r := range d.D {
		if r == DNull {
			return true
		}
		if t, ok := r.(*DTuple); ok {
			if t.ContainsNull() {
				return true
			}
		}
	}
	return false
}

type dNull struct{}

// ResolvedType implements the TypedExpr interface.
func (dNull) ResolvedType() *types.T {
	return types.Unknown
}

// Compare implements the Datum interface.
func (d dNull) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d dNull) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		return 0, nil
	}
	return -1, nil
}

// Prev implements the Datum interface.
func (d dNull) Prev(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d dNull) Next(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (dNull) IsMax(ctx CompareContext) bool {
	return true
}

// IsMin implements the Datum interface.
func (dNull) IsMin(ctx CompareContext) bool {
	return true
}

// Max implements the Datum interface.
func (dNull) Max(ctx CompareContext) (Datum, bool) {
	return DNull, true
}

// Min implements the Datum interface.
func (dNull) Min(ctx CompareContext) (Datum, bool) {
	return DNull, true
}

// AmbiguousFormat implements the Datum interface.
func (dNull) AmbiguousFormat() bool { return false }

// Format implements the NodeFormatter interface.
func (dNull) Format(ctx *FmtCtx) {
	if ctx.HasFlags(fmtPgwireFormat) {
		// NULL sub-expressions in pgwire text values are represented with
		// the empty string.
		return
	}
	ctx.WriteString("NULL")
}

// Size implements the Datum interface.
func (d dNull) Size() uintptr {
	return unsafe.Sizeof(d)
}

// DArray is the array Datum. Any Datum inserted into a DArray are treated as
// text during serialization.
type DArray struct {
	ParamTyp *types.T
	Array    Datums
	// HasNulls is set to true if any of the datums within the array are null.
	// This is used in the binary array serialization format.
	HasNulls bool
	// HasNonNulls is set to true if any of the datums within the are non-null.
	// This is used in expression serialization (FmtParsable).
	HasNonNulls bool

	// customOid, if non-0, is the oid of this array datum.
	customOid oid.Oid
}

// NewDArray returns a DArray containing elements of the specified type.
func NewDArray(paramTyp *types.T) *DArray {
	return &DArray{ParamTyp: paramTyp}
}

// AsDArray attempts to retrieve a *DArray from an Expr, returning a *DArray and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DArray wrapped by a
// *DOidWrapper is possible.
func AsDArray(e Expr) (*DArray, bool) {
	switch t := e.(type) {
	case *DArray:
		return t, true
	case *DOidWrapper:
		return AsDArray(t.Wrapped)
	}
	return nil, false
}

// MustBeDArray attempts to retrieve a *DArray from an Expr, panicking if the
// assertion fails.
func MustBeDArray(e Expr) *DArray {
	i, ok := AsDArray(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DArray, found %T", e))
	}
	return i
}

// MaybeSetCustomOid checks whether t has a special oid that we want to set into
// d. Must be kept in sync with DArray.ResolvedType. Returns an error if t is
// not an array type.
func (d *DArray) MaybeSetCustomOid(t *types.T) error {
	if t.Family() != types.ArrayFamily {
		return errors.AssertionFailedf("expected array type, got %s", t.SQLString())
	}
	switch t.Oid() {
	case oid.T_int2vector:
		d.customOid = oid.T_int2vector
	case oid.T_oidvector:
		d.customOid = oid.T_oidvector
	}
	return nil
}

// ResolvedType implements the TypedExpr interface. Must be kept in sync with
// DArray.MaybeSetCustomOid.
func (d *DArray) ResolvedType() *types.T {
	switch d.customOid {
	case oid.T_int2vector:
		return types.Int2Vector
	case oid.T_oidvector:
		return types.OidVector
	}
	return types.MakeArray(d.ParamTyp)
}

// IsComposite implements the CompositeDatum interface.
func (d *DArray) IsComposite() bool {
	for _, elem := range d.Array {
		if cdatum, ok := elem.(CompositeDatum); ok && cdatum.IsComposite() {
			return true
		}
	}
	return false
}

// FirstIndex returns the first index of the array. 1 for normal SQL arrays,
// which are 1-indexed, and 0 for the special Postgers vector types which are
// 0-indexed.
func (d *DArray) FirstIndex() int {
	switch d.customOid {
	case oid.T_int2vector, oid.T_oidvector:
		return 0
	}
	return 1
}

// Compare implements the Datum interface.
func (d *DArray) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DArray) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DArray)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	n := d.Len()
	if n > v.Len() {
		n = v.Len()
	}
	for i := 0; i < n; i++ {
		c, err := d.Array[i].CompareError(ctx, v.Array[i])
		if err != nil {
			return 0, err
		}
		if c != 0 {
			return c, nil
		}
	}
	if d.Len() < v.Len() {
		return -1, nil
	}
	if d.Len() > v.Len() {
		return 1, nil
	}
	return 0, nil
}

// Prev implements the Datum interface.
func (d *DArray) Prev(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DArray) Next(ctx CompareContext) (Datum, bool) {
	a := DArray{ParamTyp: d.ParamTyp, Array: make(Datums, d.Len()+1)}
	copy(a.Array, d.Array)
	a.Array[len(a.Array)-1] = DNull
	return &a, true
}

// Max implements the Datum interface.
func (d *DArray) Max(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Min implements the Datum interface.
func (d *DArray) Min(ctx CompareContext) (Datum, bool) {
	return &DArray{ParamTyp: d.ParamTyp}, true
}

// IsMax implements the Datum interface.
func (d *DArray) IsMax(ctx CompareContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DArray) IsMin(ctx CompareContext) bool {
	return d.Len() == 0
}

// AmbiguousFormat implements the Datum interface.
func (d *DArray) AmbiguousFormat() bool {
	// The type of the array is ambiguous if it is empty or all-null; when
	// serializing we need to annotate it with the type.
	if d.ParamTyp.Family() == types.UnknownFamily {
		// If the array's type is unknown, marking it as ambiguous would cause the
		// expression formatter to try to annotate it with UNKNOWN[], which is not
		// a valid type. So an array of unknown type is (paradoxically) unambiguous.
		return false
	}
	return !d.HasNonNulls
}

// Format implements the NodeFormatter interface.
func (d *DArray) Format(ctx *FmtCtx) {
	if ctx.flags.HasAnyFlags(fmtPgwireFormat | FmtPGCatalog) {
		d.pgwireFormat(ctx)
		return
	}

	// If we want to export arrays, we need to ensure that
	// the datums within the arrays are formatted with enclosing quotes etc.
	if ctx.HasFlags(FmtExport) {
		oldFlags := ctx.flags
		ctx.flags = oldFlags & ^FmtExport | FmtParsable
		defer func() { ctx.flags = oldFlags }()
	}

	ctx.WriteString("ARRAY[")
	comma := ""
	for _, v := range d.Array {
		ctx.WriteString(comma)
		ctx.FormatNode(v)
		comma = ","
	}
	ctx.WriteByte(']')
}

const maxArrayLength = math.MaxInt32

var errArrayTooLongError = errors.New("ARRAYs can be at most 2^31-1 elements long")

// Validate checks that the given array is valid,
// for example, that it's not too big.
func (d *DArray) Validate() error {
	if d.Len() > maxArrayLength {
		return errors.WithStack(errArrayTooLongError)
	}
	return nil
}

// Len returns the length of the Datum array.
func (d *DArray) Len() int {
	return len(d.Array)
}

// Size implements the Datum interface.
func (d *DArray) Size() uintptr {
	sz := unsafe.Sizeof(*d)
	for _, e := range d.Array {
		dsz := e.Size()
		sz += dsz
	}
	return sz
}

var errNonHomogeneousArray = pgerror.New(pgcode.ArraySubscript, "multidimensional arrays must have array expressions with matching dimensions")

// Append appends a Datum to the array, whose parameterized type must be
// consistent with the type of the Datum.
func (d *DArray) Append(v Datum) error {
	// v.ResolvedType() must be the left-hand side because EquivalentOrNull
	// only allows null tuple elements on the left-hand side.
	if !v.ResolvedType().EquivalentOrNull(d.ParamTyp, true /* allowNullTupleEquivalence */) {
		return errors.AssertionFailedf("cannot append %s to array containing %s", v.ResolvedType(), d.ParamTyp)
	}
	if d.Len() >= maxArrayLength {
		return errors.WithStack(errArrayTooLongError)
	}
	if d.ParamTyp.Family() == types.ArrayFamily {
		if v == DNull {
			return errNonHomogeneousArray
		}
		if d.Len() > 0 {
			prevItem := d.Array[d.Len()-1]
			if prevItem == DNull {
				return errNonHomogeneousArray
			}
			expectedLen := MustBeDArray(prevItem).Len()
			if MustBeDArray(v).Len() != expectedLen {
				return errNonHomogeneousArray
			}
		}
	}
	if v == DNull {
		d.HasNulls = true
	} else {
		d.HasNonNulls = true
	}
	d.Array = append(d.Array, v)
	return d.Validate()
}

// DVoid represents a void type.
type DVoid struct{}

// DVoidDatum is an instance of the DVoid datum.
var DVoidDatum = &DVoid{}

// ResolvedType implements the TypedExpr interface.
func (*DVoid) ResolvedType() *types.T {
	return types.Void
}

// Compare implements the Datum interface.
func (d *DVoid) Compare(ctx CompareContext, other Datum) int {
	ret, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return ret
}

// CompareError implements the Datum interface.
func (d *DVoid) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}

	_, ok := ctx.UnwrapDatum(other).(*DVoid)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	return 0, nil
}

// Prev implements the Datum interface.
func (d *DVoid) Prev(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Next implements the Datum interface.
func (d *DVoid) Next(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// IsMax implements the Datum interface.
func (d *DVoid) IsMax(ctx CompareContext) bool {
	return false
}

// IsMin implements the Datum interface.
func (d *DVoid) IsMin(ctx CompareContext) bool {
	return false
}

// Max implements the Datum interface.
func (d *DVoid) Max(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// Min implements the Datum interface.
func (d *DVoid) Min(ctx CompareContext) (Datum, bool) {
	return nil, false
}

// AmbiguousFormat implements the Datum interface.
func (*DVoid) AmbiguousFormat() bool { return true }

// Format implements the NodeFormatter interface.
func (d *DVoid) Format(ctx *FmtCtx) {
	buf, f := &ctx.Buffer, ctx.flags
	if !f.HasFlags(fmtRawStrings) {
		// void is an empty string.
		lexbase.EncodeSQLStringWithFlags(buf, "", f.EncodeFlags())
	}
}

// Size implements the Datum interface.
func (d *DVoid) Size() uintptr {
	return unsafe.Sizeof(*d)
}

// DEnum represents an ENUM value.
type DEnum struct {
	// EnumType is the hydrated type of this enum.
	EnumTyp *types.T
	// PhysicalRep is a slice containing the encodable and ordered physical
	// representation of this datum. It is used for comparisons and encoding.
	PhysicalRep []byte
	// LogicalRep is a string containing the user visible value of the enum.
	LogicalRep string
}

// Size implements the Datum interface.
func (d *DEnum) Size() uintptr {
	// When creating DEnums, we store pointers back into the type enum
	// metadata, so enums themselves don't pay for the memory of their
	// physical and logical representations.
	return unsafe.Sizeof(d.EnumTyp) +
		unsafe.Sizeof(d.PhysicalRep) +
		unsafe.Sizeof(d.LogicalRep)
}

// GetEnumComponentsFromPhysicalRep returns the physical and logical components
// for an enum of the requested type. It returns an error if it cannot find a
// matching physical representation.
func GetEnumComponentsFromPhysicalRep(typ *types.T, rep []byte) ([]byte, string, error) {
	idx, err := typ.EnumGetIdxOfPhysical(rep)
	if err != nil {
		return nil, "", err
	}
	meta := typ.TypeMeta.EnumData
	// Take a pointer into the enum metadata rather than holding on
	// to a pointer to the input bytes.
	return meta.PhysicalRepresentations[idx], meta.LogicalRepresentations[idx], nil
}

// GetEnumComponentsFromLogicalRep returns the physical and logical components
// for an enum of the requested type. It returns an error if it cannot find a
// matching logical representation.
func GetEnumComponentsFromLogicalRep(typ *types.T, rep string) ([]byte, string, error) {
	idx, err := typ.EnumGetIdxOfLogical(rep)
	if err != nil {
		return nil, "", err
	}
	meta := typ.TypeMeta.EnumData
	return meta.PhysicalRepresentations[idx], meta.LogicalRepresentations[idx], nil
}

// NewDEnum initializes a new DEnum from its argument.
func NewDEnum(e DEnum) *DEnum {
	return &e
}

// MakeDEnumFromPhysicalRepresentation creates a DEnum of the input type
// and the input physical representation.
func MakeDEnumFromPhysicalRepresentation(typ *types.T, rep []byte) (DEnum, error) {
	// Return a nice error if the input requested type is types.AnyEnum.
	if typ.Oid() == oid.T_anyenum {
		return DEnum{}, errors.New("cannot create enum of unspecified type")
	}
	phys, log, err := GetEnumComponentsFromPhysicalRep(typ, rep)
	if err != nil {
		return DEnum{}, err
	}
	return DEnum{
		EnumTyp:     typ,
		PhysicalRep: phys,
		LogicalRep:  log,
	}, nil
}

// MakeDEnumFromLogicalRepresentation creates a DEnum of the input type
// and input logical representation. It returns an error if the input
// logical representation is invalid.
func MakeDEnumFromLogicalRepresentation(typ *types.T, rep string) (DEnum, error) {
	// Return a nice error if the input requested type is types.AnyEnum.
	if typ.Oid() == oid.T_anyenum {
		return DEnum{}, errors.New("cannot create enum of unspecified type")
	}
	// Take a pointer into the enum metadata rather than holding on
	// to a pointer to the input string.
	idx, err := typ.EnumGetIdxOfLogical(rep)
	if err != nil {
		return DEnum{}, err
	}
	return DEnum{
		EnumTyp:     typ,
		PhysicalRep: typ.TypeMeta.EnumData.PhysicalRepresentations[idx],
		LogicalRep:  typ.TypeMeta.EnumData.LogicalRepresentations[idx],
	}, nil
}

// MakeAllDEnumsInType generates a slice of all values in an enum.
func MakeAllDEnumsInType(typ *types.T) []Datum {
	result := make([]Datum, len(typ.TypeMeta.EnumData.LogicalRepresentations))
	for i := 0; i < len(result); i++ {
		result[i] = &DEnum{
			EnumTyp:     typ,
			PhysicalRep: typ.TypeMeta.EnumData.PhysicalRepresentations[i],
			LogicalRep:  typ.TypeMeta.EnumData.LogicalRepresentations[i],
		}
	}
	return result
}

// Format implements the NodeFormatter interface.
func (d *DEnum) Format(ctx *FmtCtx) {
	if ctx.HasFlags(fmtStaticallyFormatUserDefinedTypes) {
		s := DBytes(d.PhysicalRep)
		// We use the fmtFormatByteLiterals flag here so that the bytes
		// get formatted as byte literals. Consider an enum of type t with physical
		// representation \x80. If we don't format this as a bytes literal then
		// it gets emitted as '\x80':::t. '\x80' is scanned as a string, and we try
		// to find a logical representation matching '\x80', which won't exist.
		// Instead, we want to emit b'\x80'::: so that '\x80' is scanned as bytes,
		// triggering the logic to cast the bytes \x80 to t.
		ctx.WithFlags(ctx.flags|fmtFormatByteLiterals, func() {
			s.Format(ctx)
		})
	} else if ctx.HasFlags(FmtPgwireText) {
		ctx.WriteString(d.LogicalRep)
	} else {
		s := DString(d.LogicalRep)
		s.Format(ctx)
	}
}

func (d *DEnum) String() string {
	return AsString(d)
}

// ResolvedType implements the Datum interface.
func (d *DEnum) ResolvedType() *types.T {
	return d.EnumTyp
}

// Compare implements the Datum interface.
func (d *DEnum) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DEnum) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		return 1, nil
	}
	v, ok := ctx.UnwrapDatum(other).(*DEnum)
	if !ok {
		return 0, makeUnsupportedComparisonMessage(d, other)
	}
	res := bytes.Compare(d.PhysicalRep, v.PhysicalRep)
	return res, nil
}

// Prev implements the Datum interface.
func (d *DEnum) Prev(ctx CompareContext) (Datum, bool) {
	idx, err := d.EnumTyp.EnumGetIdxOfPhysical(d.PhysicalRep)
	if err != nil {
		panic(err)
	}
	if idx == 0 {
		return nil, false
	}
	enumData := d.EnumTyp.TypeMeta.EnumData
	return &DEnum{
		EnumTyp:     d.EnumTyp,
		PhysicalRep: enumData.PhysicalRepresentations[idx-1],
		LogicalRep:  enumData.LogicalRepresentations[idx-1],
	}, true
}

// Next implements the Datum interface.
func (d *DEnum) Next(ctx CompareContext) (Datum, bool) {
	idx, err := d.EnumTyp.EnumGetIdxOfPhysical(d.PhysicalRep)
	if err != nil {
		panic(err)
	}
	enumData := d.EnumTyp.TypeMeta.EnumData
	if idx == len(enumData.PhysicalRepresentations)-1 {
		return nil, false
	}
	return &DEnum{
		EnumTyp:     d.EnumTyp,
		PhysicalRep: enumData.PhysicalRepresentations[idx+1],
		LogicalRep:  enumData.LogicalRepresentations[idx+1],
	}, true
}

// Max implements the Datum interface.
func (d *DEnum) Max(ctx CompareContext) (Datum, bool) {
	enumData := d.EnumTyp.TypeMeta.EnumData
	if len(enumData.PhysicalRepresentations) == 0 {
		return nil, false
	}
	idx := len(enumData.PhysicalRepresentations) - 1
	return &DEnum{
		EnumTyp:     d.EnumTyp,
		PhysicalRep: enumData.PhysicalRepresentations[idx],
		LogicalRep:  enumData.LogicalRepresentations[idx],
	}, true
}

// Min implements the Datum interface.
func (d *DEnum) Min(ctx CompareContext) (Datum, bool) {
	enumData := d.EnumTyp.TypeMeta.EnumData
	if len(enumData.PhysicalRepresentations) == 0 {
		return nil, false
	}
	return &DEnum{
		EnumTyp:     d.EnumTyp,
		PhysicalRep: enumData.PhysicalRepresentations[0],
		LogicalRep:  enumData.LogicalRepresentations[0],
	}, true
}

// IsMax implements the Datum interface.
func (d *DEnum) IsMax(ctx CompareContext) bool {
	physReps := d.EnumTyp.TypeMeta.EnumData.PhysicalRepresentations
	idx, err := d.EnumTyp.EnumGetIdxOfPhysical(d.PhysicalRep)
	if err != nil {
		panic(err)
	}
	return idx == len(physReps)-1
}

// IsMin implements the Datum interface.
func (d *DEnum) IsMin(ctx CompareContext) bool {
	idx, err := d.EnumTyp.EnumGetIdxOfPhysical(d.PhysicalRep)
	if err != nil {
		panic(err)
	}
	return idx == 0
}

// AmbiguousFormat implements the Datum interface.
func (d *DEnum) AmbiguousFormat() bool {
	return true
}

// MaxWriteable returns the largest member of the enum that is writeable.
func (d *DEnum) MaxWriteable() (Datum, bool) {
	enumData := d.EnumTyp.TypeMeta.EnumData
	if len(enumData.PhysicalRepresentations) == 0 {
		return nil, false
	}
	for i := len(enumData.PhysicalRepresentations) - 1; i >= 0; i-- {
		if !enumData.IsMemberReadOnly[i] {
			return &DEnum{
				EnumTyp:     d.EnumTyp,
				PhysicalRep: enumData.PhysicalRepresentations[i],
				LogicalRep:  enumData.LogicalRepresentations[i],
			}, true
		}
	}
	return nil, false
}

// MinWriteable returns the smallest member of the enum that is writeable.
func (d *DEnum) MinWriteable() (Datum, bool) {
	enumData := d.EnumTyp.TypeMeta.EnumData
	if len(enumData.PhysicalRepresentations) == 0 {
		return nil, false
	}
	for i := 0; i < len(enumData.PhysicalRepresentations); i++ {
		if !enumData.IsMemberReadOnly[i] {
			return &DEnum{
				EnumTyp:     d.EnumTyp,
				PhysicalRep: enumData.PhysicalRepresentations[i],
				LogicalRep:  enumData.LogicalRepresentations[i],
			}, true
		}
	}
	return nil, false
}

// DOid is the Postgres OID datum. It can represent either an OID type or any
// of the reg* types, such as regproc or regclass. An OID must only be
// 32 bits, since this width encoding is enforced in the pgwire protocol.
// OIDs are not guaranteed to be globally unique.
type DOid struct {
	// A DOid embeds a oid.Oid, the underlying integer OID for this OID datum.
	Oid oid.Oid
	// semanticType indicates the particular variety of OID this datum is, whether raw
	// Oid or a reg* type.
	semanticType *types.T
	// name is set to the resolved name of this OID, if available.
	name string
}

// IntToOid is a helper that turns a DInt into a *DOid and checks that the value
// is in range.
func IntToOid(i DInt) (*DOid, error) {
	if intIsOutOfOIDRange(i) {
		return nil, pgerror.Newf(
			pgcode.NumericValueOutOfRange, "OID out of range: %d", i,
		)
	}
	return NewDOid(oid.Oid(i)), nil
}

func intIsOutOfOIDRange(i DInt) bool {
	return i > math.MaxUint32 || i < math.MinInt32
}

// MakeDOid is a helper routine to create a DOid initialized from a DInt.
func MakeDOid(d oid.Oid, semanticType *types.T) DOid {
	return DOid{Oid: d, semanticType: semanticType, name: ""}
}

// NewDOidWithType constructs a DOid with the given type and no name.
func NewDOidWithType(d oid.Oid, semanticType *types.T) *DOid {
	oid := DOid{Oid: d, semanticType: semanticType}
	return &oid
}

// NewDOidWithTypeAndName constructs a DOid with the given type and name.
func NewDOidWithTypeAndName(d oid.Oid, semanticType *types.T, name string) *DOid {
	oid := DOid{Oid: d, semanticType: semanticType, name: name}
	return &oid
}

// NewDOid is a helper routine to create a *DOid initialized from a DInt.
func NewDOid(d oid.Oid) *DOid {
	// TODO(yuzefovich): audit the callers of NewDOid to see whether any want to
	// create a DOid with a semantic type different from types.Oid.
	oid := MakeDOid(d, types.Oid)
	return &oid
}

// AsDOid attempts to retrieve a DOid from an Expr, returning a DOid and
// a flag signifying whether the assertion was successful. The function should
// be used instead of direct type assertions wherever a *DOid wrapped by a
// *DOidWrapper is possible.
func AsDOid(e Expr) (*DOid, bool) {
	switch t := e.(type) {
	case *DOid:
		return t, true
	case *DOidWrapper:
		return AsDOid(t.Wrapped)
	}
	return NewDOid(0), false
}

// MustBeDOid attempts to retrieve a DOid from an Expr, panicking if the
// assertion fails.
func MustBeDOid(e Expr) *DOid {
	i, ok := AsDOid(e)
	if !ok {
		panic(errors.AssertionFailedf("expected *DOid, found %T", e))
	}
	return i
}

// NewDOidWithName is a helper routine to create a *DOid initialized from a DInt
// and a string.
func NewDOidWithName(d oid.Oid, typ *types.T, name string) *DOid {
	return &DOid{
		Oid:          d,
		semanticType: typ,
		name:         name,
	}
}

// AsRegProc changes the input DOid into a regproc with the given name and
// returns it.
func (d *DOid) AsRegProc(name string) *DOid {
	d.name = name
	d.semanticType = types.RegProc
	return d
}

// AmbiguousFormat implements the Datum interface.
func (*DOid) AmbiguousFormat() bool { return true }

// Compare implements the Datum interface.
func (d *DOid) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DOid) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	var v oid.Oid
	switch t := ctx.UnwrapDatum(other).(type) {
	case *DOid:
		v = t.Oid
	case *DInt:
		// OIDs are always unsigned 32-bit integers. Some languages, like Java,
		// compare OIDs to signed 32-bit integers, so we implement the comparison
		// by converting to a uint32 first. This matches Postgres behavior.
		o, err := IntToOid(*t)
		if err != nil {
			return 0, err
		}
		v = o.Oid
	default:
		return 0, makeUnsupportedComparisonMessage(d, other)
	}

	if d.Oid < v {
		return -1, nil
	}
	if d.Oid > v {
		return 1, nil
	}
	return 0, nil
}

// Format implements the Datum interface.
func (d *DOid) Format(ctx *FmtCtx) {
	if d.semanticType.Oid() == oid.T_oid || d.name == "" {
		ctx.Write(strconv.AppendUint(ctx.scratch[:0], uint64(d.Oid), 10))
	} else if ctx.HasFlags(fmtDisambiguateDatumTypes) {
		ctx.WriteString("crdb_internal.create_")
		ctx.WriteString(d.semanticType.SQLStandardName())
		ctx.WriteByte('(')
		ctx.Write(strconv.AppendUint(ctx.scratch[:0], uint64(d.Oid), 10))
		ctx.WriteByte(',')
		lexbase.EncodeSQLStringWithFlags(&ctx.Buffer, d.name, lexbase.EncNoFlags)
		ctx.WriteByte(')')
	} else {
		// This is used to print the name of pseudo-procedures in e.g.
		// pg_catalog.pg_type.typinput
		lexbase.EncodeSQLStringWithFlags(&ctx.Buffer, d.name, lexbase.EncBareStrings)
	}
}

// IsMax implements the Datum interface.
func (d *DOid) IsMax(ctx CompareContext) bool {
	return d.Oid == math.MaxUint32
}

// IsMin implements the Datum interface.
func (d *DOid) IsMin(ctx CompareContext) bool {
	return d.Oid == 0
}

// Next implements the Datum interface.
func (d *DOid) Next(ctx CompareContext) (Datum, bool) {
	next := d.Oid + 1
	return &DOid{next, d.semanticType, ""}, true
}

// Prev implements the Datum interface.
func (d *DOid) Prev(ctx CompareContext) (Datum, bool) {
	prev := d.Oid - 1
	return &DOid{prev, d.semanticType, ""}, true
}

// ResolvedType implements the Datum interface.
func (d *DOid) ResolvedType() *types.T {
	return d.semanticType
}

// Size implements the Datum interface.
func (d *DOid) Size() uintptr { return unsafe.Sizeof(*d) }

// Max implements the Datum interface.
func (d *DOid) Max(ctx CompareContext) (Datum, bool) {
	return &DOid{math.MaxUint32, d.semanticType, ""}, true
}

// Min implements the Datum interface.
func (d *DOid) Min(ctx CompareContext) (Datum, bool) {
	return &DOid{0, d.semanticType, ""}, true
}

// DOidWrapper is a Datum implementation which is a wrapper around a Datum, allowing
// custom Oid values to be attached to the Datum and its types.T.
// The reason the Datum type was introduced was to permit the introduction of Datum
// types with new Object IDs while maintaining identical behavior to current Datum
// types. Specifically, it obviates the need to define a new tree.Datum type for
// each possible Oid value.
//
// Instead, DOidWrapper allows a standard Datum to be wrapped with a new Oid.
// This approach provides two major advantages:
//   - performance of the existing Datum types are not affected because they
//     do not need to have custom oid.Oids added to their structure.
//   - the introduction of new Datum aliases is straightforward and does not require
//     additions to typing rules or type-dependent evaluation behavior.
//
// Types that currently benefit from DOidWrapper are:
// - DName => DOidWrapper(*DString, oid.T_name)
type DOidWrapper struct {
	Wrapped Datum
	Oid     oid.Oid
}

// ZeroOidValue represents the 0 oid value as '-', which matches the Postgres
// representation.
const ZeroOidValue = "-"

// wrapWithOid wraps a Datum with a custom Oid.
func wrapWithOid(d Datum, oid oid.Oid) Datum {
	switch v := d.(type) {
	case nil:
		return nil
	case *DInt:
	case *DString:
	case *DArray:
	case dNull, *DOidWrapper:
		panic(errors.AssertionFailedf("cannot wrap %T with an Oid", v))
	default:
		// Currently only *DInt, *DString, *DArray are hooked up to work with
		// *DOidWrapper. To support another base Datum type, replace all type
		// assertions to that type with calls to functions like AsDInt and
		// MustBeDInt.
		panic(errors.AssertionFailedf("unsupported Datum type passed to wrapWithOid: %T", d))
	}
	return &DOidWrapper{
		Wrapped: d,
		Oid:     oid,
	}
}

// WrapAsZeroOid wraps ZeroOidValue with a custom Oid.
func WrapAsZeroOid(t *types.T) Datum {
	tmpOid := NewDOid(0)
	tmpOid.semanticType = t
	if t.Oid() != oid.T_oid {
		tmpOid.name = ZeroOidValue
	}
	return tmpOid
}

// UnwrapDOidWrapper exposes the wrapped datum from a *DOidWrapper.
func UnwrapDOidWrapper(d Datum) Datum {
	if w, ok := d.(*DOidWrapper); ok {
		return w.Wrapped
	}
	return d
}

// ResolvedType implements the TypedExpr interface.
func (d *DOidWrapper) ResolvedType() *types.T {
	return types.OidToType[d.Oid]
}

// Compare implements the Datum interface.
func (d *DOidWrapper) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *DOidWrapper) CompareError(ctx CompareContext, other Datum) (int, error) {
	if other == DNull {
		// NULL is less than any non-NULL value.
		return 1, nil
	}
	if v, ok := other.(*DOidWrapper); ok {
		return d.Wrapped.CompareError(ctx, v.Wrapped)
	}
	return d.Wrapped.CompareError(ctx, other)
}

// Prev implements the Datum interface.
func (d *DOidWrapper) Prev(ctx CompareContext) (Datum, bool) {
	prev, ok := d.Wrapped.Prev(ctx)
	return wrapWithOid(prev, d.Oid), ok
}

// Next implements the Datum interface.
func (d *DOidWrapper) Next(ctx CompareContext) (Datum, bool) {
	next, ok := d.Wrapped.Next(ctx)
	return wrapWithOid(next, d.Oid), ok
}

// IsMax implements the Datum interface.
func (d *DOidWrapper) IsMax(ctx CompareContext) bool {
	return d.Wrapped.IsMax(ctx)
}

// IsMin implements the Datum interface.
func (d *DOidWrapper) IsMin(ctx CompareContext) bool {
	return d.Wrapped.IsMin(ctx)
}

// Max implements the Datum interface.
func (d *DOidWrapper) Max(ctx CompareContext) (Datum, bool) {
	max, ok := d.Wrapped.Max(ctx)
	return wrapWithOid(max, d.Oid), ok
}

// Min implements the Datum interface.
func (d *DOidWrapper) Min(ctx CompareContext) (Datum, bool) {
	min, ok := d.Wrapped.Min(ctx)
	return wrapWithOid(min, d.Oid), ok
}

// AmbiguousFormat implements the Datum interface.
func (d *DOidWrapper) AmbiguousFormat() bool {
	return d.Wrapped.AmbiguousFormat()
}

// Format implements the NodeFormatter interface.
func (d *DOidWrapper) Format(ctx *FmtCtx) {
	// Custom formatting based on d.OID could go here.
	ctx.FormatNode(d.Wrapped)
}

// Size implements the Datum interface.
func (d *DOidWrapper) Size() uintptr {
	return unsafe.Sizeof(*d) + d.Wrapped.Size()
}

// AmbiguousFormat implements the Datum interface.
func (d *Placeholder) AmbiguousFormat() bool {
	return true
}

// Compare implements the Datum interface.
func (d *Placeholder) Compare(ctx CompareContext, other Datum) int {
	res, err := d.CompareError(ctx, other)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareError implements the Datum interface.
func (d *Placeholder) CompareError(ctx CompareContext, other Datum) (int, error) {
	return ctx.MustGetPlaceholderValue(d).CompareError(ctx, other)
}

// Prev implements the Datum interface.
func (d *Placeholder) Prev(ctx CompareContext) (Datum, bool) {
	return ctx.MustGetPlaceholderValue(d).Prev(ctx)
}

// IsMin implements the Datum interface.
func (d *Placeholder) IsMin(ctx CompareContext) bool {
	return ctx.MustGetPlaceholderValue(d).IsMin(ctx)
}

// Next implements the Datum interface.
func (d *Placeholder) Next(ctx CompareContext) (Datum, bool) {
	return ctx.MustGetPlaceholderValue(d).Next(ctx)
}

// IsMax implements the Datum interface.
func (d *Placeholder) IsMax(ctx CompareContext) bool {
	return ctx.MustGetPlaceholderValue(d).IsMax(ctx)
}

// Max implements the Datum interface.
func (d *Placeholder) Max(ctx CompareContext) (Datum, bool) {
	return ctx.MustGetPlaceholderValue(d).Max(ctx)
}

// Min implements the Datum interface.
func (d *Placeholder) Min(ctx CompareContext) (Datum, bool) {
	return ctx.MustGetPlaceholderValue(d).Min(ctx)
}

// Size implements the Datum interface.
func (d *Placeholder) Size() uintptr {
	panic(errors.AssertionFailedf("shouldn't get called"))
}

// NewDNameFromDString is a helper routine to create a *DName (implemented as
// a *DOidWrapper) initialized from an existing *DString.
func NewDNameFromDString(d *DString) Datum {
	return wrapWithOid(d, oid.T_name)
}

// NewDName is a helper routine to create a *DName (implemented as a *DOidWrapper)
// initialized from a string.
func NewDName(d string) Datum {
	return NewDNameFromDString(NewDString(d))
}

// NewDIntVectorFromDArray is a helper routine to create a new *DArray,
// initialized from an existing *DArray, with the special oid for IntVector.
func NewDIntVectorFromDArray(d *DArray) Datum {
	ret := new(DArray)
	*ret = *d
	ret.customOid = oid.T_int2vector
	return ret
}

// NewDOidVectorFromDArray is a helper routine to create a new *DArray,
// initialized from an existing *DArray, with the special oid for OidVector.
func NewDOidVectorFromDArray(d *DArray) Datum {
	ret := new(DArray)
	*ret = *d
	ret.customOid = oid.T_oidvector
	return ret
}

// NewDefaultDatum returns a default non-NULL datum value for the given type.
// This is used when updating non-NULL columns that are being added or dropped
// from a table, and there is no user-defined DEFAULT value available.
func NewDefaultDatum(collationEnv *CollationEnvironment, t *types.T) (d Datum, err error) {
	switch t.Family() {
	case types.BoolFamily:
		return DBoolFalse, nil
	case types.IntFamily:
		return DZero, nil
	case types.FloatFamily:
		return DZeroFloat, nil
	case types.DecimalFamily:
		return dZeroDecimal, nil
	case types.DateFamily:
		return dEpochDate, nil
	case types.TimestampFamily:
		return DZeroTimestamp, nil
	case types.IntervalFamily:
		return dZeroInterval, nil
	case types.StringFamily:
		return dEmptyString, nil
	case types.BytesFamily:
		return dEmptyBytes, nil
	case types.TimestampTZFamily:
		return DZeroTimestampTZ, nil
	case types.CollatedStringFamily:
		return NewDCollatedString("", t.Locale(), collationEnv)
	case types.OidFamily:
		return NewDOidWithName(t.Oid(), t, t.SQLStandardName()), nil
	case types.UnknownFamily:
		return DNull, nil
	case types.UuidFamily:
		return DMinUUID, nil
	case types.ArrayFamily:
		return NewDArray(t.ArrayContents()), nil
	case types.INetFamily:
		return DMinIPAddr, nil
	case types.TimeFamily:
		return dTimeMin, nil
	case types.JsonFamily:
		return dNullJSON, nil
	case types.TimeTZFamily:
		return dZeroTimeTZ, nil
	case types.GeometryFamily, types.GeographyFamily, types.Box2DFamily:
		// TODO(otan): force Geometry/Geography to not allow `NOT NULL` columns to
		// make this impossible.
		return nil, pgerror.Newf(
			pgcode.FeatureNotSupported,
			"%s must be set or be NULL",
			t.Name(),
		)
	case types.TupleFamily:
		contents := t.TupleContents()
		datums := make([]Datum, len(contents))
		for i, subT := range contents {
			datums[i], err = NewDefaultDatum(collationEnv, subT)
			if err != nil {
				return nil, err
			}
		}
		return NewDTuple(t, datums...), nil
	case types.BitFamily:
		return bitArrayZero, nil
	case types.EnumFamily:
		// The scenario in which this arises is when the column is being dropped and
		// is NOT NULL. If there are no values for this enum, there's nothing that
		// can be put here so we'll return
		if len(t.TypeMeta.EnumData.PhysicalRepresentations) == 0 {
			return nil, pgerror.Newf(
				pgcode.NotNullViolation,
				"%s has no values which can be used to satisfy the NOT NULL "+
					"constraint while adding or dropping",
				t.Name(),
			)
		}
		// We fall back to using the smallest enum value during the dropping period.
		e, err := MakeDEnumFromPhysicalRepresentation(t, t.TypeMeta.EnumData.PhysicalRepresentations[0])
		if err != nil {
			return nil, err
		}
		return NewDEnum(e), nil
	default:
		return nil, errors.AssertionFailedf("unhandled type %v", t.SQLString())
	}
}

// DatumTypeSize returns a lower bound on the total size of a Datum
// of the given type in bytes, including memory that is
// pointed at (even if shared between Datum instances) but excluding
// allocation overhead.
//
// The second return value indicates whether data of this type have different
// sizes.
//
// It holds for every Datum d that d.Size() >= DatumSize(d.ResolvedType())
func DatumTypeSize(t *types.T) (size uintptr, isVarlen bool) {
	// The following are composite types or types that support multiple widths.
	switch t.Family() {
	case types.TupleFamily:
		if types.IsWildcardTupleType(t) {
			return uintptr(0), false
		}
		sz := uintptr(0)
		variable := false
		for i := range t.TupleContents() {
			typsz, typvariable := DatumTypeSize(t.TupleContents()[i])
			sz += typsz
			variable = variable || typvariable
		}
		return sz, variable
	case types.IntFamily, types.FloatFamily:
		return uintptr(t.Width() / 8), false

	case types.StringFamily:
		// T_char is a special string type that has a fixed size of 1. We have to
		// report its size accurately, and that it's not a variable-length datatype.
		if t.Oid() == oid.T_char {
			return 1, false
		}
	}

	// All the primary types have fixed size information.
	if bSzInfo, ok := baseDatumTypeSizes[t.Family()]; ok {
		return bSzInfo.sz, bSzInfo.variable
	}

	panic(errors.AssertionFailedf("unknown type: %T", t))
}

const (
	fixedSize    = false
	variableSize = true
)

var baseDatumTypeSizes = map[types.Family]struct {
	sz       uintptr
	variable bool
}{
	types.UnknownFamily:        {unsafe.Sizeof(dNull{}), fixedSize},
	types.BoolFamily:           {unsafe.Sizeof(DBool(false)), fixedSize},
	types.Box2DFamily:          {unsafe.Sizeof(DBox2D{CartesianBoundingBox: geo.CartesianBoundingBox{}}), fixedSize},
	types.BitFamily:            {unsafe.Sizeof(DBitArray{}), variableSize},
	types.IntFamily:            {unsafe.Sizeof(DInt(0)), fixedSize},
	types.FloatFamily:          {unsafe.Sizeof(DFloat(0.0)), fixedSize},
	types.DecimalFamily:        {unsafe.Sizeof(DDecimal{}), variableSize},
	types.StringFamily:         {unsafe.Sizeof(DString("")), variableSize},
	types.CollatedStringFamily: {unsafe.Sizeof(DCollatedString{"", "", nil}), variableSize},
	types.BytesFamily:          {unsafe.Sizeof(DBytes("")), variableSize},
	types.EncodedKeyFamily:     {unsafe.Sizeof(DBytes("")), variableSize},
	types.DateFamily:           {unsafe.Sizeof(DDate{}), fixedSize},
	types.GeographyFamily:      {unsafe.Sizeof(DGeography{}), variableSize},
	types.GeometryFamily:       {unsafe.Sizeof(DGeometry{}), variableSize},
	types.TimeFamily:           {unsafe.Sizeof(DTime(0)), fixedSize},
	types.TimeTZFamily:         {unsafe.Sizeof(DTimeTZ{}), fixedSize},
	types.TimestampFamily:      {unsafe.Sizeof(DTimestamp{}), fixedSize},
	types.TimestampTZFamily:    {unsafe.Sizeof(DTimestampTZ{}), fixedSize},
	types.TSQueryFamily:        {unsafe.Sizeof(DTSQuery{}), variableSize},
	types.TSVectorFamily:       {unsafe.Sizeof(DTSVector{}), variableSize},
	types.IntervalFamily:       {unsafe.Sizeof(DInterval{}), fixedSize},
	types.JsonFamily:           {unsafe.Sizeof(DJSON{}), variableSize},
	types.UuidFamily:           {unsafe.Sizeof(DUuid{}), fixedSize},
	types.INetFamily:           {unsafe.Sizeof(DIPAddr{}), fixedSize},
	types.OidFamily:            {unsafe.Sizeof(DOid{}.Oid), fixedSize},
	types.EnumFamily:           {unsafe.Sizeof(DEnum{}), variableSize},

	types.VoidFamily: {sz: unsafe.Sizeof(DVoid{}), variable: fixedSize},
	// TODO(jordan,justin): This seems suspicious.
	types.ArrayFamily: {unsafe.Sizeof(DString("")), variableSize},

	// TODO(jordan,justin): This seems suspicious.
	types.AnyFamily: {unsafe.Sizeof(DString("")), variableSize},
}

// MaxDistinctCount returns the maximum number of distinct values between the
// given datums (inclusive). This is possible if:
//
//	a. the types of the datums are equivalent and countable, or
//	b. the datums have the same value (in which case the distinct count is 1).
//
// If neither of these conditions hold, MaxDistinctCount returns ok=false.
// Additionally, it must be the case that first <= last, otherwise
// MaxDistinctCount returns ok=false.
func MaxDistinctCount(evalCtx CompareContext, first, last Datum) (_ int64, ok bool) {
	if !first.ResolvedType().Equivalent(last.ResolvedType()) {
		// The datums must be of the same type.
		return 0, false
	}
	if first.Compare(evalCtx, last) == 0 {
		// If the datums are equal, the distinct count is 1.
		return 1, true
	}

	// If the datums are a countable type, return the distinct count between them.
	var start, end int64

	switch t := first.(type) {
	case *DInt:
		otherDInt, otherOk := AsDInt(last)
		if otherOk {
			start = int64(*t)
			end = int64(otherDInt)
		}

	case *DOid:
		otherDOid, otherOk := AsDOid(last)
		if otherOk {
			start = int64(t.Oid)
			end = int64(otherDOid.Oid)
		}

	case *DDate:
		otherDDate, otherOk := last.(*DDate)
		if otherOk {
			if !t.IsFinite() || !otherDDate.IsFinite() {
				// One of the DDates isn't finite, so we can't extract a distinct count.
				return 0, false
			}
			start = int64((*t).PGEpochDays())
			end = int64(otherDDate.PGEpochDays())
		}

	case *DEnum:
		otherDEnum, otherOk := last.(*DEnum)
		if otherOk {
			startIdx, err := t.EnumTyp.EnumGetIdxOfPhysical(t.PhysicalRep)
			if err != nil {
				panic(err)
			}
			endIdx, err := t.EnumTyp.EnumGetIdxOfPhysical(otherDEnum.PhysicalRep)
			if err != nil {
				panic(err)
			}
			start, end = int64(startIdx), int64(endIdx)
		}

	case *DBool:
		otherDBool, otherOk := last.(*DBool)
		if otherOk {
			if *t {
				start = 1
			}
			if *otherDBool {
				end = 1
			}
		}

	default:
		// Uncountable type.
		return 0, false
	}

	if start > end {
		// Incorrect ordering.
		return 0, false
	}

	delta := (end - start) + 1
	if delta <= 0 {
		// Overflow or underflow.
		return 0, false
	}
	return delta, true
}

// ParsePath splits a string of the form "/foo/bar" into strings ["foo", "bar"].
// An empty string is allowed, otherwise the string must start with /.
func ParsePath(str string) []string {
	if str == "" {
		return nil
	}
	if str[0] != '/' {
		panic(str)
	}
	return strings.Split(str, "/")[1:]
}

// InferTypes takes a list of strings produced by ParsePath and returns a slice
// of datum types inferred from the strings. Type DInt will be used if possible,
// otherwise DString. For example, a vals slice ["1", "foo"] will give a types
// slice [Dint, DString].
func InferTypes(vals []string) []types.Family {
	// Infer the datum types and populate typs accordingly.
	typs := make([]types.Family, len(vals))
	for i := 0; i < len(vals); i++ {
		typ := types.IntFamily
		_, err := ParseDInt(vals[i])
		if err != nil {
			typ = types.StringFamily
		}
		typs[i] = typ
	}
	return typs
}

// AdjustValueToType checks that the width (for strings, byte arrays, and bit
// strings) and scale (decimal). and, shape/srid (for geospatial types) fits the
// specified column type.
//
// Additionally, some precision truncation may occur for the specified column type.
//
// In case of decimals, it can truncate fractional digits in the input
// value in order to fit the target column. If the input value fits the target
// column, it is returned unchanged. If the input value can be truncated to fit,
// then a truncated copy is returned. Otherwise, an error is returned.
//
// In the case of time, it can truncate fractional digits of time datums
// to its relevant rounding for the given type definition.
//
// In the case of geospatial types, it will check whether the SRID and Shape in the
// datum matches the type definition.
//
// This method is used by casts and parsing. It is important to note that this
// function will error if the given value is too wide for the given type. For
// explicit casts and parsing, inVal should be truncated before this function is
// called so that an error is not returned. For assignment casts, inVal should
// not be truncated before this function is called, so that an error is
// returned. The one exception for assignment casts is for the special "char"
// type. An assignment cast to "char" does not error and truncates a value if
// the width of the value is wider than a single character. For this exception,
// AdjustValueToType performs the truncation itself.
func AdjustValueToType(typ *types.T, inVal Datum) (outVal Datum, err error) {
	switch typ.Family() {
	case types.StringFamily, types.CollatedStringFamily:
		var sv string
		if v, ok := AsDString(inVal); ok {
			sv = string(v)
		} else if v, ok := inVal.(*DCollatedString); ok {
			sv = v.Contents
		}
		switch typ.Oid() {
		case oid.T_char:
			// "char" is supposed to truncate long values.
			sv = util.TruncateString(sv, 1)
		case oid.T_bpchar:
			// bpchar types truncate trailing whitespace.
			sv = strings.TrimRight(sv, " ")
		}
		if typ.Width() > 0 && utf8.RuneCountInString(sv) > int(typ.Width()) {
			return nil, pgerror.Newf(pgcode.StringDataRightTruncation,
				"value too long for type %s",
				typ.SQLString())
		}

		if typ.Oid() == oid.T_bpchar || typ.Oid() == oid.T_char {
			if _, ok := AsDString(inVal); ok {
				return NewDString(sv), nil
			} else if _, ok := inVal.(*DCollatedString); ok {
				return NewDCollatedString(sv, typ.Locale(), &CollationEnvironment{})
			}
		}
	case types.IntFamily:
		if v, ok := AsDInt(inVal); ok {
			if typ.Width() == 32 || typ.Width() == 16 {
				// Width is defined in bits.
				width := uint(typ.Width() - 1)

				// We're performing range checks in line with Go's
				// implementation of math.(Max|Min)(16|32) numbers that store
				// the boundaries of the allowed range.
				// NOTE: when updating the code below, make sure to update
				// execgen/cast_gen_util.go as well.
				shifted := v >> width
				if (v >= 0 && shifted > 0) || (v < 0 && shifted < -1) {
					if typ.Width() == 16 {
						return nil, ErrInt2OutOfRange
					}
					return nil, ErrInt4OutOfRange
				}
			}
		}
	case types.BitFamily:
		if v, ok := AsDBitArray(inVal); ok {
			if typ.Width() > 0 {
				bitLen := v.BitLen()
				switch typ.Oid() {
				case oid.T_varbit:
					if bitLen > uint(typ.Width()) {
						return nil, pgerror.Newf(pgcode.StringDataRightTruncation,
							"bit string length %d too large for type %s", bitLen, typ.SQLString())
					}
				default:
					if bitLen != uint(typ.Width()) {
						return nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
							"bit string length %d does not match type %s", bitLen, typ.SQLString())
					}
				}
			}
		}
	case types.DecimalFamily:
		if inDec, ok := inVal.(*DDecimal); ok {
			if inDec.Form != apd.Finite || typ.Precision() == 0 {
				// Non-finite form or unlimited target precision, so no need to limit.
				break
			}
			if int64(typ.Precision()) >= inDec.NumDigits() && typ.Scale() == inDec.Exponent {
				// Precision and scale of target column are sufficient.
				break
			}

			var outDec DDecimal
			outDec.Set(&inDec.Decimal)
			err := LimitDecimalWidth(&outDec.Decimal, int(typ.Precision()), int(typ.Scale()))
			if err != nil {
				return nil, errors.Wrapf(err, "type %s", typ.SQLString())
			}
			return &outDec, nil
		}
	case types.ArrayFamily:
		if inArr, ok := inVal.(*DArray); ok {
			var outArr *DArray
			elementType := typ.ArrayContents()
			for i, inElem := range inArr.Array {
				outElem, err := AdjustValueToType(elementType, inElem)
				if err != nil {
					return nil, err
				}
				if outElem != inElem {
					if outArr == nil {
						outArr = &DArray{}
						*outArr = *inArr
						outArr.Array = make(Datums, len(inArr.Array))
						copy(outArr.Array, inArr.Array[:i])
					}
				}
				if outArr != nil {
					outArr.Array[i] = inElem
				}
			}
			if outArr != nil {
				return outArr, nil
			}
		}
	case types.TimeFamily:
		if in, ok := inVal.(*DTime); ok {
			return in.Round(TimeFamilyPrecisionToRoundDuration(typ.Precision())), nil
		}
	case types.TimestampFamily:
		if in, ok := inVal.(*DTimestamp); ok {
			return in.Round(TimeFamilyPrecisionToRoundDuration(typ.Precision()))
		}
	case types.TimestampTZFamily:
		if in, ok := inVal.(*DTimestampTZ); ok {
			return in.Round(TimeFamilyPrecisionToRoundDuration(typ.Precision()))
		}
	case types.TimeTZFamily:
		if in, ok := inVal.(*DTimeTZ); ok {
			return in.Round(TimeFamilyPrecisionToRoundDuration(typ.Precision())), nil
		}
	case types.IntervalFamily:
		if in, ok := inVal.(*DInterval); ok {
			itm, err := typ.IntervalTypeMetadata()
			if err != nil {
				return nil, err
			}
			return NewDInterval(in.Duration, itm), nil
		}
	case types.GeometryFamily:
		if in, ok := inVal.(*DGeometry); ok {
			if err := geo.SpatialObjectFitsColumnMetadata(
				in.Geometry.SpatialObject(),
				typ.InternalType.GeoMetadata.SRID,
				typ.InternalType.GeoMetadata.ShapeType,
			); err != nil {
				return nil, err
			}
		}
	case types.GeographyFamily:
		if in, ok := inVal.(*DGeography); ok {
			if err := geo.SpatialObjectFitsColumnMetadata(
				in.Geography.SpatialObject(),
				typ.InternalType.GeoMetadata.SRID,
				typ.InternalType.GeoMetadata.ShapeType,
			); err != nil {
				return nil, err
			}
		}
	}
	return inVal, nil
}
