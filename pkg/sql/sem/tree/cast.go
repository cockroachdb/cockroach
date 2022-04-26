// Copyright 2020 The Cockroach Authors.
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
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/internal/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// LookupCastVolatility returns the volatility of a valid cast.
func LookupCastVolatility(
	from, to *types.T, sd *sessiondata.SessionData,
) (_ volatility.Volatility, ok bool) {
	return cast.LookupCastVolatility(from, to, sd)
}

// PerformCast performs a cast from the provided Datum to the specified
// types.T. The original datum is returned if its type is identical
// to the specified type.
func PerformCast(ctx *EvalContext, d Datum, t *types.T) (Datum, error) {
	ret, err := performCastWithoutPrecisionTruncation(ctx, d, t, true /* truncateWidth */)
	if err != nil {
		return nil, err
	}
	return AdjustValueToType(t, ret)
}

// PerformAssignmentCast performs an assignment cast from the provided Datum to
// the specified type. The original datum is returned if its type is identical
// to the specified type.
//
// It is similar to PerformCast, but differs because it errors when a bit-array
// or string values are too wide for the given type, rather than truncating the
// value. The one exception to this is casts to the special "char" type which
// are truncated.
func PerformAssignmentCast(ctx *EvalContext, d Datum, t *types.T) (Datum, error) {
	if !cast.ValidCast(d.ResolvedType(), t, cast.CastContextAssignment) {
		return nil, pgerror.Newf(
			pgcode.CannotCoerce,
			"invalid assignment cast: %s -> %s", d.ResolvedType(), t,
		)
	}
	d, err := performCastWithoutPrecisionTruncation(ctx, d, t, false /* truncateWidth */)
	if err != nil {
		return nil, err
	}
	return AdjustValueToType(t, d)
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
		sv = adjustStringValueToType(typ, sv)
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

// adjustStringToType checks that the width for strings fits the
// specified column type.
func adjustStringValueToType(typ *types.T, sv string) string {
	switch typ.Oid() {
	case oid.T_char:
		// "char" is supposed to truncate long values
		return util.TruncateString(sv, 1)
	case oid.T_bpchar:
		// bpchar types truncate trailing whitespace.
		return strings.TrimRight(sv, " ")
	}
	return sv
}

// formatBitArrayToType formats bit arrays such that they fill the total width
// if too short, or truncate if too long.
func formatBitArrayToType(d *DBitArray, t *types.T) *DBitArray {
	if t.Width() == 0 || d.BitLen() == uint(t.Width()) {
		return d
	}
	a := d.BitArray.Clone()
	switch t.Oid() {
	case oid.T_varbit:
		// VARBITs do not have padding attached, so only truncate.
		if uint(t.Width()) < a.BitLen() {
			a = a.ToWidth(uint(t.Width()))
		}
	default:
		a = a.ToWidth(uint(t.Width()))
	}
	return &DBitArray{a}
}

// performCastWithoutPrecisionTruncation performs the cast, but does not perform
// precision truncation. For example, if d is of type DECIMAL(6, 2) and t is
// DECIMAL(4, 2), d is not truncated to fit into t. However, if truncateWidth is
// true, widths are truncated to match the target type t for some types,
// including the bit and string types. If truncateWidth is false, the input
// datum is not truncated.
//
// In an ideal state, components of AdjustValueToType should be embedded into
// this function, but the code base needs a general refactor of parsing
// and casting logic before this can happen.
// See also: #55094.
func performCastWithoutPrecisionTruncation(
	ctx *EvalContext, d Datum, t *types.T, truncateWidth bool,
) (Datum, error) {
	// No conversion is needed if d is NULL.
	if d == DNull {
		return d, nil
	}

	// If we're casting a DOidWrapper, then we want to cast the wrapped datum.
	// It is also reasonable to lose the old Oid value too.
	// Note that we pass in nil as the first argument since we're not interested
	// in evaluating the placeholders.
	d = UnwrapDatum(nil /* evalCtx */, d)
	switch t.Family() {
	case types.BitFamily:
		var ba *DBitArray
		switch v := d.(type) {
		case *DBitArray:
			ba = v
		case *DInt:
			var err error
			ba, err = NewDBitArrayFromInt(int64(*v), uint(t.Width()))
			if err != nil {
				return nil, err
			}
		case *DString:
			res, err := bitarray.Parse(string(*v))
			if err != nil {
				return nil, err
			}
			ba = &DBitArray{res}
		case *DCollatedString:
			res, err := bitarray.Parse(v.Contents)
			if err != nil {
				return nil, err
			}
			ba = &DBitArray{res}
		}
		if truncateWidth {
			ba = formatBitArrayToType(ba, t)
		}
		return ba, nil

	case types.BoolFamily:
		switch v := d.(type) {
		case *DBool:
			return d, nil
		case *DInt:
			return MakeDBool(*v != 0), nil
		case *DFloat:
			return MakeDBool(*v != 0), nil
		case *DDecimal:
			return MakeDBool(v.Sign() != 0), nil
		case *DString:
			return ParseDBool(strings.TrimSpace(string(*v)))
		case *DCollatedString:
			return ParseDBool(v.Contents)
		case *DJSON:
			b, ok := v.AsBool()
			if !ok {
				return nil, failedCastFromJSON(v, t)
			}
			return MakeDBool(DBool(b)), nil
		}

	case types.IntFamily:
		var res *DInt
		switch v := d.(type) {
		case *DBitArray:
			res = v.AsDInt(uint(t.Width()))
		case *DBool:
			if *v {
				res = NewDInt(1)
			} else {
				res = DZero
			}
		case *DInt:
			// TODO(knz): enforce the coltype width here.
			res = v
		case *DFloat:
			f := float64(*v)
			// Use `<=` and `>=` here instead of just `<` and `>` because when
			// math.MaxInt64 and math.MinInt64 are converted to float64s, they are
			// rounded to numbers with larger absolute values. Note that the first
			// next FP value after and strictly greater than float64(math.MinInt64)
			// is -9223372036854774784 (= float64(math.MinInt64)+513) and the first
			// previous value and strictly smaller than float64(math.MaxInt64)
			// is 9223372036854774784 (= float64(math.MaxInt64)-513), and both are
			// convertible to int without overflow.
			if math.IsNaN(f) || f <= float64(math.MinInt64) || f >= float64(math.MaxInt64) {
				return nil, ErrIntOutOfRange
			}
			res = NewDInt(DInt(f))
		case *DDecimal:
			i, err := roundDecimalToInt(ctx, &v.Decimal)
			if err != nil {
				return nil, err
			}
			res = NewDInt(DInt(i))
		case *DString:
			var err error
			if res, err = ParseDInt(strings.TrimSpace(string(*v))); err != nil {
				return nil, err
			}
		case *DCollatedString:
			var err error
			if res, err = ParseDInt(v.Contents); err != nil {
				return nil, err
			}
		case *DTimestamp:
			res = NewDInt(DInt(v.Unix()))
		case *DTimestampTZ:
			res = NewDInt(DInt(v.Unix()))
		case *DDate:
			// TODO(mjibson): This cast is unsupported by postgres. Should we remove ours?
			if !v.IsFinite() {
				return nil, ErrIntOutOfRange
			}
			res = NewDInt(DInt(v.UnixEpochDays()))
		case *DInterval:
			iv, ok := v.AsInt64()
			if !ok {
				return nil, ErrIntOutOfRange
			}
			res = NewDInt(DInt(iv))
		case *DOid:
			res = &v.DInt
		case *DJSON:
			dec, ok := v.AsDecimal()
			if !ok {
				return nil, failedCastFromJSON(v, t)
			}
			i, err := dec.Int64()
			if err != nil {
				// Attempt to round the number to an integer.
				i, err = roundDecimalToInt(ctx, dec)
				if err != nil {
					return nil, err
				}
			}
			res = NewDInt(DInt(i))
		}
		if res != nil {
			return res, nil
		}

	case types.EnumFamily:
		switch v := d.(type) {
		case *DString:
			return MakeDEnumFromLogicalRepresentation(t, string(*v))
		case *DBytes:
			return MakeDEnumFromPhysicalRepresentation(t, []byte(*v))
		case *DEnum:
			return d, nil
		}

	case types.FloatFamily:
		switch v := d.(type) {
		case *DBool:
			if *v {
				return NewDFloat(1), nil
			}
			return NewDFloat(0), nil
		case *DInt:
			return NewDFloat(DFloat(*v)), nil
		case *DFloat:
			return d, nil
		case *DDecimal:
			f, err := v.Float64()
			if err != nil {
				return nil, ErrFloatOutOfRange
			}
			return NewDFloat(DFloat(f)), nil
		case *DString:
			return ParseDFloat(strings.TrimSpace(string(*v)))
		case *DCollatedString:
			return ParseDFloat(v.Contents)
		case *DTimestamp:
			micros := float64(v.Nanosecond() / int(time.Microsecond))
			return NewDFloat(DFloat(float64(v.Unix()) + micros*1e-6)), nil
		case *DTimestampTZ:
			micros := float64(v.Nanosecond() / int(time.Microsecond))
			return NewDFloat(DFloat(float64(v.Unix()) + micros*1e-6)), nil
		case *DDate:
			// TODO(mjibson): This cast is unsupported by postgres. Should we remove ours?
			if !v.IsFinite() {
				return nil, ErrFloatOutOfRange
			}
			return NewDFloat(DFloat(float64(v.UnixEpochDays()))), nil
		case *DInterval:
			return NewDFloat(DFloat(v.AsFloat64())), nil
		case *DJSON:
			dec, ok := v.AsDecimal()
			if !ok {
				return nil, failedCastFromJSON(v, t)
			}
			fl, err := dec.Float64()
			if err != nil {
				return nil, ErrFloatOutOfRange
			}
			return NewDFloat(DFloat(fl)), nil
		}

	case types.DecimalFamily:
		var dd DDecimal
		var err error
		unset := false
		switch v := d.(type) {
		case *DBool:
			if *v {
				dd.SetInt64(1)
			}
		case *DInt:
			dd.SetInt64(int64(*v))
		case *DDate:
			// TODO(mjibson): This cast is unsupported by postgres. Should we remove ours?
			if !v.IsFinite() {
				return nil, ErrDecOutOfRange
			}
			dd.SetInt64(v.UnixEpochDays())
		case *DFloat:
			_, err = dd.SetFloat64(float64(*v))
		case *DDecimal:
			// Small optimization to avoid copying into dd in normal case.
			if t.Precision() == 0 {
				return d, nil
			}
			dd.Set(&v.Decimal)
		case *DString:
			err = dd.SetString(strings.TrimSpace(string(*v)))
		case *DCollatedString:
			err = dd.SetString(v.Contents)
		case *DTimestamp:
			val := &dd.Coeff
			val.SetInt64(v.Unix())
			val.Mul(val, big10E6)
			micros := v.Nanosecond() / int(time.Microsecond)
			val.Add(val, apd.NewBigInt(int64(micros)))
			dd.Exponent = -6
		case *DTimestampTZ:
			val := &dd.Coeff
			val.SetInt64(v.Unix())
			val.Mul(val, big10E6)
			micros := v.Nanosecond() / int(time.Microsecond)
			val.Add(val, apd.NewBigInt(int64(micros)))
			dd.Exponent = -6
		case *DInterval:
			v.AsBigInt(&dd.Coeff)
			dd.Exponent = -9
		case *DJSON:
			dec, ok := v.AsDecimal()
			if !ok {
				return nil, failedCastFromJSON(v, t)
			}
			dd.Set(dec)
		default:
			unset = true
		}
		if err != nil {
			return nil, err
		}
		if !unset {
			// dd.Coeff must be positive. If it was set to a negative value
			// above, transfer the sign to dd.Negative.
			if dd.Coeff.Sign() < 0 {
				dd.Negative = true
				dd.Coeff.Abs(&dd.Coeff)
			}
			err = LimitDecimalWidth(&dd.Decimal, int(t.Precision()), int(t.Scale()))
			if err != nil {
				return nil, errors.Wrapf(err, "type %s", t.SQLString())
			}
			return &dd, nil
		}

	case types.StringFamily, types.CollatedStringFamily:
		var s string
		typ := t
		switch t := d.(type) {
		case *DBitArray:
			s = t.BitArray.String()
		case *DFloat:
			s = strconv.FormatFloat(float64(*t), 'g',
				ctx.SessionData().DataConversionConfig.GetFloatPrec(), 64)
		case *DInt:
			if typ.Oid() == oid.T_char {
				// int to "char" casts just return the correspondong ASCII byte.
				if *t > math.MaxInt8 || *t < math.MinInt8 {
					return nil, errCharOutOfRange
				} else if *t == 0 {
					s = ""
				} else {
					s = string([]byte{byte(*t)})
				}
			} else {
				s = d.String()
			}
		case *DBool, *DDecimal:
			s = d.String()
		case *DTimestamp, *DDate, *DTime, *DTimeTZ, *DGeography, *DGeometry, *DBox2D:
			s = AsStringWithFlags(d, FmtBareStrings)
		case *DTimestampTZ:
			// Convert to context timezone for correct display.
			ts, err := MakeDTimestampTZ(t.In(ctx.GetLocation()), time.Microsecond)
			if err != nil {
				return nil, err
			}
			s = AsStringWithFlags(
				ts,
				FmtBareStrings,
			)
		case *DTuple:
			s = AsStringWithFlags(
				d,
				FmtPgwireText,
				FmtDataConversionConfig(ctx.SessionData().DataConversionConfig),
			)
		case *DArray:
			s = AsStringWithFlags(
				d,
				FmtPgwireText,
				FmtDataConversionConfig(ctx.SessionData().DataConversionConfig),
			)
		case *DInterval:
			// When converting an interval to string, we need a string representation
			// of the duration (e.g. "5s") and not of the interval itself (e.g.
			// "INTERVAL '5s'").
			s = AsStringWithFlags(
				d,
				FmtPgwireText,
				FmtDataConversionConfig(ctx.SessionData().DataConversionConfig),
			)
		case *DUuid:
			s = t.UUID.String()
		case *DIPAddr:
			s = AsStringWithFlags(d, FmtBareStrings)
		case *DString:
			s = string(*t)
		case *DCollatedString:
			s = t.Contents
		case *DBytes:
			s = lex.EncodeByteArrayToRawBytes(
				string(*t),
				ctx.SessionData().DataConversionConfig.BytesEncodeFormat,
				false, /* skipHexPrefix */
			)
		case *DOid:
			s = t.String()
		case *DJSON:
			s = t.JSON.String()
		case *DEnum:
			s = t.LogicalRep
		case *DVoid:
			s = ""
		}
		switch t.Family() {
		case types.StringFamily:
			if t.Oid() == oid.T_name {
				return NewDName(s), nil
			}

			// bpchar types truncate trailing whitespace.
			if t.Oid() == oid.T_bpchar {
				s = strings.TrimRight(s, " ")
			}

			// If the string type specifies a limit we truncate to that limit:
			//   'hello'::CHAR(2) -> 'he'
			// This is true of all the string type variants.
			if truncateWidth && t.Width() > 0 {
				s = util.TruncateString(s, int(t.Width()))
			}
			return NewDString(s), nil
		case types.CollatedStringFamily:
			// bpchar types truncate trailing whitespace.
			if t.Oid() == oid.T_bpchar {
				s = strings.TrimRight(s, " ")
			}
			// Ditto truncation like for TString.
			if truncateWidth && t.Width() > 0 {
				s = util.TruncateString(s, int(t.Width()))
			}
			return NewDCollatedString(s, t.Locale(), &ctx.CollationEnv)
		}

	case types.BytesFamily:
		switch t := d.(type) {
		case *DString:
			return ParseDByte(string(*t))
		case *DCollatedString:
			return NewDBytes(DBytes(t.Contents)), nil
		case *DUuid:
			return NewDBytes(DBytes(t.GetBytes())), nil
		case *DBytes:
			return d, nil
		case *DGeography:
			return NewDBytes(DBytes(t.Geography.EWKB())), nil
		case *DGeometry:
			return NewDBytes(DBytes(t.Geometry.EWKB())), nil
		}

	case types.UuidFamily:
		switch t := d.(type) {
		case *DString:
			return ParseDUuidFromString(string(*t))
		case *DCollatedString:
			return ParseDUuidFromString(t.Contents)
		case *DBytes:
			return ParseDUuidFromBytes([]byte(*t))
		case *DUuid:
			return d, nil
		}

	case types.INetFamily:
		switch t := d.(type) {
		case *DString:
			return ParseDIPAddrFromINetString(string(*t))
		case *DCollatedString:
			return ParseDIPAddrFromINetString(t.Contents)
		case *DIPAddr:
			return d, nil
		}

	case types.Box2DFamily:
		switch d := d.(type) {
		case *DString:
			return ParseDBox2D(string(*d))
		case *DCollatedString:
			return ParseDBox2D(d.Contents)
		case *DBox2D:
			return d, nil
		case *DGeometry:
			bbox := d.CartesianBoundingBox()
			if bbox == nil {
				return DNull, nil
			}
			return NewDBox2D(*bbox), nil
		}

	case types.GeographyFamily:
		switch d := d.(type) {
		case *DString:
			return ParseDGeography(string(*d))
		case *DCollatedString:
			return ParseDGeography(d.Contents)
		case *DGeography:
			if err := geo.SpatialObjectFitsColumnMetadata(
				d.Geography.SpatialObject(),
				t.InternalType.GeoMetadata.SRID,
				t.InternalType.GeoMetadata.ShapeType,
			); err != nil {
				return nil, err
			}
			return d, nil
		case *DGeometry:
			g, err := d.AsGeography()
			if err != nil {
				return nil, err
			}
			if err := geo.SpatialObjectFitsColumnMetadata(
				g.SpatialObject(),
				t.InternalType.GeoMetadata.SRID,
				t.InternalType.GeoMetadata.ShapeType,
			); err != nil {
				return nil, err
			}
			return &DGeography{g}, nil
		case *DJSON:
			t, err := d.AsText()
			if err != nil {
				return nil, err
			}
			if t == nil {
				return DNull, nil
			}
			g, err := geo.ParseGeographyFromGeoJSON([]byte(*t))
			if err != nil {
				return nil, err
			}
			return &DGeography{g}, nil
		case *DBytes:
			g, err := geo.ParseGeographyFromEWKB(geopb.EWKB(*d))
			if err != nil {
				return nil, err
			}
			return &DGeography{g}, nil
		}
	case types.GeometryFamily:
		switch d := d.(type) {
		case *DString:
			return ParseDGeometry(string(*d))
		case *DCollatedString:
			return ParseDGeometry(d.Contents)
		case *DGeometry:
			if err := geo.SpatialObjectFitsColumnMetadata(
				d.Geometry.SpatialObject(),
				t.InternalType.GeoMetadata.SRID,
				t.InternalType.GeoMetadata.ShapeType,
			); err != nil {
				return nil, err
			}
			return d, nil
		case *DGeography:
			if err := geo.SpatialObjectFitsColumnMetadata(
				d.Geography.SpatialObject(),
				t.InternalType.GeoMetadata.SRID,
				t.InternalType.GeoMetadata.ShapeType,
			); err != nil {
				return nil, err
			}
			g, err := d.AsGeometry()
			if err != nil {
				return nil, err
			}
			return &DGeometry{g}, nil
		case *DJSON:
			t, err := d.AsText()
			if err != nil {
				return nil, err
			}
			if t == nil {
				return DNull, nil
			}
			g, err := geo.ParseGeometryFromGeoJSON([]byte(*t))
			if err != nil {
				return nil, err
			}
			return &DGeometry{g}, nil
		case *DBox2D:
			g, err := geo.MakeGeometryFromGeomT(d.ToGeomT(geopb.DefaultGeometrySRID))
			if err != nil {
				return nil, err
			}
			return &DGeometry{g}, nil
		case *DBytes:
			g, err := geo.ParseGeometryFromEWKB(geopb.EWKB(*d))
			if err != nil {
				return nil, err
			}
			return &DGeometry{g}, nil
		}

	case types.DateFamily:
		switch d := d.(type) {
		case *DString:
			res, _, err := ParseDDate(ctx, string(*d))
			return res, err
		case *DCollatedString:
			res, _, err := ParseDDate(ctx, d.Contents)
			return res, err
		case *DDate:
			return d, nil
		case *DInt:
			// TODO(mjibson): This cast is unsupported by postgres. Should we remove ours?
			t, err := pgdate.MakeDateFromUnixEpoch(int64(*d))
			return NewDDate(t), err
		case *DTimestampTZ:
			return NewDDateFromTime(d.Time.In(ctx.GetLocation()))
		case *DTimestamp:
			return NewDDateFromTime(d.Time)
		}

	case types.TimeFamily:
		roundTo := TimeFamilyPrecisionToRoundDuration(t.Precision())
		switch d := d.(type) {
		case *DString:
			res, _, err := ParseDTime(ctx, string(*d), roundTo)
			return res, err
		case *DCollatedString:
			res, _, err := ParseDTime(ctx, d.Contents, roundTo)
			return res, err
		case *DTime:
			return d.Round(roundTo), nil
		case *DTimeTZ:
			return MakeDTime(d.TimeOfDay.Round(roundTo)), nil
		case *DTimestamp:
			return MakeDTime(timeofday.FromTime(d.Time).Round(roundTo)), nil
		case *DTimestampTZ:
			// Strip time zone. Times don't carry their location.
			stripped, err := d.stripTimeZone(ctx)
			if err != nil {
				return nil, err
			}
			return MakeDTime(timeofday.FromTime(stripped.Time).Round(roundTo)), nil
		case *DInterval:
			return MakeDTime(timeofday.Min.Add(d.Duration).Round(roundTo)), nil
		}

	case types.TimeTZFamily:
		roundTo := TimeFamilyPrecisionToRoundDuration(t.Precision())
		switch d := d.(type) {
		case *DString:
			res, _, err := ParseDTimeTZ(ctx, string(*d), roundTo)
			return res, err
		case *DCollatedString:
			res, _, err := ParseDTimeTZ(ctx, d.Contents, roundTo)
			return res, err
		case *DTime:
			return NewDTimeTZFromLocation(timeofday.TimeOfDay(*d).Round(roundTo), ctx.GetLocation()), nil
		case *DTimeTZ:
			return d.Round(roundTo), nil
		case *DTimestampTZ:
			return NewDTimeTZFromTime(d.Time.In(ctx.GetLocation()).Round(roundTo)), nil
		}

	case types.TimestampFamily:
		roundTo := TimeFamilyPrecisionToRoundDuration(t.Precision())
		// TODO(knz): Timestamp from float, decimal.
		switch d := d.(type) {
		case *DString:
			res, _, err := ParseDTimestamp(ctx, string(*d), roundTo)
			return res, err
		case *DCollatedString:
			res, _, err := ParseDTimestamp(ctx, d.Contents, roundTo)
			return res, err
		case *DDate:
			t, err := d.ToTime()
			if err != nil {
				return nil, err
			}
			return MakeDTimestamp(t, roundTo)
		case *DInt:
			return MakeDTimestamp(timeutil.Unix(int64(*d), 0), roundTo)
		case *DTimestamp:
			return d.Round(roundTo)
		case *DTimestampTZ:
			// Strip time zone. Timestamps don't carry their location.
			stripped, err := d.stripTimeZone(ctx)
			if err != nil {
				return nil, err
			}
			return stripped.Round(roundTo)
		}

	case types.TimestampTZFamily:
		roundTo := TimeFamilyPrecisionToRoundDuration(t.Precision())
		// TODO(knz): TimestampTZ from float, decimal.
		switch d := d.(type) {
		case *DString:
			res, _, err := ParseDTimestampTZ(ctx, string(*d), roundTo)
			return res, err
		case *DCollatedString:
			res, _, err := ParseDTimestampTZ(ctx, d.Contents, roundTo)
			return res, err
		case *DDate:
			t, err := d.ToTime()
			if err != nil {
				return nil, err
			}
			_, before := t.Zone()
			_, after := t.In(ctx.GetLocation()).Zone()
			return MakeDTimestampTZ(t.Add(time.Duration(before-after)*time.Second), roundTo)
		case *DTimestamp:
			_, before := d.Time.Zone()
			_, after := d.Time.In(ctx.GetLocation()).Zone()
			return MakeDTimestampTZ(d.Time.Add(time.Duration(before-after)*time.Second), roundTo)
		case *DInt:
			return MakeDTimestampTZ(timeutil.Unix(int64(*d), 0), roundTo)
		case *DTimestampTZ:
			return d.Round(roundTo)
		}

	case types.IntervalFamily:
		itm, err := t.IntervalTypeMetadata()
		if err != nil {
			return nil, err
		}
		switch v := d.(type) {
		case *DString:
			return ParseDIntervalWithTypeMetadata(ctx.GetIntervalStyle(), string(*v), itm)
		case *DCollatedString:
			return ParseDIntervalWithTypeMetadata(ctx.GetIntervalStyle(), v.Contents, itm)
		case *DInt:
			return NewDInterval(duration.FromInt64(int64(*v)), itm), nil
		case *DFloat:
			return NewDInterval(duration.FromFloat64(float64(*v)), itm), nil
		case *DTime:
			return NewDInterval(duration.MakeDuration(int64(*v)*1000, 0, 0), itm), nil
		case *DDecimal:
			var d apd.Decimal
			dnanos := v.Decimal
			dnanos.Exponent += 9
			// We need HighPrecisionCtx because duration values can contain
			// upward of 35 decimal digits and DecimalCtx only provides 25.
			_, err := HighPrecisionCtx.Quantize(&d, &dnanos, 0)
			if err != nil {
				return nil, err
			}
			if dnanos.Negative {
				d.Coeff.Neg(&d.Coeff)
			}
			dv, ok := duration.FromBigInt(&d.Coeff)
			if !ok {
				return nil, ErrDecOutOfRange
			}
			return NewDInterval(dv, itm), nil
		case *DInterval:
			return NewDInterval(v.Duration, itm), nil
		}
	case types.JsonFamily:
		switch v := d.(type) {
		case *DString:
			return ParseDJSON(string(*v))
		case *DJSON:
			return v, nil
		case *DGeography:
			j, err := geo.SpatialObjectToGeoJSON(v.Geography.SpatialObject(), -1, geo.SpatialObjectToGeoJSONFlagZero)
			if err != nil {
				return nil, err
			}
			return ParseDJSON(string(j))
		case *DGeometry:
			j, err := geo.SpatialObjectToGeoJSON(v.Geometry.SpatialObject(), -1, geo.SpatialObjectToGeoJSONFlagZero)
			if err != nil {
				return nil, err
			}
			return ParseDJSON(string(j))
		}
	case types.ArrayFamily:
		switch v := d.(type) {
		case *DString:
			res, _, err := ParseDArrayFromString(ctx, string(*v), t.ArrayContents())
			return res, err
		case *DArray:
			dcast := NewDArray(t.ArrayContents())
			if err := dcast.MaybeSetCustomOid(t); err != nil {
				return nil, err
			}
			for _, e := range v.Array {
				ecast := DNull
				if e != DNull {
					var err error
					ecast, err = PerformCast(ctx, e, t.ArrayContents())
					if err != nil {
						return nil, err
					}
				}

				if err := dcast.Append(ecast); err != nil {
					return nil, err
				}
			}
			return dcast, nil
		}
	case types.OidFamily:
		switch v := d.(type) {
		case *DOid:
			return performIntToOidCast(ctx, t, v.DInt)
		case *DInt:
			// OIDs are always unsigned 32-bit integers. Some languages, like Java,
			// store OIDs as signed 32-bit integers, so we implement the cast
			// by converting to a uint32 first. This matches Postgres behavior.
			i := DInt(uint32(*v))
			return performIntToOidCast(ctx, t, i)
		case *DString:
			if t.Oid() != oid.T_oid && string(*v) == ZeroOidValue {
				return wrapAsZeroOid(t), nil
			}
			return ParseDOid(ctx, string(*v), t)
		}
	case types.TupleFamily:
		switch v := d.(type) {
		case *DTuple:
			if t == types.AnyTuple {
				// If AnyTuple is the target type, we can just use the input tuple.
				return v, nil
			}
			// To cast a Tuple to a Tuple, the lengths must be the same on both sides.
			// Then, each element is casted to the other element type. The labels of
			// the target type are kept.
			if len(v.D) != len(t.TupleContents()) {
				return nil, pgerror.New(
					pgcode.CannotCoerce, "cannot cast tuple; wrong number of columns")
			}
			ret := NewDTupleWithLen(t, len(v.D))
			for i := range v.D {
				var err error
				ret.D[i], err = PerformCast(ctx, v.D[i], t.TupleContents()[i])
				if err != nil {
					return nil, err
				}
			}
			return ret, nil
		case *DString:
			res, _, err := ParseDTupleFromString(ctx, string(*v), t)
			return res, err
		}
	case types.VoidFamily:
		switch d.(type) {
		case *DString:
			return DVoidDatum, nil
		}
	}

	return nil, pgerror.Newf(
		pgcode.CannotCoerce, "invalid cast: %s -> %s", d.ResolvedType(), t)
}

// performIntToOidCast casts the input integer to the OID type given by the
// input types.T.
func performIntToOidCast(ctx *EvalContext, t *types.T, v DInt) (Datum, error) {
	switch t.Oid() {
	case oid.T_oid:
		return &DOid{semanticType: t, DInt: v}, nil
	case oid.T_regtype:
		// Mapping an dOid to a regtype is easy: we have a hardcoded map.
		ret := &DOid{semanticType: t, DInt: v}
		if typ, ok := types.OidToType[oid.Oid(v)]; ok {
			ret.name = typ.PGName()
		} else if types.IsOIDUserDefinedType(oid.Oid(v)) {
			typ, err := ctx.Planner.ResolveTypeByOID(ctx.Context, oid.Oid(v))
			if err != nil {
				return nil, err
			}
			ret.name = typ.PGName()
		} else if v == 0 {
			return wrapAsZeroOid(t), nil
		}
		return ret, nil

	case oid.T_regproc, oid.T_regprocedure:
		// Mapping an dOid to a regproc is easy: we have a hardcoded map.
		name, ok := OidToBuiltinName[oid.Oid(v)]
		ret := &DOid{semanticType: t, DInt: v}
		if !ok {
			if v == 0 {
				return wrapAsZeroOid(t), nil
			}
			return ret, nil
		}
		ret.name = name
		return ret, nil

	default:
		if v == 0 {
			return wrapAsZeroOid(t), nil
		}

		dOid, err := ctx.Planner.ResolveOIDFromOID(ctx.Ctx(), t, NewDOid(v))
		if err != nil {
			dOid = NewDOid(v)
			dOid.semanticType = t
		}
		return dOid, nil
	}
}

func roundDecimalToInt(ctx *EvalContext, d *apd.Decimal) (int64, error) {
	var newD apd.Decimal
	if _, err := DecimalCtx.RoundToIntegralValue(&newD, d); err != nil {
		return 0, err
	}
	i, err := newD.Int64()
	if err != nil {
		return 0, ErrIntOutOfRange
	}
	return i, nil
}

func failedCastFromJSON(j *DJSON, t *types.T) error {
	return pgerror.Newf(
		pgcode.InvalidParameterValue,
		"cannot cast jsonb %s to type %s",
		j.Type(), t,
	)
}

// PopulateRecordWithJSON is used for the json to record function family, like
// json_populate_record. Given a JSON object, a desired tuple type, and a tuple
// of the same length as the desired type, this function will populate the tuple
// by setting each named field in the tuple to the value of the key with the
// same name in the input JSON object. Fields in the tuple that are not present
// in the JSON will not be modified, and JSON keys that are not in the tuple
// will be ignored.
// Each field will be set by a best-effort coercion to its type from the JSON
// field. The logic is more permissive than casts.
func PopulateRecordWithJSON(
	ctx *EvalContext, j json.JSON, desiredType *types.T, tup *DTuple,
) error {
	if j.Type() != json.ObjectJSONType {
		return pgerror.Newf(pgcode.InvalidParameterValue, "expected JSON object")
	}
	tupleTypes := desiredType.TupleContents()
	labels := desiredType.TupleLabels()
	if labels == nil {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"anonymous records cannot be used with json{b}_populate_record{set}",
		)
	}
	for i := range tupleTypes {
		val, err := j.FetchValKey(labels[i])
		if err != nil || val == nil {
			// No value? Use the value that was already in the tuple.
			continue
		}
		tup.D[i], err = PopulateDatumWithJSON(ctx, val, tupleTypes[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// PopulateDatumWithJSON is used for the json to record function family, like
// json_populate_record. It's less restrictive than the casting system, which
// is why it's implemented separately.
func PopulateDatumWithJSON(ctx *EvalContext, j json.JSON, desiredType *types.T) (Datum, error) {
	if j == json.NullJSONValue {
		return DNull, nil
	}
	switch desiredType.Family() {
	case types.ArrayFamily:
		if j.Type() != json.ArrayJSONType {
			return nil, pgerror.Newf(pgcode.InvalidTextRepresentation, "expected JSON array")
		}
		n := j.Len()
		elementTyp := desiredType.ArrayContents()
		d := NewDArray(elementTyp)
		d.Array = make(Datums, n)
		for i := 0; i < n; i++ {
			elt, err := j.FetchValIdx(i)
			if err != nil {
				return nil, err
			}
			d.Array[i], err = PopulateDatumWithJSON(ctx, elt, elementTyp)
			if err != nil {
				return nil, err
			}
		}
		return d, nil
	case types.TupleFamily:
		tup := NewDTupleWithLen(desiredType, len(desiredType.TupleContents()))
		for i := range tup.D {
			tup.D[i] = DNull
		}
		err := PopulateRecordWithJSON(ctx, j, desiredType, tup)
		return tup, err
	}
	var s string
	switch j.Type() {
	case json.StringJSONType:
		t, err := j.AsText()
		if err != nil {
			return nil, err
		}
		s = *t
	default:
		s = j.String()
	}
	return PerformCast(ctx, NewDString(s), desiredType)
}

// castCounterType represents a cast from one family to another.
type castCounterType struct {
	from, to types.Family
}

// castCounterMap is a map of cast counter types to their corresponding
// telemetry counters.
var castCounters map[castCounterType]telemetry.Counter

// Initialize castCounters.
func init() {
	castCounters = make(map[castCounterType]telemetry.Counter)
	for fromID := range types.Family_name {
		for toID := range types.Family_name {
			from := types.Family(fromID)
			to := types.Family(toID)
			var c telemetry.Counter
			switch {
			case from == types.ArrayFamily && to == types.ArrayFamily:
				c = sqltelemetry.ArrayCastCounter
			case from == types.TupleFamily && to == types.TupleFamily:
				c = sqltelemetry.TupleCastCounter
			case from == types.EnumFamily && to == types.EnumFamily:
				c = sqltelemetry.EnumCastCounter
			default:
				c = sqltelemetry.CastOpCounter(from.Name(), to.Name())
			}
			castCounters[castCounterType{from, to}] = c
		}
	}
}

// GetCastCounter returns the telemetry counter for the cast from one family to
// another family.
func GetCastCounter(from, to types.Family) telemetry.Counter {
	if c, ok := castCounters[castCounterType{from, to}]; ok {
		return c
	}
	panic(errors.AssertionFailedf(
		"no cast counter found for cast from %s to %s", from.Name(), to.Name(),
	))
}
