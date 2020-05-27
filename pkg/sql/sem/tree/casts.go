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
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/lib/pq/oid"
)

type castInfo struct {
	fromT   *types.T
	counter telemetry.Counter
}

// annotateCast produces an array of cast types decorated with cast
// type telemetry counters.
func annotateCast(toType *types.T, fromTypes []*types.T) []castInfo {
	ci := make([]castInfo, len(fromTypes))
	for i, fromType := range fromTypes {
		ci[i].fromT = fromType
	}
	rname := toType.String()

	for i, fromType := range fromTypes {
		lname := fromType.String()
		ci[i].counter = sqltelemetry.CastOpCounter(lname, rname)
	}
	return ci
}

var (
	bitArrayCastTypes = annotateCast(types.VarBit, []*types.T{types.Unknown, types.VarBit, types.Int, types.String, types.AnyCollatedString})
	boolCastTypes     = annotateCast(types.Bool, []*types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.AnyCollatedString})
	intCastTypes      = annotateCast(types.Int, []*types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.AnyCollatedString,
		types.Timestamp, types.TimestampTZ, types.Date, types.Interval, types.Oid, types.VarBit})
	floatCastTypes = annotateCast(types.Float, []*types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.AnyCollatedString,
		types.Timestamp, types.TimestampTZ, types.Date, types.Interval})
	geographyCastTypes = annotateCast(types.Geography, []*types.T{types.Unknown, types.String, types.Geography, types.Geometry})
	geometryCastTypes  = annotateCast(types.Geometry, []*types.T{types.Unknown, types.String, types.Geography, types.Geometry})
	decimalCastTypes   = annotateCast(types.Decimal, []*types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.AnyCollatedString,
		types.Timestamp, types.TimestampTZ, types.Date, types.Interval})
	stringCastTypes = annotateCast(types.String, []*types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.AnyCollatedString,
		types.VarBit,
		types.AnyArray, types.AnyTuple,
		types.Geometry, types.Geography,
		types.Bytes, types.Timestamp, types.TimestampTZ, types.Interval, types.Uuid, types.Date, types.Time, types.TimeTZ, types.Oid, types.INet, types.Jsonb, types.AnyEnum})
	bytesCastTypes = annotateCast(types.Bytes, []*types.T{types.Unknown, types.String, types.AnyCollatedString, types.AnyEnum, types.Bytes, types.Uuid})
	dateCastTypes  = annotateCast(types.Date, []*types.T{types.Unknown, types.String, types.AnyCollatedString, types.Date, types.Timestamp, types.TimestampTZ, types.Int})
	timeCastTypes  = annotateCast(types.Time, []*types.T{types.Unknown, types.String, types.AnyCollatedString, types.Time, types.TimeTZ,
		types.Timestamp, types.TimestampTZ, types.Interval})
	timeTZCastTypes    = annotateCast(types.TimeTZ, []*types.T{types.Unknown, types.String, types.AnyCollatedString, types.Time, types.TimeTZ, types.TimestampTZ})
	timestampCastTypes = annotateCast(types.Timestamp, []*types.T{types.Unknown, types.String, types.AnyCollatedString, types.Date, types.Timestamp, types.TimestampTZ, types.Int})
	intervalCastTypes  = annotateCast(types.Interval, []*types.T{types.Unknown, types.String, types.AnyCollatedString, types.Int, types.Time, types.Interval, types.Float, types.Decimal})
	oidCastTypes       = annotateCast(types.Oid, []*types.T{types.Unknown, types.String, types.AnyCollatedString, types.Int, types.Oid})
	uuidCastTypes      = annotateCast(types.Uuid, []*types.T{types.Unknown, types.String, types.AnyCollatedString, types.Bytes, types.Uuid})
	inetCastTypes      = annotateCast(types.INet, []*types.T{types.Unknown, types.String, types.AnyCollatedString, types.INet})
	arrayCastTypes     = annotateCast(types.AnyArray, []*types.T{types.Unknown, types.String})
	jsonCastTypes      = annotateCast(types.Jsonb, []*types.T{types.Unknown, types.String, types.Jsonb})
	enumCastTypes      = annotateCast(types.AnyEnum, []*types.T{types.Unknown, types.String, types.AnyEnum})
)

// validCastTypes returns a set of types that can be cast into the provided type.
func validCastTypes(t *types.T) []castInfo {
	switch t.Family() {
	case types.BitFamily:
		return bitArrayCastTypes
	case types.BoolFamily:
		return boolCastTypes
	case types.IntFamily:
		return intCastTypes
	case types.FloatFamily:
		return floatCastTypes
	case types.DecimalFamily:
		return decimalCastTypes
	case types.StringFamily, types.CollatedStringFamily:
		return stringCastTypes
	case types.BytesFamily:
		return bytesCastTypes
	case types.DateFamily:
		return dateCastTypes
	case types.GeographyFamily:
		return geographyCastTypes
	case types.GeometryFamily:
		return geometryCastTypes
	case types.TimeFamily:
		return timeCastTypes
	case types.TimeTZFamily:
		return timeTZCastTypes
	case types.TimestampFamily, types.TimestampTZFamily:
		return timestampCastTypes
	case types.IntervalFamily:
		return intervalCastTypes
	case types.JsonFamily:
		return jsonCastTypes
	case types.UuidFamily:
		return uuidCastTypes
	case types.INetFamily:
		return inetCastTypes
	case types.OidFamily:
		return oidCastTypes
	case types.ArrayFamily:
		return arrayCastTypes
	case types.EnumFamily:
		return enumCastTypes
	default:
		return nil
	}
}

// PerformCast performs a cast from the provided Datum to the specified
// types.T.
func PerformCast(ctx *EvalContext, d Datum, t *types.T) (Datum, error) {
	switch t.Family() {
	case types.BitFamily:
		switch v := d.(type) {
		case *DBitArray:
			if t.Width() == 0 || v.BitLen() == uint(t.Width()) {
				return d, nil
			}
			var a DBitArray
			switch t.Oid() {
			case oid.T_varbit:
				// VARBITs do not have padding attached.
				a.BitArray = v.BitArray.Clone()
				if uint(t.Width()) < a.BitArray.BitLen() {
					a.BitArray = a.BitArray.ToWidth(uint(t.Width()))
				}
			default:
				a.BitArray = v.BitArray.Clone().ToWidth(uint(t.Width()))
			}
			return &a, nil
		case *DInt:
			return NewDBitArrayFromInt(int64(*v), uint(t.Width()))
		case *DString:
			res, err := bitarray.Parse(string(*v))
			if err != nil {
				return nil, err
			}
			if t.Width() > 0 {
				res = res.ToWidth(uint(t.Width()))
			}
			return &DBitArray{BitArray: res}, nil
		case *DCollatedString:
			res, err := bitarray.Parse(v.Contents)
			if err != nil {
				return nil, err
			}
			if t.Width() > 0 {
				res = res.ToWidth(uint(t.Width()))
			}
			return &DBitArray{BitArray: res}, nil
		}

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
			return ParseDBool(string(*v))
		case *DCollatedString:
			return ParseDBool(v.Contents)
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
			d := ctx.getTmpDec()
			_, err := DecimalCtx.RoundToIntegralValue(d, &v.Decimal)
			if err != nil {
				return nil, err
			}
			i, err := d.Int64()
			if err != nil {
				return nil, ErrIntOutOfRange
			}
			res = NewDInt(DInt(i))
		case *DString:
			var err error
			if res, err = ParseDInt(string(*v)); err != nil {
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
			return ParseDFloat(string(*v))
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
		}

	case types.DecimalFamily:
		var dd DDecimal
		var err error
		unset := false
		switch v := d.(type) {
		case *DBool:
			if *v {
				dd.SetFinite(1, 0)
			}
		case *DInt:
			dd.SetFinite(int64(*v), 0)
		case *DDate:
			// TODO(mjibson): This cast is unsupported by postgres. Should we remove ours?
			if !v.IsFinite() {
				return nil, errDecOutOfRange
			}
			dd.SetFinite(v.UnixEpochDays(), 0)
		case *DFloat:
			_, err = dd.SetFloat64(float64(*v))
		case *DDecimal:
			// Small optimization to avoid copying into dd in normal case.
			if t.Precision() == 0 {
				return d, nil
			}
			dd.Set(&v.Decimal)
		case *DString:
			err = dd.SetString(string(*v))
		case *DCollatedString:
			err = dd.SetString(v.Contents)
		case *DTimestamp:
			val := &dd.Coeff
			val.SetInt64(v.Unix())
			val.Mul(val, big10E6)
			micros := v.Nanosecond() / int(time.Microsecond)
			val.Add(val, big.NewInt(int64(micros)))
			dd.Exponent = -6
		case *DTimestampTZ:
			val := &dd.Coeff
			val.SetInt64(v.Unix())
			val.Mul(val, big10E6)
			micros := v.Nanosecond() / int(time.Microsecond)
			val.Add(val, big.NewInt(int64(micros)))
			dd.Exponent = -6
		case *DInterval:
			v.AsBigInt(&dd.Coeff)
			dd.Exponent = -9
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
			return &dd, err
		}

	case types.StringFamily, types.CollatedStringFamily:
		var s string
		switch t := d.(type) {
		case *DBitArray:
			s = t.BitArray.String()
		case *DFloat:
			s = strconv.FormatFloat(float64(*t), 'g',
				ctx.SessionData.DataConversion.GetFloatPrec(), 64)
		case *DBool, *DInt, *DDecimal:
			s = d.String()
		case *DTimestamp, *DDate, *DTime, *DTimeTZ, *DGeography, *DGeometry:
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
			s = AsStringWithFlags(d, FmtPgwireText)
		case *DArray:
			s = AsStringWithFlags(d, FmtPgwireText)
		case *DInterval:
			// When converting an interval to string, we need a string representation
			// of the duration (e.g. "5s") and not of the interval itself (e.g.
			// "INTERVAL '5s'").
			s = t.ValueAsString()
		case *DUuid:
			s = t.UUID.String()
		case *DIPAddr:
			s = AsStringWithFlags(d, FmtBareStrings)
		case *DString:
			s = string(*t)
		case *DCollatedString:
			s = t.Contents
		case *DBytes:
			s = lex.EncodeByteArrayToRawBytes(string(*t),
				ctx.SessionData.DataConversion.BytesEncodeFormat, false /* skipHexPrefix */)
		case *DOid:
			s = t.String()
		case *DJSON:
			s = t.JSON.String()
		case *DEnum:
			s = t.LogicalRep
		}
		switch t.Family() {
		case types.StringFamily:
			if t.Oid() == oid.T_name {
				return NewDName(s), nil
			}

			// If the string type specifies a limit we truncate to that limit:
			//   'hello'::CHAR(2) -> 'he'
			// This is true of all the string type variants.
			if t.Width() > 0 && int(t.Width()) < len(s) {
				s = s[:t.Width()]
			}
			return NewDString(s), nil
		case types.CollatedStringFamily:
			// Ditto truncation like for TString.
			if t.Width() > 0 && int(t.Width()) < len(s) {
				s = s[:t.Width()]
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

	case types.GeographyFamily:
		switch d := d.(type) {
		case *DString:
			return ParseDGeography(string(*d))
		case *DCollatedString:
			return ParseDGeography(d.Contents)
		case *DGeography:
			if err := geo.GeospatialTypeFitsColumnMetadata(
				d.Geography,
				t.InternalType.GeoMetadata.SRID,
				t.InternalType.GeoMetadata.Shape,
			); err != nil {
				return nil, err
			}
			return d, nil
		case *DGeometry:
			g, err := d.AsGeography()
			if err != nil {
				return nil, err
			}
			if err := geo.GeospatialTypeFitsColumnMetadata(
				g,
				t.InternalType.GeoMetadata.SRID,
				t.InternalType.GeoMetadata.Shape,
			); err != nil {
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
			if err := geo.GeospatialTypeFitsColumnMetadata(
				d.Geometry,
				t.InternalType.GeoMetadata.SRID,
				t.InternalType.GeoMetadata.Shape,
			); err != nil {
				return nil, err
			}
			return d, nil
		case *DGeography:
			if err := geo.GeospatialTypeFitsColumnMetadata(
				d.Geography,
				t.InternalType.GeoMetadata.SRID,
				t.InternalType.GeoMetadata.Shape,
			); err != nil {
				return nil, err
			}
			return &DGeometry{d.AsGeometry()}, nil
		}

	case types.DateFamily:
		switch d := d.(type) {
		case *DString:
			return ParseDDate(ctx, string(*d))
		case *DCollatedString:
			return ParseDDate(ctx, d.Contents)
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
			return ParseDTime(ctx, string(*d), roundTo)
		case *DCollatedString:
			return ParseDTime(ctx, d.Contents, roundTo)
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
			return ParseDTimeTZ(ctx, string(*d), roundTo)
		case *DCollatedString:
			return ParseDTimeTZ(ctx, d.Contents, roundTo)
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
			return ParseDTimestamp(ctx, string(*d), roundTo)
		case *DCollatedString:
			return ParseDTimestamp(ctx, d.Contents, roundTo)
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
			return ParseDTimestampTZ(ctx, string(*d), roundTo)
		case *DCollatedString:
			return ParseDTimestampTZ(ctx, d.Contents, roundTo)
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
			return ParseDIntervalWithTypeMetadata(string(*v), itm)
		case *DCollatedString:
			return ParseDIntervalWithTypeMetadata(v.Contents, itm)
		case *DInt:
			return NewDInterval(duration.FromInt64(int64(*v)), itm), nil
		case *DFloat:
			return NewDInterval(duration.FromFloat64(float64(*v)), itm), nil
		case *DTime:
			return NewDInterval(duration.MakeDuration(int64(*v)*1000, 0, 0), itm), nil
		case *DDecimal:
			d := ctx.getTmpDec()
			dnanos := v.Decimal
			dnanos.Exponent += 9
			// We need HighPrecisionCtx because duration values can contain
			// upward of 35 decimal digits and DecimalCtx only provides 25.
			_, err := HighPrecisionCtx.Quantize(d, &dnanos, 0)
			if err != nil {
				return nil, err
			}
			if dnanos.Negative {
				d.Coeff.Neg(&d.Coeff)
			}
			dv, ok := duration.FromBigInt(&d.Coeff)
			if !ok {
				return nil, errDecOutOfRange
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
		}
	case types.ArrayFamily:
		switch v := d.(type) {
		case *DString:
			return ParseDArrayFromString(ctx, string(*v), t.ArrayContents())
		case *DArray:
			dcast := NewDArray(t.ArrayContents())
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
			switch t.Oid() {
			case oid.T_oid:
				return &DOid{semanticType: t, DInt: v.DInt}, nil
			case oid.T_regtype:
				// Mapping an oid to a regtype is easy: we have a hardcoded map.
				typ, ok := types.OidToType[oid.Oid(v.DInt)]
				ret := &DOid{semanticType: t, DInt: v.DInt}
				if !ok {
					return ret, nil
				}
				ret.name = typ.PGName()
				return ret, nil
			default:
				oid, err := queryOid(ctx, t, v)
				if err != nil {
					oid = NewDOid(v.DInt)
					oid.semanticType = t
				}
				return oid, nil
			}
		case *DInt:
			switch t.Oid() {
			case oid.T_oid:
				return &DOid{semanticType: t, DInt: *v}, nil
			default:
				tmpOid := NewDOid(*v)
				oid, err := queryOid(ctx, t, tmpOid)
				if err != nil {
					oid = tmpOid
					oid.semanticType = t
				}
				return oid, nil
			}
		case *DString:
			s := string(*v)
			// Trim whitespace and unwrap outer quotes if necessary.
			// This is required to mimic postgres.
			s = strings.TrimSpace(s)
			origS := s
			if len(s) > 1 && s[0] == '"' && s[len(s)-1] == '"' {
				s = s[1 : len(s)-1]
			}

			switch t.Oid() {
			case oid.T_oid:
				i, err := ParseDInt(s)
				if err != nil {
					return nil, err
				}
				return &DOid{semanticType: t, DInt: *i}, nil
			case oid.T_regproc, oid.T_regprocedure:
				// Trim procedure type parameters, e.g. `max(int)` becomes `max`.
				// Postgres only does this when the cast is ::regprocedure, but we're
				// going to always do it.
				// We additionally do not yet implement disambiguation based on type
				// parameters: we return the match iff there is exactly one.
				s = pgSignatureRegexp.ReplaceAllString(s, "$1")
				// Resolve function name.
				substrs := strings.Split(s, ".")
				if len(substrs) > 3 {
					// A fully qualified function name in pg's dialect can contain
					// at most 3 parts: db.schema.funname.
					// For example mydb.pg_catalog.max().
					// Anything longer is always invalid.
					return nil, pgerror.Newf(pgcode.Syntax,
						"invalid function name: %s", s)
				}
				name := UnresolvedName{NumParts: len(substrs)}
				for i := 0; i < len(substrs); i++ {
					name.Parts[i] = substrs[len(substrs)-1-i]
				}
				funcDef, err := name.ResolveFunction(ctx.SessionData.SearchPath)
				if err != nil {
					return nil, err
				}
				return queryOid(ctx, t, NewDString(funcDef.Name))
			case oid.T_regtype:
				parsedTyp, err := ctx.Planner.ParseType(s)
				if err == nil {
					return &DOid{
						semanticType: t,
						DInt:         DInt(parsedTyp.Oid()),
						name:         parsedTyp.SQLStandardName(),
					}, nil
				}
				// Fall back to searching pg_type, since we don't provide syntax for
				// every postgres type that we understand OIDs for.
				// Trim type modifiers, e.g. `numeric(10,3)` becomes `numeric`.
				s = pgSignatureRegexp.ReplaceAllString(s, "$1")
				dOid, missingTypeErr := queryOid(ctx, t, NewDString(s))
				if missingTypeErr == nil {
					return dOid, missingTypeErr
				}
				// Fall back to some special cases that we support for compatibility
				// only. Client use syntax like 'sometype'::regtype to produce the oid
				// for a type that they want to search a catalog table for. Since we
				// don't support that type, we return an artificial OID that will never
				// match anything.
				switch s {
				// We don't support triggers, but some tools search for them
				// specifically.
				case "trigger":
				default:
					return nil, missingTypeErr
				}
				return &DOid{
					semanticType: t,
					// Types we don't support get OID -1, so they won't match anything
					// in catalogs.
					DInt: -1,
					name: s,
				}, nil

			case oid.T_regclass:
				tn, err := ctx.Planner.ParseQualifiedTableName(origS)
				if err != nil {
					return nil, err
				}
				id, err := ctx.Planner.ResolveTableName(ctx.Ctx(), tn)
				if err != nil {
					return nil, err
				}
				return &DOid{
					semanticType: t,
					DInt:         DInt(id),
					name:         tn.ObjectName.String(),
				}, nil
			default:
				return queryOid(ctx, t, NewDString(s))
			}
		}
	}

	return nil, pgerror.Newf(
		pgcode.CannotCoerce, "invalid cast: %s -> %s", d.ResolvedType(), t)
}
