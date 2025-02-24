// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"context"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/tsearch"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// ReType ensures that the given expression evaluates to the requested type,
// wrapping the expression in a cast if necessary. Returns ok=false if a cast
// cannot wrap the expression because no valid cast from the expression's type
// to the wanted type exists.
func ReType(expr tree.TypedExpr, wantedType *types.T) (_ tree.TypedExpr, ok bool) {
	resolvedType := expr.ResolvedType()
	if wantedType.Family() == types.AnyFamily || resolvedType.Identical(wantedType) {
		return expr, true
	}
	// TODO(#75103): For legacy reasons, we check for a valid cast in the most
	// permissive context, ContextExplicit. To be consistent with Postgres,
	// we should check for a valid cast in the most restrictive context,
	// ContextImplicit.
	if !cast.ValidCast(resolvedType, wantedType, cast.ContextExplicit) {
		return nil, false
	}
	ce := tree.NewTypedCastExpr(expr, wantedType)
	ce.SyntaxMode = 0 // preserved pre-existing behavior
	return ce, true
}

// PerformCast performs a cast from the provided Datum to the specified
// types.T. The original datum is returned if its type is identical
// to the specified type.
func PerformCast(
	ctx context.Context, evalCtx *Context, d tree.Datum, t *types.T,
) (tree.Datum, error) {
	return performCast(ctx, evalCtx, d, t, true /* truncateWidth */)
}

// PerformCastNoTruncate performs an explicit cast, but returns an error if the
// value doesn't fit in the required type. It is used for coercing types in a
// PLpgSQL INTO clause.
func PerformCastNoTruncate(
	ctx context.Context, evalCtx *Context, d tree.Datum, t *types.T,
) (tree.Datum, error) {
	return performCast(ctx, evalCtx, d, t, false /* truncateWidth */)
}

// PerformAssignmentCast performs an assignment cast from the provided Datum to
// the specified type. The original datum is returned if its type is identical
// to the specified type.
//
// It is similar to PerformCast, but differs because it errors when a bit-array
// or string values are too wide for the given type, rather than truncating the
// value. The one exception to this is casts to the special "char" type which
// are truncated.
func PerformAssignmentCast(
	ctx context.Context, evalCtx *Context, d tree.Datum, t *types.T,
) (tree.Datum, error) {
	if !cast.ValidCast(d.ResolvedType(), t, cast.ContextAssignment) {
		return nil, pgerror.Newf(
			pgcode.CannotCoerce,
			"invalid assignment cast: %s -> %s", d.ResolvedType(), t,
		)
	}
	return performCast(ctx, evalCtx, d, t, false /* truncateWidth */)
}

func performCast(
	ctx context.Context, evalCtx *Context, d tree.Datum, t *types.T, truncateWidth bool,
) (tree.Datum, error) {
	d, err := performCastWithoutPrecisionTruncation(ctx, evalCtx, d, t, truncateWidth)
	if err != nil {
		return nil, err
	}
	return tree.AdjustValueToType(t, d)
}

var (
	big10E6  = apd.NewBigInt(1e6)
	big10E10 = apd.NewBigInt(1e10)
)

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
	ctx context.Context, evalCtx *Context, d tree.Datum, t *types.T, truncateWidth bool,
) (tree.Datum, error) {
	// No conversion is needed if d is NULL.
	if d == tree.DNull {
		return d, nil
	}

	// If we're casting a DOidWrapper, then we want to cast the wrapped datum.
	// It is also reasonable to lose the old Oid value too.
	// Note that we pass in nil as the first argument since we're not interested
	// in evaluating the placeholders.
	d = tree.UnwrapDOidWrapper(d)
	switch t.Family() {
	case types.BitFamily:
		var ba *tree.DBitArray
		switch v := d.(type) {
		case *tree.DBitArray:
			ba = v
		case *tree.DInt:
			var err error
			ba, err = tree.NewDBitArrayFromInt(int64(*v), uint(t.Width()))
			if err != nil {
				return nil, err
			}
		case *tree.DString:
			res, err := bitarray.Parse(string(*v))
			if err != nil {
				return nil, err
			}
			ba = &tree.DBitArray{BitArray: res}
		case *tree.DCollatedString:
			res, err := bitarray.Parse(v.Contents)
			if err != nil {
				return nil, err
			}
			ba = &tree.DBitArray{BitArray: res}
		}
		if ba != nil {
			if truncateWidth {
				ba = tree.FormatBitArrayToType(ba, t)
			}
			return ba, nil
		}

	case types.BoolFamily:
		switch v := d.(type) {
		case *tree.DBool:
			return d, nil
		case *tree.DInt:
			return tree.MakeDBool(*v != 0), nil
		case *tree.DFloat:
			return tree.MakeDBool(*v != 0), nil
		case *tree.DDecimal:
			return tree.MakeDBool(v.Sign() != 0), nil
		case *tree.DString:
			// No need to trim the spaces explicitly since ParseDBool does that
			// itself.
			return tree.ParseDBool(string(*v))
		case *tree.DCollatedString:
			return tree.ParseDBool(v.Contents)
		case *tree.DJSON:
			b, ok := v.AsBool()
			if !ok {
				return nil, failedCastFromJSON(v, t)
			}
			return tree.MakeDBool(tree.DBool(b)), nil
		}

	case types.IntFamily:
		var res *tree.DInt
		switch v := d.(type) {
		case *tree.DBitArray:
			res = v.AsDInt(uint(t.Width()))
		case *tree.DBool:
			if *v {
				res = tree.NewDInt(1)
			} else {
				res = tree.DZero
			}
		case *tree.DInt:
			// TODO(knz): enforce the coltype width here.
			res = v
		case *tree.DFloat:
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
				return nil, tree.ErrIntOutOfRange
			}
			res = tree.NewDInt(tree.DInt(math.RoundToEven(f)))
		case *tree.DDecimal:
			i, err := roundDecimalToInt(&v.Decimal)
			if err != nil {
				return nil, err
			}
			res = tree.NewDInt(tree.DInt(i))
		case *tree.DString:
			var err error
			if res, err = tree.ParseDInt(strings.TrimSpace(string(*v))); err != nil {
				return nil, err
			}
		case *tree.DCollatedString:
			var err error
			if res, err = tree.ParseDInt(v.Contents); err != nil {
				return nil, err
			}
		case *tree.DTimestamp:
			res = tree.NewDInt(tree.DInt(v.Unix()))
		case *tree.DTimestampTZ:
			res = tree.NewDInt(tree.DInt(v.Unix()))
		case *tree.DDate:
			// TODO(mjibson): This cast is unsupported by postgres. Should we remove ours?
			if !v.IsFinite() {
				return nil, tree.ErrIntOutOfRange
			}
			res = tree.NewDInt(tree.DInt(v.UnixEpochDays()))
		case *tree.DInterval:
			// TODO(mgartner): This cast is not supported in Postgres. We should
			// remove it.
			iv, ok := v.AsInt64()
			if !ok {
				return nil, tree.ErrIntOutOfRange
			}
			res = tree.NewDInt(tree.DInt(iv))
		case *tree.DOid:
			res = tree.NewDInt(tree.DInt(v.Oid))
		case *tree.DJSON:
			dec, ok := v.AsDecimal()
			if !ok {
				return nil, failedCastFromJSON(v, t)
			}
			i, err := dec.Int64()
			if err != nil {
				// Attempt to round the number to an integer.
				i, err = roundDecimalToInt(dec)
				if err != nil {
					return nil, err
				}
			}
			res = tree.NewDInt(tree.DInt(i))
		}
		if res != nil {
			return res, nil
		}

	case types.EnumFamily:
		switch v := d.(type) {
		case *tree.DString:
			e, err := tree.MakeDEnumFromLogicalRepresentation(t, string(*v))
			if err != nil {
				return nil, err
			}
			return tree.NewDEnum(e), nil
		case *tree.DBytes:
			e, err := tree.MakeDEnumFromPhysicalRepresentation(t, []byte(*v))
			if err != nil {
				return nil, err
			}
			return tree.NewDEnum(e), nil
		case *tree.DEnum:
			return d, nil
		}

	case types.FloatFamily:
		switch v := d.(type) {
		case *tree.DBool:
			if *v {
				return tree.NewDFloat(1), nil
			}
			return tree.NewDFloat(0), nil
		case *tree.DInt:
			return tree.NewDFloat(tree.DFloat(*v)), nil
		case *tree.DFloat:
			return d, nil
		case *tree.DDecimal:
			f, err := v.Float64()
			if err != nil {
				return nil, tree.ErrFloatOutOfRange
			}
			return tree.NewDFloat(tree.DFloat(f)), nil
		case *tree.DString:
			return tree.ParseDFloat(strings.TrimSpace(string(*v)))
		case *tree.DCollatedString:
			return tree.ParseDFloat(v.Contents)
		case *tree.DTimestamp:
			micros := float64(v.Nanosecond() / int(time.Microsecond))
			return tree.NewDFloat(tree.DFloat(float64(v.Unix()) + micros*1e-6)), nil
		case *tree.DTimestampTZ:
			micros := float64(v.Nanosecond() / int(time.Microsecond))
			return tree.NewDFloat(tree.DFloat(float64(v.Unix()) + micros*1e-6)), nil
		case *tree.DDate:
			// TODO(mjibson): This cast is unsupported by postgres. Should we remove ours?
			if !v.IsFinite() {
				return nil, tree.ErrFloatOutOfRange
			}
			return tree.NewDFloat(tree.DFloat(v.UnixEpochDays())), nil
		case *tree.DInterval:
			return tree.NewDFloat(tree.DFloat(v.AsFloat64())), nil
		case *tree.DJSON:
			dec, ok := v.AsDecimal()
			if !ok {
				return nil, failedCastFromJSON(v, t)
			}
			fl, err := dec.Float64()
			if err != nil {
				return nil, tree.ErrFloatOutOfRange
			}
			return tree.NewDFloat(tree.DFloat(fl)), nil
		}

	case types.DecimalFamily:
		var dd tree.DDecimal
		var err error
		unset := false
		switch v := d.(type) {
		case *tree.DBool:
			if *v {
				dd.SetInt64(1)
			}
		case *tree.DInt:
			dd.SetInt64(int64(*v))
		case *tree.DDate:
			// TODO(mjibson): This cast is unsupported by postgres. Should we remove ours?
			if !v.IsFinite() {
				return nil, tree.ErrDecOutOfRange
			}
			dd.SetInt64(v.UnixEpochDays())
		case *tree.DFloat:
			_, err = dd.SetFloat64(float64(*v))
		case *tree.DDecimal:
			// Small optimization to avoid copying into dd in normal case.
			if t.Precision() == 0 {
				return d, nil
			}
			dd.Set(&v.Decimal)
		case *tree.DString:
			err = dd.SetString(strings.TrimSpace(string(*v)))
		case *tree.DCollatedString:
			err = dd.SetString(v.Contents)
		case *tree.DTimestamp:
			val := &dd.Coeff
			val.SetInt64(v.Unix())
			val.Mul(val, big10E6)
			micros := v.Nanosecond() / int(time.Microsecond)
			val.Add(val, apd.NewBigInt(int64(micros)))
			dd.Exponent = -6
		case *tree.DTimestampTZ:
			val := &dd.Coeff
			val.SetInt64(v.Unix())
			val.Mul(val, big10E6)
			micros := v.Nanosecond() / int(time.Microsecond)
			val.Add(val, apd.NewBigInt(int64(micros)))
			dd.Exponent = -6
		case *tree.DInterval:
			v.AsBigInt(&dd.Coeff)
			dd.Exponent = -9
		case *tree.DJSON:
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
			err = tree.LimitDecimalWidth(&dd.Decimal, int(t.Precision()), int(t.Scale()))
			if err != nil {
				return nil, errors.Wrapf(err, "type %s", t.SQLString())
			}
			return &dd, nil
		}

	case types.StringFamily, types.CollatedStringFamily:
		var s string
		typ := t
		switch t := d.(type) {
		case *tree.DBitArray:
			s = t.BitArray.String()
		case *tree.DFloat:
			// At the time of this writing, the FLOAT4 -> TEXT cast is flawed, since
			// the resolved type for a DFloat is always FLOAT8, meaning
			// floatTyp.Width() will always return 64.
			floatTyp := t.ResolvedType()
			b := tree.PgwireFormatFloat(nil /* buf */, float64(*t), evalCtx.SessionData().DataConversionConfig, floatTyp)
			s = string(b)
		case *tree.DInt:
			if typ.Oid() == oid.T_char {
				// int to "char" casts just return the corresponding ASCII byte.
				if *t > math.MaxInt8 || *t < math.MinInt8 {
					return nil, tree.ErrCharOutOfRange
				} else if *t == 0 {
					s = ""
				} else {
					s = string([]byte{byte(*t)})
				}
			} else {
				s = d.String()
			}
		case *tree.DBool, *tree.DDecimal:
			s = d.String()
		case *tree.DTimestamp, *tree.DDate, *tree.DTime, *tree.DTimeTZ, *tree.DGeography, *tree.DGeometry, *tree.DBox2D, *tree.DPGLSN:
			s = tree.AsStringWithFlags(d, tree.FmtBareStrings)
		case *tree.DTimestampTZ:
			// Convert to context timezone for correct display.
			ts, err := tree.MakeDTimestampTZ(t.In(evalCtx.GetLocation()), time.Microsecond)
			if err != nil {
				return nil, err
			}
			s = tree.AsStringWithFlags(
				ts,
				tree.FmtBareStrings,
			)
		case *tree.DTuple:
			s = tree.AsStringWithFlags(
				d,
				tree.FmtPgwireText,
				tree.FmtDataConversionConfig(evalCtx.SessionData().DataConversionConfig),
				tree.FmtLocation(evalCtx.GetLocation()),
			)
		case *tree.DArray:
			s = tree.AsStringWithFlags(
				d,
				tree.FmtPgwireText,
				tree.FmtDataConversionConfig(evalCtx.SessionData().DataConversionConfig),
				tree.FmtLocation(evalCtx.GetLocation()),
			)
		case *tree.DInterval:
			// When converting an interval to string, we need a string representation
			// of the duration (e.g. "5s") and not of the interval itself (e.g.
			// "INTERVAL '5s'").
			s = tree.AsStringWithFlags(
				d,
				tree.FmtPgwireText,
				tree.FmtDataConversionConfig(evalCtx.SessionData().DataConversionConfig),
			)
		case *tree.DUuid:
			s = t.UUID.String()
		case *tree.DIPAddr:
			s = t.IPAddr.String()
			// Ensure the string has a "/mask" suffix.
			if strings.IndexByte(s, '/') == -1 {
				s += "/" + strconv.Itoa(int(t.IPAddr.Mask))
			}
		case *tree.DString:
			s = string(*t)
		case *tree.DCollatedString:
			s = t.Contents
		case *tree.DBytes:
			s = lex.EncodeByteArrayToRawBytes(
				string(*t),
				evalCtx.SessionData().DataConversionConfig.BytesEncodeFormat,
				false, /* skipHexPrefix */
			)
		case *tree.DOid:
			// The "unknown" oid has special handling.
			s = tree.AsStringWithFlags(t, tree.FmtPgwireText)
		case *tree.DJSON:
			s = t.JSON.String()
		case *tree.DJsonpath:
			s = string(*t)
		case *tree.DTSQuery:
			s = t.TSQuery.String()
		case *tree.DTSVector:
			s = t.TSVector.String()
		case *tree.DPGVector:
			s = t.T.String()
		case *tree.DEnum:
			s = t.LogicalRep
		case *tree.DVoid:
			s = ""
		}
		switch t.Family() {
		case types.StringFamily:
			if t.Oid() == oid.T_name {
				return tree.NewDName(s), nil
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
			return tree.NewDString(s), nil
		case types.CollatedStringFamily:
			// bpchar types truncate trailing whitespace.
			if t.Oid() == oid.T_bpchar {
				s = strings.TrimRight(s, " ")
			}
			// Ditto truncation like for TString.
			if truncateWidth && t.Width() > 0 {
				s = util.TruncateString(s, int(t.Width()))
			}
			return tree.NewDCollatedString(s, t.Locale(), &evalCtx.CollationEnv)
		}

	case types.BytesFamily:
		switch t := d.(type) {
		case *tree.DString:
			return tree.ParseDByte(string(*t))
		case *tree.DCollatedString:
			return tree.NewDBytes(tree.DBytes(t.Contents)), nil
		case *tree.DUuid:
			return tree.NewDBytes(tree.DBytes(t.GetBytes())), nil
		case *tree.DBytes:
			return d, nil
		case *tree.DGeography:
			return tree.NewDBytes(tree.DBytes(t.Geography.EWKB())), nil
		case *tree.DGeometry:
			return tree.NewDBytes(tree.DBytes(t.Geometry.EWKB())), nil
		}

	case types.UuidFamily:
		switch t := d.(type) {
		case *tree.DString:
			return tree.ParseDUuidFromString(string(*t))
		case *tree.DCollatedString:
			return tree.ParseDUuidFromString(t.Contents)
		case *tree.DBytes:
			return tree.ParseDUuidFromBytes([]byte(*t))
		case *tree.DUuid:
			return d, nil
		}

	case types.INetFamily:
		switch t := d.(type) {
		case *tree.DString:
			return tree.ParseDIPAddrFromINetString(string(*t))
		case *tree.DCollatedString:
			return tree.ParseDIPAddrFromINetString(t.Contents)
		case *tree.DIPAddr:
			return d, nil
		}

	case types.Box2DFamily:
		switch d := d.(type) {
		case *tree.DString:
			return tree.ParseDBox2D(string(*d))
		case *tree.DCollatedString:
			return tree.ParseDBox2D(d.Contents)
		case *tree.DBox2D:
			return d, nil
		case *tree.DGeometry:
			bbox := d.CartesianBoundingBox()
			if bbox == nil {
				return tree.DNull, nil
			}
			return tree.NewDBox2D(*bbox), nil
		}

	case types.PGLSNFamily:
		switch d := d.(type) {
		case *tree.DString:
			return tree.ParseDPGLSN(string(*d))
		case *tree.DCollatedString:
			return tree.ParseDPGLSN(d.Contents)
		case *tree.DPGLSN:
			return d, nil
		}

	case types.PGVectorFamily:
		switch d := d.(type) {
		case *tree.DString:
			return tree.ParseDPGVector(string(*d))
		case *tree.DCollatedString:
			return tree.ParseDPGVector(d.Contents)
		case *tree.DArray:
			switch d.ParamTyp.Family() {
			case types.FloatFamily, types.IntFamily, types.DecimalFamily:
				v := make(vector.T, len(d.Array))
				for i, elem := range d.Array {
					if elem == tree.DNull {
						return nil, pgerror.Newf(pgcode.NullValueNotAllowed,
							"array must not contain nulls")
					}
					datum, err := performCast(ctx, evalCtx, elem, types.Float4, false)
					if err != nil {
						return nil, err
					}
					v[i] = float32(*datum.(*tree.DFloat))
				}
				return tree.NewDPGVector(v), nil
			}
		case *tree.DPGVector:
			return d, nil
		}

	case types.RefCursorFamily:
		switch d := d.(type) {
		case *tree.DString:
			return tree.NewDRefCursor(string(*d)), nil
		case *tree.DCollatedString:
			return tree.NewDRefCursor(d.Contents), nil
		}

	case types.GeographyFamily:
		switch d := d.(type) {
		case *tree.DString:
			return tree.ParseDGeography(string(*d))
		case *tree.DCollatedString:
			return tree.ParseDGeography(d.Contents)
		case *tree.DGeography:
			if err := geo.SpatialObjectFitsColumnMetadata(
				d.Geography.SpatialObject(),
				t.InternalType.GeoMetadata.SRID,
				t.InternalType.GeoMetadata.ShapeType,
			); err != nil {
				return nil, err
			}
			return d, nil
		case *tree.DGeometry:
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
			return &tree.DGeography{Geography: g}, nil
		case *tree.DJSON:
			t, err := d.AsText()
			if err != nil {
				return nil, err
			}
			if t == nil {
				return tree.DNull, nil
			}
			g, err := geo.ParseGeographyFromGeoJSON(encoding.UnsafeConvertStringToBytes(*t))
			if err != nil {
				return nil, err
			}
			return &tree.DGeography{Geography: g}, nil
		case *tree.DBytes:
			g, err := geo.ParseGeographyFromEWKB(geopb.EWKB(*d))
			if err != nil {
				return nil, err
			}
			return &tree.DGeography{Geography: g}, nil
		}
	case types.GeometryFamily:
		switch d := d.(type) {
		case *tree.DString:
			return tree.ParseDGeometry(string(*d))
		case *tree.DCollatedString:
			return tree.ParseDGeometry(d.Contents)
		case *tree.DGeometry:
			if err := geo.SpatialObjectFitsColumnMetadata(
				d.Geometry.SpatialObject(),
				t.InternalType.GeoMetadata.SRID,
				t.InternalType.GeoMetadata.ShapeType,
			); err != nil {
				return nil, err
			}
			return d, nil
		case *tree.DGeography:
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
			return &tree.DGeometry{Geometry: g}, nil
		case *tree.DJSON:
			t, err := d.AsText()
			if err != nil {
				return nil, err
			}
			if t == nil {
				return tree.DNull, nil
			}
			g, err := geo.ParseGeometryFromGeoJSON(encoding.UnsafeConvertStringToBytes(*t))
			if err != nil {
				return nil, err
			}
			return &tree.DGeometry{Geometry: g}, nil
		case *tree.DBox2D:
			g, err := geo.MakeGeometryFromGeomT(d.ToGeomT(geopb.DefaultGeometrySRID))
			if err != nil {
				return nil, err
			}
			return &tree.DGeometry{Geometry: g}, nil
		case *tree.DBytes:
			g, err := geo.ParseGeometryFromEWKB(geopb.EWKB(*d))
			if err != nil {
				return nil, err
			}
			return &tree.DGeometry{Geometry: g}, nil
		}

	case types.DateFamily:
		switch d := d.(type) {
		case *tree.DString:
			res, _, err := tree.ParseDDate(evalCtx, string(*d))
			return res, err
		case *tree.DCollatedString:
			res, _, err := tree.ParseDDate(evalCtx, d.Contents)
			return res, err
		case *tree.DDate:
			return d, nil
		case *tree.DInt:
			// TODO(mjibson): This cast is unsupported by postgres. Should we remove ours?
			t, err := pgdate.MakeDateFromUnixEpoch(int64(*d))
			return tree.NewDDate(t), err
		case *tree.DTimestampTZ:
			return tree.NewDDateFromTime(d.Time.In(evalCtx.GetLocation()))
		case *tree.DTimestamp:
			return tree.NewDDateFromTime(d.Time)
		}

	case types.TimeFamily:
		roundTo := tree.TimeFamilyPrecisionToRoundDuration(t.Precision())
		switch d := d.(type) {
		case *tree.DString:
			res, _, err := tree.ParseDTime(evalCtx, string(*d), roundTo)
			return res, err
		case *tree.DCollatedString:
			res, _, err := tree.ParseDTime(evalCtx, d.Contents, roundTo)
			return res, err
		case *tree.DTime:
			return d.Round(roundTo), nil
		case *tree.DTimeTZ:
			return tree.MakeDTime(d.TimeOfDay.Round(roundTo)), nil
		case *tree.DTimestamp:
			return tree.MakeDTime(timeofday.FromTime(d.Time).Round(roundTo)), nil
		case *tree.DTimestampTZ:
			// Strip time zone. Times don't carry their location.
			stripped, err := d.EvalAtAndRemoveTimeZone(evalCtx.GetLocation(), time.Microsecond)
			if err != nil {
				return nil, err
			}
			return tree.MakeDTime(timeofday.FromTime(stripped.Time).Round(roundTo)), nil
		case *tree.DInterval:
			return tree.MakeDTime(timeofday.Min.Add(d.Duration).Round(roundTo)), nil
		}

	case types.TimeTZFamily:
		roundTo := tree.TimeFamilyPrecisionToRoundDuration(t.Precision())
		switch d := d.(type) {
		case *tree.DString:
			res, _, err := tree.ParseDTimeTZ(evalCtx, string(*d), roundTo)
			return res, err
		case *tree.DCollatedString:
			res, _, err := tree.ParseDTimeTZ(evalCtx, d.Contents, roundTo)
			return res, err
		case *tree.DTime:
			return tree.NewDTimeTZFromLocation(timeofday.TimeOfDay(*d).Round(roundTo), evalCtx.GetLocation()), nil
		case *tree.DTimeTZ:
			return d.Round(roundTo), nil
		case *tree.DTimestampTZ:
			return tree.NewDTimeTZFromTime(d.Time.In(evalCtx.GetLocation()).Round(roundTo)), nil
		}

	case types.TimestampFamily:
		roundTo := tree.TimeFamilyPrecisionToRoundDuration(t.Precision())
		// TODO(knz): Timestamp from float, decimal.
		switch d := d.(type) {
		case *tree.DString:
			res, _, err := tree.ParseDTimestamp(evalCtx, string(*d), roundTo)
			return res, err
		case *tree.DCollatedString:
			res, _, err := tree.ParseDTimestamp(evalCtx, d.Contents, roundTo)
			return res, err
		case *tree.DDate:
			t, err := d.ToTime()
			if err != nil {
				return nil, err
			}
			return tree.MakeDTimestamp(t, roundTo)
		case *tree.DInt:
			return tree.MakeDTimestamp(timeutil.Unix(int64(*d), 0), roundTo)
		case *tree.DTimestamp:
			return d.Round(roundTo)
		case *tree.DTimestampTZ:
			// Strip time zone. Timestamps don't carry their location.
			return d.EvalAtAndRemoveTimeZone(evalCtx.GetLocation(), roundTo)
		}

	case types.TimestampTZFamily:
		roundTo := tree.TimeFamilyPrecisionToRoundDuration(t.Precision())
		// TODO(knz): TimestampTZ from float, decimal.
		switch d := d.(type) {
		case *tree.DString:
			res, _, err := tree.ParseDTimestampTZ(evalCtx, string(*d), roundTo)
			return res, err
		case *tree.DCollatedString:
			res, _, err := tree.ParseDTimestampTZ(evalCtx, d.Contents, roundTo)
			return res, err
		case *tree.DDate:
			t, err := d.ToTime()
			if err != nil {
				return nil, err
			}
			_, before := t.Zone()
			_, after := t.In(evalCtx.GetLocation()).Zone()
			return tree.MakeDTimestampTZ(t.Add(time.Duration(before-after)*time.Second), roundTo)
		case *tree.DTimestamp:
			return d.AddTimeZone(evalCtx.GetLocation(), roundTo)
		case *tree.DInt:
			return tree.MakeDTimestampTZ(timeutil.Unix(int64(*d), 0), roundTo)
		case *tree.DTimestampTZ:
			return d.Round(roundTo)
		}

	case types.IntervalFamily:
		itm, err := t.IntervalTypeMetadata()
		if err != nil {
			return nil, err
		}
		switch v := d.(type) {
		case *tree.DString:
			return tree.ParseDIntervalWithTypeMetadata(evalCtx.GetIntervalStyle(), string(*v), itm)
		case *tree.DCollatedString:
			return tree.ParseDIntervalWithTypeMetadata(evalCtx.GetIntervalStyle(), v.Contents, itm)
		case *tree.DInt:
			return tree.NewDInterval(duration.FromInt64(int64(*v)), itm), nil
		case *tree.DFloat:
			return tree.NewDInterval(duration.FromFloat64(float64(*v)), itm), nil
		case *tree.DTime:
			return tree.NewDInterval(duration.MakeDuration(int64(*v)*1000, 0, 0), itm), nil
		case *tree.DDecimal:
			var d apd.Decimal
			dnanos := v.Decimal
			dnanos.Exponent += 9
			// We need HighPrecisionCtx because duration values can contain
			// upward of 35 decimal digits and DecimalCtx only provides 25.
			_, err := tree.HighPrecisionCtx.Quantize(&d, &dnanos, 0)
			if err != nil {
				return nil, err
			}
			if dnanos.Negative {
				d.Coeff.Neg(&d.Coeff)
			}
			dv, ok := duration.FromBigInt(&d.Coeff)
			if !ok {
				return nil, tree.ErrDecOutOfRange
			}
			return tree.NewDInterval(dv, itm), nil
		case *tree.DInterval:
			return tree.NewDInterval(v.Duration, itm), nil
		}
	case types.JsonFamily:
		switch v := d.(type) {
		case *tree.DString:
			return tree.ParseDJSON(string(*v))
		case *tree.DJSON:
			return v, nil
		case *tree.DGeography:
			j, err := geo.SpatialObjectToGeoJSON(v.Geography.SpatialObject(), geo.FullPrecisionGeoJSON, geo.SpatialObjectToGeoJSONFlagZero)
			if err != nil {
				return nil, err
			}
			return tree.ParseDJSON(string(j))
		case *tree.DGeometry:
			j, err := geo.SpatialObjectToGeoJSON(v.Geometry.SpatialObject(), geo.FullPrecisionGeoJSON, geo.SpatialObjectToGeoJSONFlagZero)
			if err != nil {
				return nil, err
			}
			return tree.ParseDJSON(string(j))
		}
	case types.JsonpathFamily:
		switch v := d.(type) {
		case *tree.DString:
			return tree.ParseDJsonpath(string(*v))
		}
	case types.TSQueryFamily:
		switch v := d.(type) {
		case *tree.DString:
			q, err := tsearch.ParseTSQuery(string(*v))
			if err != nil {
				return nil, err
			}
			return &tree.DTSQuery{TSQuery: q}, nil
		case *tree.DTSQuery:
			return d, nil
		}
	case types.TSVectorFamily:
		switch v := d.(type) {
		case *tree.DString:
			vec, err := tsearch.ParseTSVector(string(*v))
			if err != nil {
				return nil, err
			}
			return &tree.DTSVector{TSVector: vec}, nil
		case *tree.DTSVector:
			return d, nil
		}
	case types.ArrayFamily:
		switch v := d.(type) {
		case *tree.DString:
			res, _, err := tree.ParseDArrayFromString(evalCtx, string(*v), t.ArrayContents())
			return res, err
		case *tree.DArray:
			dcast := tree.NewDArray(t.ArrayContents())
			if err := dcast.MaybeSetCustomOid(t); err != nil {
				return nil, err
			}
			for _, e := range v.Array {
				ecast := tree.DNull
				if e != tree.DNull {
					var err error
					ecast, err = performCast(ctx, evalCtx, e, t.ArrayContents(), truncateWidth)
					if err != nil {
						return nil, err
					}
				}

				if err := dcast.Append(ecast); err != nil {
					return nil, err
				}
			}
			return dcast, nil
		case *tree.DPGVector:
			dcast := tree.NewDArray(t.ArrayContents())
			for i := range v.T {
				if err := dcast.Append(tree.NewDFloat(tree.DFloat(v.T[i]))); err != nil {
					return nil, err
				}
			}
			return dcast, nil
		}
	case types.OidFamily:
		switch v := d.(type) {
		case *tree.DOid:
			return performIntToOidCast(ctx, evalCtx.Planner, t, tree.DInt(v.Oid))
		case *tree.DInt:
			return performIntToOidCast(ctx, evalCtx.Planner, t, *v)
		case *tree.DString:
			return ParseDOid(ctx, evalCtx, string(*v), t)
		}
	case types.TupleFamily:
		switch v := d.(type) {
		case *tree.DTuple:
			if t.Identical(types.AnyTuple) {
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
			ret := tree.NewDTupleWithLen(t, len(v.D))
			for i := range v.D {
				var err error
				ret.D[i], err = PerformCast(ctx, evalCtx, v.D[i], t.TupleContents()[i])
				if err != nil {
					return nil, err
				}
			}
			return ret, nil
		case *tree.DString:
			res, _, err := tree.ParseDTupleFromString(evalCtx, string(*v), t)
			return res, err
		}
	case types.VoidFamily:
		switch d.(type) {
		case *tree.DString:
			return tree.DVoidDatum, nil
		}
	}

	return nil, pgerror.Newf(
		pgcode.CannotCoerce, "invalid cast: %s -> %s", d.ResolvedType(), t)
}

// performIntToOidCast casts the input integer to the OID type given by the
// input types.T.
func performIntToOidCast(
	ctx context.Context, res Planner, t *types.T, v tree.DInt,
) (tree.Datum, error) {
	if v == 0 {
		// This is the "unknown" oid.
		return tree.NewDOidWithType(tree.UnknownOidValue, t), nil
	}
	// OIDs are always unsigned 32-bit integers. Some languages, like Java,
	// store OIDs as signed 32-bit integers, so we implement the cast
	// by converting to a uint32 first. This matches Postgres behavior.
	o, err := tree.IntToOid(v)
	if err != nil {
		return nil, err
	}
	switch t.Oid() {
	case oid.T_oid:
		return tree.NewDOidWithType(o, t), nil
	case oid.T_regtype:
		// Mapping an dOid to a regtype is easy: we have a hardcoded map.
		var name string
		if typ, ok := types.OidToType[o]; ok {
			name = typ.PGName()
		} else if types.IsOIDUserDefinedType(o) {
			typ, err := res.ResolveTypeByOID(ctx, o)
			if err != nil {
				return nil, err
			}
			name = typ.PGName()
		}
		return tree.NewDOidWithTypeAndName(o, t, name), nil

	case oid.T_regproc, oid.T_regprocedure:
		name, _, err := res.ResolveFunctionByOID(ctx, oid.Oid(v))
		if err != nil {
			if errors.Is(err, tree.ErrRoutineUndefined) {
				return tree.NewDOidWithType(o, t), nil //nolint:returnerrcheck
			}
			return nil, err
		}
		return tree.NewDOidWithTypeAndName(o, t, name.Object()), nil

	default:
		dOid, errSafeToIgnore, err := res.ResolveOIDFromOID(ctx, t, tree.NewDOid(o))
		if err != nil {
			if !errSafeToIgnore {
				return nil, err
			}
			dOid = tree.NewDOidWithType(o, t)
		}
		return dOid, nil
	}
}

func roundDecimalToInt(d *apd.Decimal) (int64, error) {
	var newD apd.Decimal
	if _, err := tree.DecimalCtx.RoundToIntegralValue(&newD, d); err != nil {
		return 0, err
	}
	i, err := newD.Int64()
	if err != nil {
		return 0, tree.ErrIntOutOfRange
	}
	return i, nil
}

func failedCastFromJSON(j *tree.DJSON, t *types.T) error {
	return pgerror.Newf(
		pgcode.InvalidParameterValue,
		"cannot cast jsonb %s to type %s",
		j.Type(), t,
	)
}
