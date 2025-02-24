// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// ParseAndRequireString parses s as type t for simple types.
//
// The dependsOnContext return value indicates if we had to consult the
// ParseContext (either for the time or the local timezone).
func ParseAndRequireString(
	t *types.T, s string, ctx ParseContext,
) (d Datum, dependsOnContext bool, err error) {
	switch t.Family() {
	case types.ArrayFamily:
		d, dependsOnContext, err = ParseDArrayFromString(ctx, s, t.ArrayContents())
	case types.BitFamily:
		r, err := ParseDBitArray(s)
		if err != nil {
			return nil, false, err
		}
		d = FormatBitArrayToType(r, t)
	case types.BoolFamily:
		d, err = ParseDBool(strings.TrimSpace(s))
	case types.BytesFamily:
		d, err = ParseDByte(s)
	case types.DateFamily:
		d, dependsOnContext, err = ParseDDate(ctx, s)
	case types.DecimalFamily:
		d, err = ParseDDecimal(strings.TrimSpace(s))
	case types.FloatFamily:
		d, err = ParseDFloat(strings.TrimSpace(s))
	case types.INetFamily:
		d, err = ParseDIPAddrFromINetString(s)
	case types.IntFamily:
		d, err = ParseDInt(strings.TrimSpace(s))
	case types.IntervalFamily:
		itm, typErr := t.IntervalTypeMetadata()
		if typErr != nil {
			return nil, false, typErr
		}
		d, err = ParseDIntervalWithTypeMetadata(intervalStyle(ctx), s, itm)
	case types.PGLSNFamily:
		d, err = ParseDPGLSN(s)
	case types.PGVectorFamily:
		d, err = ParseDPGVector(s)
	case types.RefCursorFamily:
		d = NewDRefCursor(s)
	case types.Box2DFamily:
		d, err = ParseDBox2D(s)
	case types.GeographyFamily:
		d, err = ParseDGeography(s)
	case types.GeometryFamily:
		d, err = ParseDGeometry(s)
	case types.JsonFamily:
		d, err = ParseDJSON(s)
	case types.JsonpathFamily:
		d, err = ParseDJsonpath(s)
	case types.OidFamily:
		if t.Oid() != oid.T_oid && s == UnknownOidName {
			d = NewDOidWithType(UnknownOidValue, t)
		} else {
			d, err = ParseDOidAsInt(s)
		}
	case types.CollatedStringFamily:
		d, err = NewDCollatedString(s, t.Locale(), ctx.GetCollationEnv())
	case types.StringFamily:
		s = truncateString(s, t)
		return NewDString(s), false, nil
	case types.TimeFamily:
		d, dependsOnContext, err = ParseDTime(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.TimeTZFamily:
		d, dependsOnContext, err = ParseDTimeTZ(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.TimestampFamily:
		d, dependsOnContext, err = ParseDTimestamp(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.TimestampTZFamily:
		d, dependsOnContext, err = ParseDTimestampTZ(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.UuidFamily:
		d, err = ParseDUuidFromString(s)
	case types.EnumFamily:
		var e DEnum
		e, err = MakeDEnumFromLogicalRepresentation(t, s)
		if err == nil {
			d = NewDEnum(e)
		}
	case types.TSQueryFamily:
		d, err = ParseDTSQuery(s)
	case types.TSVectorFamily:
		d, err = ParseDTSVector(s)
	case types.TupleFamily:
		d, dependsOnContext, err = ParseDTupleFromString(ctx, s, t)
	case types.VoidFamily:
		d = DVoidDatum
	default:
		return nil, false, errors.AssertionFailedf("unknown type %s", t.SQLStringForError())
	}
	if err != nil {
		return d, dependsOnContext, err
	}
	d, err = AdjustValueToType(t, d)
	return d, dependsOnContext, err
}

func truncateString(s string, t *types.T) string {
	// If the string type specifies a limit we truncate to that limit:
	//   'hello'::CHAR(2) -> 'he'
	// This is true of all the string type variants.
	if t.Width() > 0 {
		s = util.TruncateString(s, int(t.Width()))
	}
	return s
}

// ParseDOidAsInt parses the input and returns it as an OID. If the input
// is not formatted as an int, an error is returned.
func ParseDOidAsInt(s string) (*DOid, error) {
	i, err := strconv.ParseInt(strings.TrimSpace(s), 0, 64)
	if err != nil {
		return nil, MakeParseError(s, types.Oid, err)
	}
	o, err := IntToOid(DInt(i))
	return NewDOid(o), err
}

// FormatBitArrayToType formats bit arrays such that they fill the total width
// if too short, or truncate if too long.
func FormatBitArrayToType(d *DBitArray, t *types.T) *DBitArray {
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

// ValueHandler is an interface to allow raw types to be extracted from strings.
type ValueHandler interface {
	Len() int
	Null()
	Date(d pgdate.Date)
	Datum(d Datum)
	Bool(b bool)
	Bytes(b []byte)
	// Decimal returns a pointer into the vec for in place construction.
	Decimal() *apd.Decimal
	Float(f float64)
	Int16(i int16)
	Int32(i int32)
	Int(i int64)
	Duration(d duration.Duration)
	JSON(j json.JSON)
	String(s string)
	TimestampTZ(t time.Time)
	Reset()
}

// ParseAndRequireStringHandler parses a string and passes values
// supported by the vector engine directly to a ValueHandler. Other types are
// handled by ParseAndRequireString.
func ParseAndRequireStringHandler(
	t *types.T, s string, ctx ParseContext, vh ValueHandler, ph *pgdate.ParseHelper,
) (err error) {
	switch t.Family() {
	case types.BoolFamily:
		var b bool
		if b, err = ParseBool(strings.TrimSpace(s)); err == nil {
			vh.Bool(b)
		}
	case types.BytesFamily:
		var res []byte
		if res, err = lex.DecodeRawBytesToByteArrayAuto(encoding.UnsafeConvertStringToBytes(s)); err == nil {
			vh.Bytes(res)
		} else {
			err = MakeParseError(s, types.Bytes, err)
		}
	case types.DateFamily:
		now := relativeParseTime(ctx)
		var t pgdate.Date
		if t, _, err = pgdate.ParseDate(now, dateStyle(ctx), s, ph); err == nil {
			vh.Date(t)
		}
	case types.DecimalFamily:
		// Decimal is a little different to allow in place construction.
		dec := vh.Decimal()
		if err = setDecimalString(s, dec); err != nil {
			// Erase any invalid results.
			*dec = apd.Decimal{}
		}
	case types.FloatFamily:
		var f float64
		if f, err = strconv.ParseFloat(s, 64); err == nil {
			vh.Float(f)
		} else {
			err = MakeParseError(s, types.Float, err)
		}
	case types.IntFamily:
		var i int64
		switch t.Width() {
		case 16:
			if i, err = strconv.ParseInt(s, 0, 16); err == nil {
				vh.Int16(int16(i))
			} else {
				err = MakeParseError(s, t, err)
			}
		case 32:
			if i, err = strconv.ParseInt(s, 0, 32); err == nil {
				vh.Int32(int32(i))
			} else {
				err = MakeParseError(s, t, err)
			}
		default:
			if i, err = strconv.ParseInt(s, 0, 64); err == nil {
				vh.Int(i)
			} else {
				err = MakeParseError(s, t, err)
			}
		}
	case types.JsonFamily:
		var j json.JSON
		if j, err = json.ParseJSON(s); err == nil {
			vh.JSON(j)
		} else {
			err = pgerror.Wrapf(err, pgcode.Syntax, "could not parse JSON")
		}
	case types.StringFamily:
		s = truncateString(s, t)
		vh.String(s)
	case types.TimestampTZFamily:
		var ts time.Time
		if ts, _, err = ParseTimestampTZ(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision())); err == nil {
			vh.TimestampTZ(ts)
		}
	case types.TimestampFamily:
		// TODO(yuzefovich): can we refactor the next 2 case arms to be simpler
		// and avoid code duplication?
		now := relativeParseTime(ctx)
		var ts time.Time
		if ts, _, err = pgdate.ParseTimestampWithoutTimezone(now, dateStyle(ctx), s, dateParseHelper(ctx)); err == nil {
			// Always normalize time to the current location.
			if ts, err = roundAndCheck(ts, TimeFamilyPrecisionToRoundDuration(t.Precision())); err == nil {
				vh.TimestampTZ(ts)
			}
		}
	case types.IntervalFamily:
		var itm types.IntervalTypeMetadata
		itm, err = t.IntervalTypeMetadata()
		if err == nil {
			var d duration.Duration
			d, err = ParseIntervalWithTypeMetadata(intervalStyle(ctx), s, itm)
			if err == nil {
				vh.Duration(d)
			}
		}
	case types.UuidFamily:
		var uv uuid.UUID
		uv, err = uuid.FromString(s)
		if err == nil {
			vh.Bytes(uv.GetBytes())
		} else {
			err = MakeParseError(s, types.Uuid, err)
		}
	case types.EnumFamily:
		var d DEnum
		d, err = MakeDEnumFromLogicalRepresentation(t, s)
		if err == nil {
			vh.Bytes(d.PhysicalRep)
		}
	default:
		if typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) != typeconv.DatumVecCanonicalTypeFamily {
			return errors.AssertionFailedf("unexpected type %v in datum case arm, does a new type need to be handled?", t)
		}
		var d Datum
		if d, _, err = ParseAndRequireString(t, s, ctx); err == nil {
			vh.Datum(d)
		}
	}
	return err
}
