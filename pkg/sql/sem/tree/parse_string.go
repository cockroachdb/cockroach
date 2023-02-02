// Copyright 2018 The Cockroach Authors.
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
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// ParseAndRequireString parses s as type t for simple types. Collated
// strings are not handled.
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
	case types.Box2DFamily:
		d, err = ParseDBox2D(s)
	case types.GeographyFamily:
		d, err = ParseDGeography(s)
	case types.GeometryFamily:
		d, err = ParseDGeometry(s)
	case types.JsonFamily:
		d, err = ParseDJSON(s)
	case types.OidFamily:
		if t.Oid() != oid.T_oid && s == ZeroOidValue {
			d = WrapAsZeroOid(t)
		} else {
			d, err = ParseDOidAsInt(s)
		}
	case types.CollatedStringFamily:
		d, err = NewDCollatedString(s, t.Locale(), ctx.GetCollationEnv())
	case types.StringFamily:
		// If the string type specifies a limit we truncate to that limit:
		//   'hello'::CHAR(2) -> 'he'
		// This is true of all the string type variants.
		if t.Width() > 0 {
			s = util.TruncateString(s, int(t.Width()))
		}
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
		return nil, false, errors.AssertionFailedf("unknown type %s (%T)", t, t)
	}
	if err != nil {
		return d, dependsOnContext, err
	}
	d, err = AdjustValueToType(t, d)
	return d, dependsOnContext, err
}

// ParseDOidAsInt parses the input and returns it as an OID. If the input
// is not formatted as an int, an error is returned.
func ParseDOidAsInt(s string) (*DOid, error) {
	i, err := strconv.ParseInt(strings.TrimSpace(s), 0, 64)
	if err != nil {
		return nil, MakeParseError(s, types.Oid, err)
	}
	return IntToOid(DInt(i))
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
