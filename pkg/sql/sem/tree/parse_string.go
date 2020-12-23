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
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// ParseAndRequireString parses s as type t for simple types. Collated
// strings are not handled.
//
// The dependsOnContext return value indicates if we had to consult the
// ParseTimeContext (either for the time or the local timezone).
func ParseAndRequireString(
	t *types.T, s string, ctx ParseTimeContext,
) (d Datum, dependsOnContext bool, err error) {
	switch t.Family() {
	case types.ArrayFamily:
		d, dependsOnContext, err = ParseDArrayFromString(ctx, s, t.ArrayContents())
	case types.BitFamily:
		r, err := ParseDBitArray(s)
		if err != nil {
			return nil, false, err
		}
		d = formatBitArrayToType(r, t)
	case types.BoolFamily:
		d, err = ParseDBool(s)
	case types.BytesFamily:
		d, err = ParseDByte(s)
	case types.DateFamily:
		d, dependsOnContext, err = ParseDDate(ctx, s)
	case types.DecimalFamily:
		d, err = ParseDDecimal(s)
	case types.FloatFamily:
		d, err = ParseDFloat(s)
	case types.INetFamily:
		d, err = ParseDIPAddrFromINetString(s)
	case types.IntFamily:
		d, err = ParseDInt(s)
	case types.IntervalFamily:
		itm, typErr := t.IntervalTypeMetadata()
		if typErr != nil {
			return nil, false, typErr
		}
		d, err = ParseDIntervalWithTypeMetadata(s, itm)
	case types.Box2DFamily:
		d, err = ParseDBox2D(s)
	case types.GeographyFamily:
		d, err = ParseDGeography(s)
	case types.GeometryFamily:
		d, err = ParseDGeometry(s)
	case types.JsonFamily:
		d, err = ParseDJSON(s)
	case types.OidFamily:
		i, err := ParseDInt(s)
		if err != nil {
			return nil, false, err
		}
		d = NewDOid(*i)
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
		d, err = MakeDEnumFromLogicalRepresentation(t, s)
	default:
		return nil, false, errors.AssertionFailedf("unknown type %s (%T)", t, t)
	}
	if err != nil {
		return d, dependsOnContext, err
	}
	d, err = AdjustValueToType(t, d)
	return d, dependsOnContext, err
}
