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

// ParseAndRequireString parses s as type t for simple types. Arrays and collated
// strings are not handled.
func ParseAndRequireString(t *types.T, s string, ctx ParseTimeContext) (Datum, error) {
	switch t.Family() {
	case types.ArrayFamily:
		return ParseDArrayFromString(ctx, s, t.ArrayContents())
	case types.BitFamily:
		return ParseDBitArray(s)
	case types.BoolFamily:
		return ParseDBool(s)
	case types.BytesFamily:
		return ParseDByte(s)
	case types.DateFamily:
		return ParseDDate(ctx, s)
	case types.DecimalFamily:
		return ParseDDecimal(s)
	case types.FloatFamily:
		return ParseDFloat(s)
	case types.INetFamily:
		return ParseDIPAddrFromINetString(s)
	case types.IntFamily:
		return ParseDInt(s)
	case types.IntervalFamily:
		itm, err := t.IntervalTypeMetadata()
		if err != nil {
			return nil, err
		}
		return ParseDIntervalWithTypeMetadata(s, itm)
	case types.JsonFamily:
		return ParseDJSON(s)
	case types.OidFamily:
		i, err := ParseDInt(s)
		return NewDOid(*i), err
	case types.StringFamily:
		// If the string type specifies a limit we truncate to that limit:
		//   'hello'::CHAR(2) -> 'he'
		// This is true of all the string type variants.
		if t.Width() > 0 {
			s = util.TruncateString(s, int(t.Width()))
		}
		return NewDString(s), nil
	case types.TimeFamily:
		return ParseDTime(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.TimeTZFamily:
		return ParseDTimeTZ(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.TimestampFamily:
		return ParseDTimestamp(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.TimestampTZFamily:
		return ParseDTimestampTZ(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.UuidFamily:
		return ParseDUuidFromString(s)
	default:
		return nil, errors.AssertionFailedf("unknown type %s (%T)", t, t)
	}
}
