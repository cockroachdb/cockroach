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
	"github.com/cockroachdb/errors"
)

// ParseStringAsWithRawBytes reads s as type t. If t is Bytes or String, s is returned
// unchanged. Otherwise s is parsed with the given type's Parse func. This function is
// used when the input string might be unescaped raw bytes, so we don't want to run
// a bytes parsing routine on that input.
func ParseStringAsWithRawBytes(t *types.T, s string, evalCtx *EvalContext) (Datum, error) {
	var d Datum
	var err error
	switch t.Family() {
	case types.BytesFamily:
		d = NewDBytes(DBytes(s))
	case types.CollatedStringFamily:
		d, err = NewDCollatedString(s, t.Locale(), &evalCtx.CollationEnv)
	case types.ArrayFamily:
		d, err = ParseDArrayFromString(evalCtx, s, t.ArrayContents())
		if err != nil {
			return nil, err
		}
	default:
		d, err = ParseAndRequireString(t, s, evalCtx)
	}
	return d, err
}

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
