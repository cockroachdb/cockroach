// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package tree

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// ParseStringAs reads s as type t. If t is Bytes or String, s is returned
// unchanged. Otherwise s is parsed with the given type's Parse func.
func ParseStringAs(t *types.T, s string, evalCtx *EvalContext) (Datum, error) {
	var d Datum
	var err error
	switch t.Family() {
	case types.BytesFamily:
		d = NewDBytes(DBytes(s))
	case types.CollatedStringFamily:
		d = NewDCollatedString(s, t.Locale(), &evalCtx.CollationEnv)
	case types.ArrayFamily:
		d, err = ParseDArrayFromString(evalCtx, s, t.ArrayContents())
		if err != nil {
			return nil, err
		}
	default:
		d, err = parseStringAs(t, s, evalCtx)
		if d == nil && err == nil {
			return nil, errors.AssertionFailedf("unknown type %s (%T)", t, t)
		}
	}
	return d, err
}

// ParseDatumStringAs parses s as type t. This function is guaranteed to
// round-trip when printing a Datum with FmtExport.
func ParseDatumStringAs(t *types.T, s string, evalCtx *EvalContext) (Datum, error) {
	switch t.Family() {
	case types.BytesFamily:
		return ParseDByte(s)
	default:
		return ParseStringAs(t, s, evalCtx)
	}
}

// parseStringAs parses s as type t for simple types. Bytes, arrays, collated
// strings are not handled. nil, nil is returned if t is not a supported type.
func parseStringAs(t *types.T, s string, ctx ParseTimeContext) (Datum, error) {
	switch t.Family() {
	case types.BitFamily:
		return ParseDBitArray(s)
	case types.BoolFamily:
		return ParseDBool(s)
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
		return ParseDInterval(s)
	case types.JsonFamily:
		return ParseDJSON(s)
	case types.StringFamily:
		return NewDString(s), nil
	case types.TimeFamily:
		return ParseDTime(ctx, s)
	case types.TimestampFamily:
		if t.Precision() == 0 {
			return ParseDTimestamp(ctx, s, time.Second)
		}
		return ParseDTimestamp(ctx, s, time.Microsecond)
	case types.TimestampTZFamily:
		if t.Precision() == 0 {
			return ParseDTimestampTZ(ctx, s, time.Second)
		}
		return ParseDTimestampTZ(ctx, s, time.Microsecond)
	case types.UuidFamily:
		return ParseDUuidFromString(s)
	default:
		return nil, nil
	}
}
