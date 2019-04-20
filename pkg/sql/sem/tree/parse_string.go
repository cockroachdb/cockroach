// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
			return nil, pgerror.AssertionFailedf("unknown type %s (%T)", t, t)
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
		return ParseDTimestamp(ctx, s, time.Microsecond)
	case types.TimestampTZFamily:
		return ParseDTimestampTZ(ctx, s, time.Microsecond)
	case types.UuidFamily:
		return ParseDUuidFromString(s)
	default:
		return nil, nil
	}
}
