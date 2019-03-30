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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// ParseStringAs reads s as type t. If t is Bytes or String, s is returned
// unchanged. Otherwise s is parsed with the given type's Parse func.
func ParseStringAs(t *types.T, s string, evalCtx *EvalContext) (Datum, error) {
	var d Datum
	var err error
	switch t.SemanticType {
	case types.BYTES:
		d = NewDBytes(DBytes(s))
	case types.COLLATEDSTRING:
		d = NewDCollatedString(s, *t.Locale, &evalCtx.CollationEnv)
	case types.ARRAY:
		d, err = ParseDArrayFromString(evalCtx, s, t.ArrayContents)
		if err != nil {
			return nil, err
		}
	default:
		d, err = parseStringAs(t, s, evalCtx)
		if d == nil && err == nil {
			return nil, pgerror.NewAssertionErrorf("unknown type %s (%T)", t, t)
		}
	}
	return d, err
}

// ParseDatumStringAs parses s as type t. This function is guaranteed to
// round-trip when printing a Datum with FmtExport.
func ParseDatumStringAs(t *types.T, s string, evalCtx *EvalContext) (Datum, error) {
	switch t.SemanticType {
	case types.BYTES:
		return ParseDByte(s)
	default:
		return ParseStringAs(t, s, evalCtx)
	}
}

// parseStringAs parses s as type t for simple types. Bytes, arrays, collated
// strings are not handled. nil, nil is returned if t is not a supported type.
func parseStringAs(t *types.T, s string, ctx ParseTimeContext) (Datum, error) {
	switch t.SemanticType {
	case types.BIT:
		return ParseDBitArray(s)
	case types.BOOL:
		return ParseDBool(s)
	case types.DATE:
		return ParseDDate(ctx, s)
	case types.DECIMAL:
		return ParseDDecimal(s)
	case types.FLOAT:
		return ParseDFloat(s)
	case types.INET:
		return ParseDIPAddrFromINetString(s)
	case types.INT:
		return ParseDInt(s)
	case types.INTERVAL:
		return ParseDInterval(s)
	case types.JSON:
		return ParseDJSON(s)
	case types.STRING:
		return NewDString(s), nil
	case types.TIME:
		return ParseDTime(ctx, s)
	case types.TIMESTAMP:
		return ParseDTimestamp(ctx, s, time.Microsecond)
	case types.TIMESTAMPTZ:
		return ParseDTimestampTZ(ctx, s, time.Microsecond)
	case types.UUID:
		return ParseDUuidFromString(s)
	default:
		return nil, nil
	}
}
