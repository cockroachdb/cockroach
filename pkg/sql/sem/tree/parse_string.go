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

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// ParseStringAs reads s as type t. If t is Bytes or String, s is returned
// unchanged. Otherwise s is parsed with the given type's Parse func.
func ParseStringAs(t types.T, s string, evalCtx *EvalContext) (Datum, error) {
	var d Datum
	var err error
	switch t {
	case types.Bytes:
		d = NewDBytes(DBytes(s))
	default:
		switch t := t.(type) {
		case types.TArray:
			typ, err := coltypes.DatumTypeToColumnType(t.Typ)
			if err != nil {
				return nil, err
			}
			d, err = ParseDArrayFromString(evalCtx, s, typ)
			if err != nil {
				return nil, err
			}
		case types.TCollatedString:
			d = NewDCollatedString(s, t.Locale, &evalCtx.collationEnv)
		default:
			d, err = parseStringAs(t, s, evalCtx)
			if d == nil && err == nil {
				return nil, pgerror.NewAssertionErrorf("unknown type %s (%T)", t, t)
			}
		}
	}
	return d, err
}

// ParseDatumStringAs parses s as type t. This function is guaranteed to
// round-trip when printing a Datum with FmtParseDatums.
func ParseDatumStringAs(t types.T, s string, evalCtx *EvalContext) (Datum, error) {
	switch t {
	case types.Bytes:
		return ParseDByte(s)
	default:
		return ParseStringAs(t, s, evalCtx)
	}
}

type locationContext interface {
	GetLocation() *time.Location
}

var _ locationContext = &EvalContext{}
var _ locationContext = &SemaContext{}

// parseStringAs parses s as type t for simple types. Bytes, arrays, collated
// strings are not handled. nil, nil is returned if t is not a supported type.
func parseStringAs(t types.T, s string, loc locationContext) (Datum, error) {
	switch t {
	case types.BitArray:
		return ParseDBitArray(s)
	case types.Bool:
		return ParseDBool(s)
	case types.Date:
		return ParseDDate(s, loc.GetLocation())
	case types.Decimal:
		return ParseDDecimal(s)
	case types.Float:
		return ParseDFloat(s)
	case types.INet:
		return ParseDIPAddrFromINetString(s)
	case types.Int:
		return ParseDInt(s)
	case types.Interval:
		return ParseDInterval(s)
	case types.JSON:
		return ParseDJSON(s)
	case types.String:
		return NewDString(s), nil
	case types.Time:
		return ParseDTime(s)
	case types.Timestamp:
		eCtx, _ := loc.(*EvalContext)
		return ParseDTimestamp(eCtx, s, time.Microsecond)
	case types.TimestampTZ:
		return ParseDTimestampTZ(s, loc.GetLocation(), time.Microsecond)
	case types.UUID:
		return ParseDUuidFromString(s)
	default:
		return nil, nil
	}
}
