// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package typeconv

import (
	"fmt"
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// DatumVecCanonicalTypeFamily is the "canonical" type family of all types that
// are physically represented by coldata.DatumVec.
var DatumVecCanonicalTypeFamily = types.Family(1000000)

// TypeFamilyToCanonicalTypeFamily converts all type families to their
// "canonical" counterparts. "Canonical" type families are representatives
// from a set of "equivalent" type families where "equivalence" means having
// the same physical representation.
//
// All type families that do not have an optimized physical representation are
// handled by using tree.Datums, and such types are mapped to
// DatumVecCanonicalTypeFamily.
func TypeFamilyToCanonicalTypeFamily(family types.Family) types.Family {
	switch family {
	case types.BoolFamily:
		return types.BoolFamily
	case types.BytesFamily, types.StringFamily, types.UuidFamily:
		return types.BytesFamily
	case types.DecimalFamily:
		return types.DecimalFamily
	case types.JsonFamily:
		return types.JsonFamily
	case types.IntFamily, types.DateFamily:
		return types.IntFamily
	case types.FloatFamily:
		return types.FloatFamily
	case types.TimestampTZFamily, types.TimestampFamily:
		return types.TimestampTZFamily
	case types.IntervalFamily:
		return types.IntervalFamily
	default:
		// TODO(yuzefovich): consider adding native support for
		// types.UnknownFamily.
		return DatumVecCanonicalTypeFamily
	}
}

// ToCanonicalTypeFamilies converts typs to the corresponding canonical type
// families.
func ToCanonicalTypeFamilies(typs []*types.T) []types.Family {
	families := make([]types.Family, len(typs))
	for i := range typs {
		families[i] = TypeFamilyToCanonicalTypeFamily(typs[i].Family())
	}
	return families
}

// UnsafeFromGoType returns the type for a Go value, if applicable. Shouldn't
// be used at runtime. This method is unsafe because multiple logical types can
// be represented by the same physical type. Types that are backed by DatumVec
// are *not* supported by this function.
func UnsafeFromGoType(v interface{}) *types.T {
	switch t := v.(type) {
	case int16:
		return types.Int2
	case int32:
		return types.Int4
	case int, int64:
		return types.Int
	case bool:
		return types.Bool
	case float64:
		return types.Float
	case []byte:
		return types.Bytes
	case string:
		return types.String
	case apd.Decimal:
		return types.Decimal
	case time.Time:
		return types.TimestampTZ
	case duration.Duration:
		return types.Interval
	case json.JSON:
		return types.Jsonb
	default:
		panic(fmt.Sprintf("type %s not supported yet", t))
	}
}
