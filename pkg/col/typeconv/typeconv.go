// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package typeconv

import (
	"fmt"
	"time"

	"github.com/cockroachdb/apd/v3"
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
	case types.BytesFamily, types.StringFamily, types.UuidFamily, types.EncodedKeyFamily, types.EnumFamily:
		// Note that by using Bytes family as the canonical one for other type
		// families we allow the execution engine to evaluate invalid operations
		// (e.g. the concat binary operation between a UUID and an enum "has"
		// the execution engine support). However, it's not a big deal since the
		// type-checking for validity of operations is done before the query
		// reaches the execution engine.
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

// TypesSupportedNatively contains types that are supported natively by the
// vectorized engine.
var TypesSupportedNatively []*types.T

func init() {
	for _, t := range types.Scalar {
		if TypeFamilyToCanonicalTypeFamily(t.Family()) == DatumVecCanonicalTypeFamily {
			continue
		}
		if t.Family() == types.IntFamily {
			TypesSupportedNatively = append(TypesSupportedNatively, types.Int2)
			TypesSupportedNatively = append(TypesSupportedNatively, types.Int4)
		}
		TypesSupportedNatively = append(TypesSupportedNatively, t)
	}
}
