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

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

// TypeFamilyToCanonicalTypeFamily maps all type families that are supported by
// the vectorized engine to their "canonical" counterparts. "Canonical" type
// families are representatives from a set of "equivalent" type families where
// "equivalence" means having the same physical representation.
//
// All type families that do not have an optimized physical representation are
// handled by using tree.Datums, and such types are mapped to types.AnyFamily.
var TypeFamilyToCanonicalTypeFamily = map[types.Family]types.Family{
	types.BoolFamily:        types.BoolFamily,
	types.IntFamily:         types.IntFamily,
	types.FloatFamily:       types.FloatFamily,
	types.DecimalFamily:     types.DecimalFamily,
	types.DateFamily:        types.IntFamily,
	types.TimestampFamily:   types.TimestampTZFamily,
	types.IntervalFamily:    types.IntervalFamily,
	types.StringFamily:      types.BytesFamily,
	types.BytesFamily:       types.BytesFamily,
	types.TimestampTZFamily: types.TimestampTZFamily,
	types.OidFamily:         types.IntFamily,
	types.UuidFamily:        types.BytesFamily,
	// Types backed by tree.Datums.
	types.CollatedStringFamily: types.AnyFamily,
	// TODO(yuzefovich): consider adding explicit support for
	// types.UnknownFamily.
	types.UnknownFamily:   types.AnyFamily,
	types.ArrayFamily:     types.AnyFamily,
	types.INetFamily:      types.AnyFamily,
	types.TimeFamily:      types.AnyFamily,
	types.JsonFamily:      types.AnyFamily,
	types.TimeTZFamily:    types.AnyFamily,
	types.TupleFamily:     types.AnyFamily,
	types.BitFamily:       types.AnyFamily,
	types.GeometryFamily:  types.AnyFamily,
	types.GeographyFamily: types.AnyFamily,
	types.EnumFamily:      types.AnyFamily,
}

// ToCanonicalTypeFamilies converts typs to the corresponding canonical type
// families.
func ToCanonicalTypeFamilies(typs []*types.T) []types.Family {
	families := make([]types.Family, len(typs))
	for i := range typs {
		families[i] = TypeFamilyToCanonicalTypeFamily[typs[i].Family()]
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
	default:
		panic(fmt.Sprintf("type %s not supported yet", t))
	}
}
