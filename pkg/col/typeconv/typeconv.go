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
	"github.com/pkg/errors"
)

// TypeFamilyToCanonicalTypeFamily maps all type families that are supported by
// the vectorized engine to their "canonical" counterparts. "Canonical" type
// families are representatives from a set of "equivalent" type families where
// "equivalence" means having the same physical representation.
var TypeFamilyToCanonicalTypeFamily = map[types.Family]types.Family{
	types.BoolFamily:        types.BoolFamily,
	types.BytesFamily:       types.BytesFamily,
	types.DateFamily:        types.IntFamily,
	types.DecimalFamily:     types.DecimalFamily,
	types.IntFamily:         types.IntFamily,
	types.OidFamily:         types.IntFamily,
	types.FloatFamily:       types.FloatFamily,
	types.StringFamily:      types.BytesFamily,
	types.UuidFamily:        types.BytesFamily,
	types.TimestampFamily:   types.TimestampTZFamily,
	types.TimestampTZFamily: types.TimestampTZFamily,
	types.IntervalFamily:    types.IntervalFamily,
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

// AllSupportedSQLTypes is a slice of all SQL types that the vectorized engine
// currently supports.
var AllSupportedSQLTypes = []*types.T{
	types.Bool,
	types.Bytes,
	types.Date,
	types.Decimal,
	types.Int2,
	types.Int4,
	types.Int,
	types.Oid,
	types.Float,
	types.Float4,
	types.String,
	types.Uuid,
	types.Timestamp,
	types.TimestampTZ,
	types.Interval,
}

// IsTypeSupported returns whether t is supported by the vectorized engine.
func IsTypeSupported(t *types.T) bool {
	if _, found := TypeFamilyToCanonicalTypeFamily[t.Family()]; found {
		switch t.Family() {
		case types.IntFamily:
			switch t.Width() {
			// Note that here we do not expect to see an integer type with
			// width 0.
			case 16, 32, 64:
				return true
			}
			panic(fmt.Sprintf("integer with unknown width %d", t.Width()))
		default:
			return true
		}
	}
	return false
}

// AreTypesSupported checks whether all types in typs are supported by the
// vectorized engine and returns an error if they are not.
func AreTypesSupported(typs []*types.T) error {
	for _, t := range typs {
		if !IsTypeSupported(t) {
			return errors.Errorf("unsupported type %s", t)
		}
	}
	return nil
}

// UnsafeFromGoType returns the type for a Go value, if applicable. Shouldn't
// be used at runtime. This method is unsafe because multiple logical types can
// be represented by the same physical type.
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
