// Copyright 2018 The Cockroach Authors.
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

// AllSupportedSQLTypes is a slice of all SQL types that the vectorized engine
// currently supports. It should be kept in sync with IsSupported().
var AllSupportedSQLTypes = []types.T{
	*types.Bool,
	*types.Bytes,
	*types.Date,
	*types.Decimal,
	*types.Int2,
	*types.Int4,
	*types.Int,
	*types.Oid,
	*types.Float,
	*types.Float4,
	*types.String,
	*types.Uuid,
	*types.Timestamp,
	*types.TimestampTZ,
	*types.Interval,
}

// TypeFamilyToCanonicalTypeFamily maps all type families that are supported by
// the vectorized engine to their "canonical equivalents".
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

// IsSupported returns whether t is supported by the vectorized engine.
// Note: if you're adding a new type here, add it to AllSupportedSQLTypes as
// well.
func IsSupported(t *types.T) bool {
	switch t.Family() {
	case types.BoolFamily:
		return true
	case types.BytesFamily, types.StringFamily, types.UuidFamily:
		return true
	case types.DateFamily, types.OidFamily:
		return true
	case types.DecimalFamily:
		return true
	case types.IntFamily:
		switch t.Width() {
		case 16:
			return true
		case 32:
			return true
		case 64:
			return true
		}
		panic(fmt.Sprintf("integer with unknown width %d", t.Width()))
	case types.FloatFamily:
		return true
	case types.TimestampTZFamily, types.TimestampFamily:
		return true
	case types.IntervalFamily:
		return true
	}
	return false
}

// AreSupported checks whether all types in typs are supported by the
// vectorized engine and returns an error if they are not.
func AreSupported(typs []types.T) error {
	for i := range typs {
		if !IsSupported(&typs[i]) {
			return errors.Errorf("unsupported type %s", typs[i].String())
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
