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

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/pkg/errors"
)

// AllSupportedSQLTypes is a slice of all SQL types that the vectorized engine
// currently supports. It should be kept in sync with FromColumnType().
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

// FromColumnType returns the T that corresponds to the input ColumnType.
// Note: if you're adding a new type here, add it to AllSupportedSQLTypes as
// well.
func FromColumnType(ct *types.T) coltypes.T {
	switch ct.Family() {
	case types.BoolFamily:
		return coltypes.Bool
	case types.BytesFamily, types.StringFamily, types.UuidFamily:
		return coltypes.Bytes
	case types.DateFamily, types.OidFamily:
		return coltypes.Int64
	case types.DecimalFamily:
		return coltypes.Decimal
	case types.IntFamily:
		switch ct.Width() {
		case 16:
			return coltypes.Int16
		case 32:
			return coltypes.Int32
		case 0, 64:
			return coltypes.Int64
		}
		panic(fmt.Sprintf("integer with unknown width %d", ct.Width()))
	case types.FloatFamily:
		return coltypes.Float64
	case types.TimestampFamily:
		return coltypes.Timestamp
	case types.TimestampTZFamily:
		return coltypes.Timestamp
	case types.IntervalFamily:
		return coltypes.Interval
	}
	return coltypes.Unhandled
}

// FromColumnTypes calls FromColumnType on each element of cts, returning the
// resulting slice.
func FromColumnTypes(cts []types.T) ([]coltypes.T, error) {
	typs := make([]coltypes.T, len(cts))
	for i := range typs {
		typs[i] = FromColumnType(&cts[i])
		if typs[i] == coltypes.Unhandled {
			return nil, errors.Errorf("unsupported type %s", cts[i].String())
		}
	}
	return typs, nil
}

// UnsafeToSQLType converts the given coltype to the logical SQL type. Note
// that this conversion is lossful since multiple logical types can map to a
// single coltype, so use this method *only* when such behavior is acceptable.
func UnsafeToSQLType(t coltypes.T) (*types.T, error) {
	switch t {
	case coltypes.Bool:
		return types.Bool, nil
	case coltypes.Bytes:
		return types.Bytes, nil
	case coltypes.Decimal:
		return types.Decimal, nil
	case coltypes.Int16:
		return types.Int2, nil
	case coltypes.Int32:
		return types.Int4, nil
	case coltypes.Int64:
		return types.Int, nil
	case coltypes.Float64:
		return types.Float, nil
	case coltypes.Timestamp:
		return types.Timestamp, nil
	case coltypes.Interval:
		return types.Interval, nil
	default:
		return nil, errors.Errorf("unsupported coltype %s", t)
	}
}
