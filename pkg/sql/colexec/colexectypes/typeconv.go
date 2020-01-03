// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexectypes

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/col/phystypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

// TODO(yuzefovich): we might want to avoid allocating a new struct on every
// conversion.

// FromColumnType returns the T that corresponds to the input ColumnType.
// Note: if you're adding a new type here, add it to
// colexec.AllSupportedSQLTypes as well.
func FromColumnType(ct *types.T) T {
	switch ct.Family() {
	case types.BoolFamily:
		return T{T: phystypes.Bool, LogicalType: ct}
	case types.BytesFamily, types.StringFamily, types.UuidFamily:
		return T{T: phystypes.Bytes, LogicalType: ct}
	case types.DateFamily, types.OidFamily:
		return T{T: phystypes.Int64, LogicalType: ct}
	case types.DecimalFamily:
		return T{T: phystypes.Decimal, LogicalType: ct}
	case types.IntFamily:
		switch ct.Width() {
		case 16:
			return T{T: phystypes.Int16, LogicalType: ct}
		case 32:
			return T{T: phystypes.Int32, LogicalType: ct}
		case 0, 64:
			return T{T: phystypes.Int64, LogicalType: ct}
		}
		execerror.VectorizedInternalPanic(fmt.Sprintf("integer with unknown width %d", ct.Width()))
	case types.FloatFamily:
		return T{T: phystypes.Float64, LogicalType: ct}
	case types.TimestampFamily:
		return T{T: phystypes.Timestamp, LogicalType: ct}
	case types.TimestampTZFamily:
		return T{T: phystypes.Timestamp, LogicalType: ct}
	}
	return T{T: phystypes.Unhandled, LogicalType: ct}
}

// FromColumnTypes calls FromColumnType on each element of cts, returning the
// resulting slice.
func FromColumnTypes(cts []types.T) ([]T, error) {
	typs := make([]T, len(cts))
	for i := range typs {
		typs[i] = FromColumnType(&cts[i])
		if typs[i].T == phystypes.Unhandled {
			return nil, errors.Errorf("unsupported type %s", cts[i].String())
		}
	}
	return typs, nil
}

// AsPhysTypes returns the colexectypes slice as a phystypes slice.
func AsPhysTypes(typs []T) []phystypes.T {
	phystyps := make([]phystypes.T, len(typs))
	for i := range typs {
		phystyps[i] = typs[i].T
	}
	return phystyps
}

// ToColumnType converts a types.T that corresponds to the column type. Note
// that due to the fact that multiple types.T's are represented by a single
// column type, this conversion might return the type that is unexpected.
// NOTE: this should only be used in tests.
func ToColumnType(t phystypes.T) *types.T {
	switch t {
	case phystypes.Bool:
		return types.Bool
	case phystypes.Bytes:
		return types.Bytes
	case phystypes.Decimal:
		return types.Decimal
	case phystypes.Int16:
		return types.Int2
	case phystypes.Int32:
		return types.Int4
	case phystypes.Int64:
		return types.Int
	case phystypes.Float64:
		return types.Float
	case phystypes.Timestamp:
		return types.Timestamp
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected coltype %s", t.String()))
	return nil
}

// ToColumnTypes calls ToColumnType on each element of typs returning the
// resulting slice.
func ToColumnTypes(typs []phystypes.T) []types.T {
	cts := make([]types.T, len(typs))
	for i := range cts {
		t := ToColumnType(typs[i])
		cts[i] = *t
	}
	return cts
}

// GetDatumToPhysicalFn returns a function for converting a datum of the given
// ColumnType to the corresponding Go type.
func GetDatumToPhysicalFn(ct *types.T) func(tree.Datum) (interface{}, error) {
	switch ct.Family() {
	case types.BoolFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBool)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBool, found %s", reflect.TypeOf(datum))
			}
			return bool(*d), nil
		}
	case types.BytesFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBytes)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBytes, found %s", reflect.TypeOf(datum))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case types.IntFamily:
		switch ct.Width() {
		case 16:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int16(*d), nil
			}
		case 32:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int32(*d), nil
			}
		case 0, 64:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int64(*d), nil
			}
		}
		execerror.VectorizedInternalPanic(fmt.Sprintf("unhandled INT width %d", ct.Width()))
	case types.DateFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DDate)
			if !ok {
				return nil, errors.Errorf("expected *tree.DDate, found %s", reflect.TypeOf(datum))
			}
			return d.UnixEpochDaysWithOrig(), nil
		}
	case types.FloatFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DFloat)
			if !ok {
				return nil, errors.Errorf("expected *tree.DFloat, found %s", reflect.TypeOf(datum))
			}
			return float64(*d), nil
		}
	case types.OidFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DOid)
			if !ok {
				return nil, errors.Errorf("expected *tree.DOid, found %s", reflect.TypeOf(datum))
			}
			return int64(d.DInt), nil
		}
	case types.StringFamily:
		return func(datum tree.Datum) (interface{}, error) {
			// Handle other STRING-related OID types, like oid.T_name.
			wrapper, ok := datum.(*tree.DOidWrapper)
			if ok {
				datum = wrapper.Wrapped
			}

			d, ok := datum.(*tree.DString)
			if !ok {
				return nil, errors.Errorf("expected *tree.DString, found %s", reflect.TypeOf(datum))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case types.DecimalFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DDecimal)
			if !ok {
				return nil, errors.Errorf("expected *tree.DDecimal, found %s", reflect.TypeOf(datum))
			}
			return d.Decimal, nil
		}
	case types.UuidFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DUuid)
			if !ok {
				return nil, errors.Errorf("expected *tree.DUuid, found %s", reflect.TypeOf(datum))
			}
			return d.UUID.GetBytesMut(), nil
		}
	case types.TimestampFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DTimestamp)
			if !ok {
				return nil, errors.Errorf("expected *tree.DTimestamp, found %s", reflect.TypeOf(datum))
			}
			return d.Time, nil
		}
	case types.TimestampTZFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DTimestampTZ)
			if !ok {
				return nil, errors.Errorf("expected *tree.DTimestampTZ, found %s", reflect.TypeOf(datum))
			}
			return d.Time, nil
		}
	}
	// It would probably be more correct to return an error here, rather than a
	// function which always returns an error. But since the function tends to be
	// invoked immediately after GetDatumToPhysicalFn is called, this works just
	// as well and makes the error handling less messy for the caller.
	return func(datum tree.Datum) (interface{}, error) {
		return nil, errors.Errorf("unhandled type %s", ct.DebugString())
	}
}
