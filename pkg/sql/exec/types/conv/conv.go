// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package conv

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

// FromColumnType returns the T that corresponds to the input ColumnType.
func FromColumnType(ct *semtypes.T) types.T {
	switch ct.Family() {
	case semtypes.BoolFamily:
		return types.Bool
	case semtypes.BytesFamily, semtypes.StringFamily:
		return types.Bytes
	case semtypes.DateFamily, semtypes.OidFamily:
		return types.Int64
	case semtypes.DecimalFamily:
		return types.Decimal
	case semtypes.IntFamily:
		switch ct.Width() {
		case 8:
			return types.Int8
		case 16:
			return types.Int16
		case 32:
			return types.Int32
		case 0, 64:
			return types.Int64
		}
		panic(fmt.Sprintf("integer with unknown width %d", ct.Width()))
	case semtypes.FloatFamily:
		return types.Float64
	}
	return types.Unhandled
}

// FromColumnTypes calls FromColumnType on each element of cts, returning the
// resulting slice.
func FromColumnTypes(cts []semtypes.T) []types.T {
	typs := make([]types.T, len(cts))
	for i := range typs {
		typs[i] = FromColumnType(&cts[i])
	}
	return typs
}

// GetDatumToPhysicalFn returns a function for converting a datum of the given
// ColumnType to the corresponding Go type.
func GetDatumToPhysicalFn(ct *semtypes.T) func(tree.Datum) (interface{}, error) {
	switch ct.Family() {
	case semtypes.BoolFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBool)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBool, found %s", reflect.TypeOf(datum))
			}
			return bool(*d), nil
		}
	case semtypes.BytesFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBytes)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBytes, found %s", reflect.TypeOf(datum))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case semtypes.IntFamily:
		switch ct.Width() {
		case 8:
			return func(datum tree.Datum) (interface{}, error) {
				d, ok := datum.(*tree.DInt)
				if !ok {
					return nil, errors.Errorf("expected *tree.DInt, found %s", reflect.TypeOf(datum))
				}
				return int8(*d), nil
			}
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
		panic(fmt.Sprintf("unhandled INT width %d", ct.Width()))
	case semtypes.DateFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DDate)
			if !ok {
				return nil, errors.Errorf("expected *tree.DDate, found %s", reflect.TypeOf(datum))
			}
			return d.UnixEpochDaysWithOrig(), nil
		}
	case semtypes.FloatFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DFloat)
			if !ok {
				return nil, errors.Errorf("expected *tree.DFloat, found %s", reflect.TypeOf(datum))
			}
			return float64(*d), nil
		}
	case semtypes.OidFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DOid)
			if !ok {
				return nil, errors.Errorf("expected *tree.DOid, found %s", reflect.TypeOf(datum))
			}
			return int64(d.DInt), nil
		}
	case semtypes.StringFamily:
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
	case semtypes.DecimalFamily:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DDecimal)
			if !ok {
				return nil, errors.Errorf("expected *tree.DDecimal, found %s", reflect.TypeOf(datum))
			}
			return d.Decimal, nil
		}
	}
	panic(fmt.Sprintf("unhandled type %s", ct.DebugString()))
}
