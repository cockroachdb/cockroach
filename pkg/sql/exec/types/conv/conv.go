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

package conv

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

// FromColumnType returns the T that corresponds to the input ColumnType.
func FromColumnType(ct sqlbase.ColumnType) types.T {
	switch ct.SemanticType {
	case sqlbase.ColumnType_BOOL:
		return types.Bool
	case sqlbase.ColumnType_BYTES, sqlbase.ColumnType_STRING, sqlbase.ColumnType_NAME:
		return types.Bytes
	case sqlbase.ColumnType_DATE, sqlbase.ColumnType_OID:
		return types.Int64
	case sqlbase.ColumnType_DECIMAL:
		return types.Decimal
	case sqlbase.ColumnType_INT:
		switch ct.Width {
		case 8:
			return types.Int8
		case 16:
			return types.Int16
		case 32:
			return types.Int32
		case 0, 64:
			return types.Int64
		}
		panic(fmt.Sprintf("integer with unknown width %d", ct.Width))
	case sqlbase.ColumnType_FLOAT:
		return types.Float64
	}
	return types.Unhandled
}

// FromColumnTypes calls FromColumnType on each element of cts, returning the
// resulting slice.
func FromColumnTypes(cts []sqlbase.ColumnType) []types.T {
	typs := make([]types.T, len(cts))
	for i := range typs {
		typs[i] = FromColumnType(cts[i])
	}
	return typs
}

// GetDatumToPhysicalFn returns a function for converting a datum of the given
// ColumnType to the corresponding Go type.
func GetDatumToPhysicalFn(ct sqlbase.ColumnType) func(tree.Datum) (interface{}, error) {
	switch ct.SemanticType {
	case sqlbase.ColumnType_BOOL:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBool)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBool, found %s", reflect.TypeOf(datum))
			}
			return bool(*d), nil
		}
	case sqlbase.ColumnType_BYTES:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DBytes)
			if !ok {
				return nil, errors.Errorf("expected *tree.DBytes, found %s", reflect.TypeOf(datum))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case sqlbase.ColumnType_INT:
		switch ct.Width {
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
		panic(fmt.Sprintf("unhandled INT width %d", ct.Width))
	case sqlbase.ColumnType_DATE:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DDate)
			if !ok {
				return nil, errors.Errorf("expected *tree.DDate, found %s", reflect.TypeOf(datum))
			}
			return int64(*d), nil
		}
	case sqlbase.ColumnType_FLOAT:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DFloat)
			if !ok {
				return nil, errors.Errorf("expected *tree.DFloat, found %s", reflect.TypeOf(datum))
			}
			return float64(*d), nil
		}
	case sqlbase.ColumnType_OID:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DOid)
			if !ok {
				return nil, errors.Errorf("expected *tree.DOid, found %s", reflect.TypeOf(datum))
			}
			return int64(d.DInt), nil
		}
	case sqlbase.ColumnType_STRING:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DString)
			if !ok {
				return nil, errors.Errorf("expected *tree.DString, found %s", reflect.TypeOf(datum))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case sqlbase.ColumnType_NAME:
		return func(datum tree.Datum) (interface{}, error) {
			wrapper, ok := datum.(*tree.DOidWrapper)
			if !ok {
				return nil, errors.Errorf("expected *tree.DOidWrapper, found %s", reflect.TypeOf(datum))
			}
			d, ok := wrapper.Wrapped.(*tree.DString)
			if !ok {
				return nil, errors.Errorf("expected *tree.DString, found %s", reflect.TypeOf(wrapper))
			}
			return encoding.UnsafeConvertStringToBytes(string(*d)), nil
		}
	case sqlbase.ColumnType_DECIMAL:
		return func(datum tree.Datum) (interface{}, error) {
			d, ok := datum.(*tree.DDecimal)
			if !ok {
				return nil, errors.Errorf("expected *tree.DDecimal, found %s", reflect.TypeOf(datum))
			}
			return d.Decimal, nil
		}
	}
	panic(fmt.Sprintf("unhandled ColumnType %s", ct.String()))
}
