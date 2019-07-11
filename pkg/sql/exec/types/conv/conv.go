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
	"unsafe"
<<<<<<< HEAD
=======

	"github.com/lib/pq/oid"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
>>>>>>> exec: Refactor colvec element to datum conversion in materialize.

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
<<<<<<< HEAD
	"github.com/lib/pq/oid"
=======
>>>>>>> exec: Refactor colvec element to datum conversion in materialize.
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
	// It would probably be more correct to return an error here, rather than a
	// function which always returns an error. But since the function tends to be
	// invoked immediately after GetDatumToPhysicalFn is called, this works just
	// as well and makes the error handling less messy for the caller.
	return func(datum tree.Datum) (interface{}, error) {
		return nil, errors.Errorf("unhandled type %s", ct.DebugString())
	}
}

// PhysicalTypeColElemToDatum converts an element in a colvec to a datum of semtype ct.
<<<<<<< HEAD
func PhysicalTypeColElemToDatum(
	col coldata.Vec, rowIdx uint16, da sqlbase.DatumAlloc, ct semtypes.T,
) tree.Datum {
=======
func PhysicalTypeColElemToDatum(col coldata.Vec, rowIdx uint16, da sqlbase.DatumAlloc, ct semtypes.T) tree.Datum {
>>>>>>> exec: Refactor colvec element to datum conversion in materialize.
	switch ct.Family() {
	case semtypes.BoolFamily:
		if col.Bool()[rowIdx] {
			return tree.DBoolTrue
		}
		return tree.DBoolFalse
	case semtypes.IntFamily:
		switch ct.Width() {
		case 8:
			return da.NewDInt(tree.DInt(col.Int8()[rowIdx]))
		case 16:
			return da.NewDInt(tree.DInt(col.Int16()[rowIdx]))
		case 32:
			return da.NewDInt(tree.DInt(col.Int32()[rowIdx]))
		default:
			return da.NewDInt(tree.DInt(col.Int64()[rowIdx]))
		}
	case semtypes.FloatFamily:
		return da.NewDFloat(tree.DFloat(col.Float64()[rowIdx]))
	case semtypes.DecimalFamily:
		return da.NewDDecimal(tree.DDecimal{Decimal: col.Decimal()[rowIdx]})
	case semtypes.DateFamily:
		return tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(col.Int64()[rowIdx]))
	case semtypes.StringFamily:
		b := col.Bytes()[rowIdx]
		if ct.Oid() == oid.T_name {
			return da.NewDName(tree.DString(*(*string)(unsafe.Pointer(&b))))
		}
		return da.NewDString(tree.DString(*(*string)(unsafe.Pointer(&b))))
	case semtypes.BytesFamily:
		return da.NewDBytes(tree.DBytes(col.Bytes()[rowIdx]))
	case semtypes.OidFamily:
		return da.NewDOid(tree.MakeDOid(tree.DInt(col.Int64()[rowIdx])))
	default:
		panic(fmt.Sprintf("Unsupported column type %s", ct.String()))
	}
}
