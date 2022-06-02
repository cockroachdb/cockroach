// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlclient

import (
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgtype"
)

// scanType returns the Go type used for the given column type.
func scanType(typeOID uint32, typeName string) reflect.Type {
	if typeName == "" || strings.HasPrefix(typeName, "_") {
		// User-defined types and array types are scanned into []byte.
		// These are handled separately since we can't easily include all the OIDs
		// for user-defined types and arrays in the switch statement.
		return reflect.TypeOf([]byte(nil))
	}
	// This switch is copied from lib/pq, and modified with a few additional cases.
	// https://github.com/lib/pq/blob/b2901c7946b69f1e7226214f9760e31620499595/rows.go#L24
	switch typeOID {
	case pgtype.Int8OID:
		return reflect.TypeOf(int64(0))
	case pgtype.Int4OID:
		return reflect.TypeOf(int32(0))
	case pgtype.Int2OID:
		return reflect.TypeOf(int16(0))
	case pgtype.VarcharOID, pgtype.TextOID, pgtype.NameOID,
		pgtype.ByteaOID, pgtype.NumericOID, pgtype.RecordOID,
		pgtype.QCharOID, pgtype.BPCharOID:
		return reflect.TypeOf("")
	case pgtype.BoolOID:
		return reflect.TypeOf(false)
	case pgtype.DateOID, pgtype.TimeOID, 1266, pgtype.TimestampOID, pgtype.TimestamptzOID:
		// 1266 is the OID for TimeTZ.
		// TODO(rafi): Add TimetzOID to pgtype.
		return reflect.TypeOf(time.Time{})
	default:
		return reflect.TypeOf(new(interface{})).Elem()
	}
}

// databaseTypeName returns the database type name for the given type OID.
func databaseTypeName(ci *pgtype.ConnInfo, typeOID uint32) string {
	dataType, ok := ci.DataTypeForOID(typeOID)
	if !ok {
		// TODO(rafi): remove special logic once jackc/pgtype includes these types.
		switch typeOID {
		case 1002:
			return "_CHAR"
		case 1003:
			return "_NAME"
		case 1266:
			return "TIMETZ"
		case 1270:
			return "_TIMETZ"
		default:
			return ""
		}
	}
	return strings.ToUpper(dataType.Name)
}
