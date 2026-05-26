// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlclient

import (
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

// databaseTypeName returns the database type name for the given type OID.
func databaseTypeName(tm *pgtype.Map, typeOID uint32) string {
	dataType, ok := tm.TypeForOID(typeOID)
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
