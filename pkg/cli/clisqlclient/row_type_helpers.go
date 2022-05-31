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
	"strings"

	"github.com/jackc/pgtype"
)

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
