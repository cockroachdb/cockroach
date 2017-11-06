// Copyright 2017 The Cockroach Authors.
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

package types

import "fmt"

// SQLName returns the type's SQL standard name. This can be looked up for a
// type `t` in postgres by running `SELECT format_type(t::regtype, NULL)`.
func SQLName(t Type) string {
	switch ty := t.(type) {
	case TOid:
		return oidTypeName[ty.oidType]
	case TOidWrapper:
		return SQLName(ty.Type)
	case TCollatedString:
		return "text"
	case TTuple:
		return "record"
	case TTable:
		return "anyelement"
	case TArray:
		return SQLName(ty.Typ) + "[]"
	}
	if s, ok := sqlTypeNames[t]; ok {
		return s
	}
	panic(fmt.Sprintf("no SQL name known for type %v (%T)", t, t))
}

var sqlTypeNames = map[Type]string{
	TypeNull:        "unknown",
	TypeBool:        "boolean",
	TypeInt:         "bigint",
	TypeFloat:       "double precision",
	TypeDecimal:     "numeric",
	TypeString:      "text",
	TypeBytes:       "bytea",
	TypeDate:        "date",
	TypeTimestamp:   "timestamp without time zone",
	TypeTimestampTZ: "timestamp with time zone",
	TypeInterval:    "interval",
	TypeJSON:        "json",
	TypeUUID:        "uuid",
	TypeINet:        "inet",
	TypeAny:         "anyelement",
}
