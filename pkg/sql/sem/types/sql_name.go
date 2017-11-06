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

// SQLName returns the type's SQL standard name. This can be looked up for a
// type `t` in postgres by running `SELECT format_type(t::regtype, NULL)`.
func SQLName(t T) string {
	switch ty := t.(type) {
	case TOid:
		return oidTypeName[ty.oidType]
	case TOidWrapper:
		return SQLName(ty.T)
	case TCollatedString:
		return "text"
	case TTuple:
		return "record"
	case TTable:
		return "anyelement"
	case TArray:
		return SQLName(ty.Typ) + "[]"
	}
	return sqlTypeNames[t]
}

var sqlTypeNames = map[T]string{
	Unknown:     "unknown",
	Bool:        "boolean",
	Int:         "bigint",
	Float:       "double precision",
	Decimal:     "numeric",
	String:      "text",
	Bytes:       "bytea",
	Date:        "date",
	Timestamp:   "timestamp without time zone",
	TimestampTZ: "timestamp with time zone",
	Interval:    "interval",
	JSON:        "json",
	UUID:        "uuid",
	INet:        "inet",
	Any:         "anyelement",
}
