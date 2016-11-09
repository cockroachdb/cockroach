// Copyright 2016 The Cockroach Authors.
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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package sql

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/lib/pq/oid"
)

var oidToDatum = map[oid.Oid]parser.Type{
	oid.T_anyelement:  parser.TypeAny,
	oid.T_bool:        parser.TypeBool,
	oid.T_bytea:       parser.TypeBytes,
	oid.T_date:        parser.TypeDate,
	oid.T_float4:      parser.TypeFloat,
	oid.T_float8:      parser.TypeFloat,
	oid.T_int2:        parser.TypeInt,
	oid.T_int4:        parser.TypeInt,
	oid.T_int8:        parser.TypeInt,
	oid.T_interval:    parser.TypeInterval,
	oid.T_numeric:     parser.TypeDecimal,
	oid.T_text:        parser.TypeString,
	oid.T__text:       parser.TypeStringArray,
	oid.T__int2:       parser.TypeIntArray,
	oid.T__int4:       parser.TypeIntArray,
	oid.T__int8:       parser.TypeIntArray,
	oid.T_timestamp:   parser.TypeTimestamp,
	oid.T_timestamptz: parser.TypeTimestampTZ,
	oid.T_varchar:     parser.TypeString,
}

var datumToOid = map[reflect.Type]oid.Oid{
	reflect.TypeOf(parser.TypeAny):         oid.T_anyelement,
	reflect.TypeOf(parser.TypeIntArray):    oid.T__int8,
	reflect.TypeOf(parser.TypeStringArray): oid.T__text,
	reflect.TypeOf(parser.TypeBool):        oid.T_bool,
	reflect.TypeOf(parser.TypeBytes):       oid.T_bytea,
	reflect.TypeOf(parser.TypeDate):        oid.T_date,
	reflect.TypeOf(parser.TypeFloat):       oid.T_float8,
	reflect.TypeOf(parser.TypeInt):         oid.T_int8,
	reflect.TypeOf(parser.TypeInterval):    oid.T_interval,
	reflect.TypeOf(parser.TypeDecimal):     oid.T_numeric,
	reflect.TypeOf(parser.TypeString):      oid.T_text,
	reflect.TypeOf(parser.TypeTimestamp):   oid.T_timestamp,
	reflect.TypeOf(parser.TypeTimestampTZ): oid.T_timestamptz,
	reflect.TypeOf(parser.TypeTuple):       oid.T_record,
}

// OidToDatum maps Postgres object IDs to CockroachDB types.
func OidToDatum(oid oid.Oid) (parser.Type, bool) {
	t, ok := oidToDatum[oid]
	return t, ok
}

// DatumToOid maps CockroachDB types to Postgres object IDs, using reflection
// to support unhashable types.
func DatumToOid(typ parser.Type) (oid.Oid, bool) {
	oid, ok := datumToOid[reflect.TypeOf(typ)]
	return oid, ok
}
