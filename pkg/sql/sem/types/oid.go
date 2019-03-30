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

import "github.com/lib/pq/oid"

var (
	// Oid is the type of an OID. Can be compared with ==.
	Oid = &T{SemanticType: OID}
	// RegClass is the type of an regclass OID variant. Can be compared with ==.
	RegClass = &T{SemanticType: OID, ZZZ_Oid: oid.T_regclass}
	// RegNamespace is the type of an regnamespace OID variant. Can be compared with ==.
	RegNamespace = &T{SemanticType: OID, ZZZ_Oid: oid.T_regnamespace}
	// RegProc is the type of an regproc OID variant. Can be compared with ==.
	RegProc = &T{SemanticType: OID, ZZZ_Oid: oid.T_regproc}
	// RegProcedure is the type of an regprocedure OID variant. Can be compared with ==.
	RegProcedure = &T{SemanticType: OID, ZZZ_Oid: oid.T_regprocedure}
	// RegType is the type of an regtype OID variant. Can be compared with ==.
	RegType = &T{SemanticType: OID, ZZZ_Oid: oid.T_regtype}

	// Name is a type-alias for String with a different OID. Can be
	// compared with ==.
	Name = &T{SemanticType: STRING, ZZZ_Oid: oid.T_name}
	// Int2Vector is a type-alias for an IntArray with a different OID. Can
	// be compared with ==.
	Int2Vector = &T{SemanticType: ARRAY, ZZZ_Oid: oid.T_int2vector, ArrayContents: Int2}
	// OidVector is a type-alias for an OidArray with a different OID. Can
	// be compared with ==.
	OidVector = &T{SemanticType: ARRAY, ZZZ_Oid: oid.T_oidvector, ArrayContents: Oid}
)

var semanticTypeToOid = map[SemanticType]oid.Oid{
	BOOL:           oid.T_bool,
	INT:            oid.T_int8,
	FLOAT:          oid.T_float8,
	DECIMAL:        oid.T_numeric,
	DATE:           oid.T_date,
	TIMESTAMP:      oid.T_timestamp,
	INTERVAL:       oid.T_interval,
	STRING:         oid.T_text,
	BYTES:          oid.T_bytea,
	TIMESTAMPTZ:    oid.T_timestamptz,
	COLLATEDSTRING: oid.T_text,
	OID:            oid.T_oid,
	NULL:           oid.T_unknown,
	UUID:           oid.T_uuid,
	ARRAY:          oid.T_anyarray,
	INET:           oid.T_inet,
	TIME:           oid.T_time,
	JSON:           oid.T_jsonb,
	TUPLE:          oid.T_record,
	BIT:            oid.T_bit,
	ANY:            oid.T_anyelement,
}

// OidToType maps Postgres object IDs to CockroachDB types.  We export the map
// instead of a method so that other packages can iterate over the map directly.
// Note that additional elements for the array Oid types are added in init().
var OidToType = map[oid.Oid]*T{
	oid.T_anyelement:   Any,
	oid.T_bit:          typeBit,
	oid.T_bool:         Bool,
	oid.T_bpchar:       typeBpChar,
	oid.T_bytea:        Bytes,
	oid.T_char:         typeQChar,
	oid.T_date:         Date,
	oid.T_float4:       Float4,
	oid.T_float8:       Float,
	oid.T_int2:         Int2,
	oid.T_int2vector:   Int2Vector,
	oid.T_int4:         Int4,
	oid.T_int8:         Int,
	oid.T_inet:         INet,
	oid.T_interval:     Interval,
	oid.T_jsonb:        Jsonb,
	oid.T_name:         Name,
	oid.T_numeric:      Decimal,
	oid.T_oid:          Oid,
	oid.T_oidvector:    OidVector,
	oid.T_record:       AnyTuple,
	oid.T_regclass:     RegClass,
	oid.T_regnamespace: RegNamespace,
	oid.T_regproc:      RegProc,
	oid.T_regprocedure: RegProcedure,
	oid.T_regtype:      RegType,
	oid.T_text:         String,
	oid.T_time:         Time,
	oid.T_timestamp:    Timestamp,
	oid.T_timestamptz:  TimestampTZ,
	oid.T_uuid:         Uuid,
	oid.T_varbit:       VarBit,
	oid.T_varchar:      VarChar,
}

// oidToArrayOid maps scalar type Oids to their corresponding array type Oid.
var oidToArrayOid = map[oid.Oid]oid.Oid{
	oid.T_anyelement:   oid.T_anyarray,
	oid.T_bit:          oid.T__bit,
	oid.T_bool:         oid.T__bool,
	oid.T_bpchar:       oid.T__bpchar,
	oid.T_bytea:        oid.T__bytea,
	oid.T_char:         oid.T__char,
	oid.T_date:         oid.T__date,
	oid.T_float4:       oid.T__float4,
	oid.T_float8:       oid.T__float8,
	oid.T_inet:         oid.T__inet,
	oid.T_int2:         oid.T__int2,
	oid.T_int2vector:   oid.T__int2vector,
	oid.T_int4:         oid.T__int4,
	oid.T_int8:         oid.T__int8,
	oid.T_interval:     oid.T__interval,
	oid.T_jsonb:        oid.T__jsonb,
	oid.T_name:         oid.T__name,
	oid.T_numeric:      oid.T__numeric,
	oid.T_oid:          oid.T__oid,
	oid.T_oidvector:    oid.T__oidvector,
	oid.T_record:       oid.T__record,
	oid.T_regclass:     oid.T__regclass,
	oid.T_regnamespace: oid.T__regnamespace,
	oid.T_regproc:      oid.T__regproc,
	oid.T_regprocedure: oid.T__regprocedure,
	oid.T_regtype:      oid.T__regtype,
	oid.T_text:         oid.T__text,
	oid.T_time:         oid.T__time,
	oid.T_timestamp:    oid.T__timestamp,
	oid.T_timestamptz:  oid.T__timestamptz,
	oid.T_uuid:         oid.T__uuid,
	oid.T_varbit:       oid.T__varbit,
	oid.T_varchar:      oid.T__varchar,
}

// ArrayOids is a set of all oids which correspond to an array type.
var ArrayOids = map[oid.Oid]struct{}{}

func init() {
	if len(oidToArrayOid) != len(oidToArrayOid) {
		panic("missing some mapping from array element OID to array OID")
	}
	for o, ao := range oidToArrayOid {
		ArrayOids[ao] = struct{}{}
		OidToType[ao] = &T{SemanticType: ARRAY, ArrayContents: OidToType[o]}
	}
}
