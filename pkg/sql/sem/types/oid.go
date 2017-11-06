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
	// Unexported wrapper types. These exist for Postgres type compatibility.
	typeInt2      = WrapTypeWithOid(TypeInt, oid.T_int2)
	typeInt4      = WrapTypeWithOid(TypeInt, oid.T_int4)
	typeFloat4    = WrapTypeWithOid(TypeFloat, oid.T_float4)
	typeVarChar   = WrapTypeWithOid(TypeString, oid.T_varchar)
	typeInt2Array = TArray{typeInt2}
	typeInt4Array = TArray{typeInt4}
)

// OidToType maps Postgres object IDs to CockroachDB types.
var OidToType = map[oid.Oid]Type{
	oid.T_anyelement:   TypeAny,
	oid.T_bool:         TypeBool,
	oid.T__bool:        TArray{TypeBool},
	oid.T_bytea:        TypeBytes,
	oid.T__bytea:       TArray{TypeBytes},
	oid.T_date:         TypeDate,
	oid.T__date:        TArray{TypeDate},
	oid.T_float4:       typeFloat4,
	oid.T__float4:      TArray{typeFloat4},
	oid.T_float8:       TypeFloat,
	oid.T__float8:      TArray{TypeFloat},
	oid.T_int2:         typeInt2,
	oid.T_int4:         typeInt4,
	oid.T_int8:         TypeInt,
	oid.T_int2vector:   TypeIntVector,
	oid.T_interval:     TypeInterval,
	oid.T__interval:    TArray{TypeInterval},
	oid.T_jsonb:        TypeJSON,
	oid.T_name:         TypeName,
	oid.T__name:        TArray{TypeName},
	oid.T_numeric:      TypeDecimal,
	oid.T__numeric:     TArray{TypeDecimal},
	oid.T_oid:          TypeOid,
	oid.T__oid:         TArray{TypeOid},
	oid.T_regclass:     TypeRegClass,
	oid.T_regnamespace: TypeRegNamespace,
	oid.T_regproc:      TypeRegProc,
	oid.T_regprocedure: TypeRegProcedure,
	oid.T_regtype:      TypeRegType,
	oid.T__text:        TArray{TypeString},
	oid.T__int2:        typeInt2Array,
	oid.T__int4:        typeInt4Array,
	oid.T__int8:        TArray{TypeInt},
	oid.T_record:       TypeTuple,
	oid.T_text:         TypeString,
	oid.T_timestamp:    TypeTimestamp,
	oid.T__timestamp:   TArray{TypeTimestamp},
	oid.T_timestamptz:  TypeTimestampTZ,
	oid.T__timestamptz: TArray{TypeTimestampTZ},
	oid.T_uuid:         TypeUUID,
	oid.T__uuid:        TArray{TypeUUID},
	oid.T_inet:         TypeINet,
	oid.T__inet:        TArray{TypeINet},
	oid.T_varchar:      typeVarChar,
	oid.T__varchar:     TArray{typeVarChar},
}

// AliasedOidToName maps Postgres object IDs to type names for those OIDs that map to
// Cockroach types that have more than one associated OID, like Int. The name
// for these OIDs will override the type name of the corresponding type when
// looking up the display name for an OID.
var aliasedOidToName = map[oid.Oid]string{
	oid.T_float4:     "float4",
	oid.T_float8:     "float8",
	oid.T_int2:       "int2",
	oid.T_int4:       "int4",
	oid.T_int8:       "int8",
	oid.T_int2vector: "int2vector",
	oid.T_text:       "text",
	oid.T_bytea:      "bytea",
	oid.T_varchar:    "varchar",
	oid.T_numeric:    "numeric",
	oid.T_record:     "record",
	oid.T__int2:      "_int2",
	oid.T__int4:      "_int4",
	oid.T__int8:      "_int8",
	oid.T__text:      "_text",
	// TODO(justin): find a better solution to this than mapping every array type.
	oid.T__float4:      "_float4",
	oid.T__float8:      "_float8",
	oid.T__bool:        "_bool",
	oid.T__bytea:       "_bytea",
	oid.T__date:        "_date",
	oid.T__interval:    "_interval",
	oid.T__name:        "_name",
	oid.T__numeric:     "_numeric",
	oid.T__oid:         "_oid",
	oid.T__timestamp:   "_timestamp",
	oid.T__timestamptz: "_timestamptz",
	oid.T__uuid:        "_uuid",
	oid.T__inet:        "_inet",
	oid.T__varchar:     "_varchar",
}

// oidToArrayOid maps scalar type Oids to their corresponding array type Oid.
var oidToArrayOid = map[oid.Oid]oid.Oid{
	oid.T_bool:        oid.T__bool,
	oid.T_bytea:       oid.T__bytea,
	oid.T_name:        oid.T__name,
	oid.T_int8:        oid.T__int8,
	oid.T_int2:        oid.T__int2,
	oid.T_int4:        oid.T__int4,
	oid.T_text:        oid.T__text,
	oid.T_oid:         oid.T__oid,
	oid.T_float4:      oid.T__float4,
	oid.T_float8:      oid.T__float8,
	oid.T_inet:        oid.T__inet,
	oid.T_varchar:     oid.T__varchar,
	oid.T_date:        oid.T__date,
	oid.T_timestamp:   oid.T__timestamp,
	oid.T_timestamptz: oid.T__timestamptz,
	oid.T_interval:    oid.T__interval,
	oid.T_numeric:     oid.T__numeric,
	oid.T_uuid:        oid.T__uuid,
}

// PGDisplayName returns the Postgres display name for a given type.
func PGDisplayName(typ Type) string {
	if typname, ok := aliasedOidToName[typ.Oid()]; ok {
		return typname
	}
	return typ.String()
}
