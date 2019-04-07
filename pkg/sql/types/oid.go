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

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/lib/pq/oid"
)

// Convenience list of pre-constructed OID-related types.
var (
	// Oid is the type of a Postgres Object ID value.
	Oid = &T{InternalType: InternalType{
		SemanticType: OID, Oid: oid.T_oid, Locale: &emptyLocale}}

	// Regclass is the type of a Postgres regclass OID variant (T_regclass).
	RegClass = &T{InternalType: InternalType{
		SemanticType: OID, Oid: oid.T_regclass, Locale: &emptyLocale}}

	// RegNamespace is the type of a Postgres regnamespace OID variant
	// (T_regnamespace).
	RegNamespace = &T{InternalType: InternalType{
		SemanticType: OID, Oid: oid.T_regnamespace, Locale: &emptyLocale}}

	// RegProc is the type of a Postgres regproc OID variant (T_regproc).
	RegProc = &T{InternalType: InternalType{
		SemanticType: OID, Oid: oid.T_regproc, Locale: &emptyLocale}}

	// RegProcedure is the type of a Postgres regprocedure OID variant
	// (T_regprocedure).
	RegProcedure = &T{InternalType: InternalType{
		SemanticType: OID, Oid: oid.T_regprocedure, Locale: &emptyLocale}}

	// RegType is the type of of a Postgres regtype OID variant (T_regtype).
	RegType = &T{InternalType: InternalType{
		SemanticType: OID, Oid: oid.T_regtype, Locale: &emptyLocale}}

	// OidVector is a type-alias for an array of Oid values, but with a different
	// OID (T_oidvector instead of T__oid). It is a special VECTOR type used by
	// Postgres in system tables.
	OidVector = &T{InternalType: InternalType{
		SemanticType: ARRAY, Oid: oid.T_oidvector, ArrayContents: Oid, Locale: &emptyLocale}}
)

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
	oid.T_unknown:      Unknown,
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

// semanticTypeToOid maps SemanticType values to a default OID value that is
// used when another Oid is not present (e.g. when deserializing a type saved
// by a previous version of CRDB).
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
	UNKNOWN:        oid.T_unknown,
	UUID:           oid.T_uuid,
	ARRAY:          oid.T_anyarray,
	INET:           oid.T_inet,
	TIME:           oid.T_time,
	JSON:           oid.T_jsonb,
	TUPLE:          oid.T_record,
	BIT:            oid.T_bit,
	ANY:            oid.T_anyelement,
}

// ArrayOids is a set of all oids which correspond to an array type.
var ArrayOids = map[oid.Oid]struct{}{}

func init() {
	if len(oidToArrayOid) != len(oidToArrayOid) {
		panic("missing some mapping from array element OID to array OID")
	}
	for o, ao := range oidToArrayOid {
		ArrayOids[ao] = struct{}{}
		OidToType[ao] = MakeArray(OidToType[o])
	}
}

// calcArrayOid returns the OID of the array type having elements of the given
// type.
func calcArrayOid(elemTyp *T) oid.Oid {
	o := elemTyp.Oid()
	switch elemTyp.SemanticType() {
	case ARRAY:
		// Postgres nested arrays return the OID of the nested array (i.e. the
		// OID doesn't change no matter how many levels of nesting there are),
		// except in the special-case of the vector types.
		switch o {
		case oid.T_int2vector, oid.T_oidvector:
			// Vector types have their own array OID types.
		default:
			return o
		}

	case UNKNOWN:
		// Postgres doesn't have an OID for ARRAY of UNKNOWN, since it's not
		// possible to create that in Postgres. But CRDB does allow that, so
		// return 0 for that case (since there's no T__unknown). This is what
		// previous versions of CRDB returned for this case.
		return unknownArrayOid
	}

	// Map the OID of the array element type to the corresponding array OID.
	// This should always be possible for all other OIDs (checked in oid.go
	// init method).
	o = oidToArrayOid[o]
	if o == 0 {
		panic(pgerror.NewAssertionErrorf("oid %d couldn't be mapped to array oid", o))
	}
	return o
}
