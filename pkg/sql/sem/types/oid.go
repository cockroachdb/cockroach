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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/lib/pq/oid"
)

var (
	// Oid is the type of an OID. Can be compared with ==.
	Oid = TOid{oid.T_oid}
	// RegClass is the type of an regclass OID variant. Can be compared with ==.
	RegClass = TOid{oid.T_regclass}
	// RegNamespace is the type of an regnamespace OID variant. Can be compared with ==.
	RegNamespace = TOid{oid.T_regnamespace}
	// RegProc is the type of an regproc OID variant. Can be compared with ==.
	RegProc = TOid{oid.T_regproc}
	// RegProcedure is the type of an regprocedure OID variant. Can be compared with ==.
	RegProcedure = TOid{oid.T_regprocedure}
	// RegType is the type of an regtype OID variant. Can be compared with ==.
	RegType = TOid{oid.T_regtype}

	// Name is a type-alias for String with a different OID. Can be
	// compared with ==.
	Name = WrapTypeWithOid(String, oid.T_name)
	// IntVector is a type-alias for an IntArray with a different OID. Can
	// be compared with ==.
	IntVector = WrapTypeWithOid(TArray{Int}, oid.T_int2vector)
	// OidVector is a type-alias for an OidArray with a different OID. Can
	// be compared with ==.
	OidVector = WrapTypeWithOid(TArray{Oid}, oid.T_oidvector)
	// NameArray is the type family of a DArray containing the Name alias type.
	// Can be compared with ==.
	NameArray T = TArray{Name}
)

var (
	// Unexported wrapper types. These exist for Postgres type compatibility.
	typeInt2      = WrapTypeWithOid(Int, oid.T_int2)
	typeInt4      = WrapTypeWithOid(Int, oid.T_int4)
	typeFloat4    = WrapTypeWithOid(Float, oid.T_float4)
	typeVarChar   = WrapTypeWithOid(String, oid.T_varchar)
	typeInt2Array = TArray{typeInt2}
	typeInt4Array = TArray{typeInt4}
)

// OidToType maps Postgres object IDs to CockroachDB types.  We export
// the map instead of a method so that other packages can iterate over
// the map directly.
var OidToType = map[oid.Oid]T{
	oid.T_anyelement:   Any,
	oid.T_anyarray:     TArray{Any},
	oid.T_bool:         Bool,
	oid.T__bool:        TArray{Bool},
	oid.T_bytea:        Bytes,
	oid.T__bytea:       TArray{Bytes},
	oid.T_date:         Date,
	oid.T__date:        TArray{Date},
	oid.T_time:         Time,
	oid.T__time:        TArray{Time},
	oid.T_float4:       typeFloat4,
	oid.T__float4:      TArray{typeFloat4},
	oid.T_float8:       Float,
	oid.T__float8:      TArray{Float},
	oid.T_int2:         typeInt2,
	oid.T_int4:         typeInt4,
	oid.T_int8:         Int,
	oid.T_int2vector:   IntVector,
	oid.T_interval:     Interval,
	oid.T__interval:    TArray{Interval},
	oid.T_jsonb:        JSON,
	oid.T_name:         Name,
	oid.T__name:        TArray{Name},
	oid.T_numeric:      Decimal,
	oid.T__numeric:     TArray{Decimal},
	oid.T_oid:          Oid,
	oid.T__oid:         TArray{Oid},
	oid.T_oidvector:    OidVector,
	oid.T_regclass:     RegClass,
	oid.T_regnamespace: RegNamespace,
	oid.T_regproc:      RegProc,
	oid.T_regprocedure: RegProcedure,
	oid.T_regtype:      RegType,
	oid.T__text:        TArray{String},
	oid.T__int2:        typeInt2Array,
	oid.T__int4:        typeInt4Array,
	oid.T__int8:        TArray{Int},
	// TODO(jordan): I think this entry for T_record is out of place.
	oid.T_record:       FamTuple,
	oid.T_text:         String,
	oid.T_timestamp:    Timestamp,
	oid.T__timestamp:   TArray{Timestamp},
	oid.T_timestamptz:  TimestampTZ,
	oid.T__timestamptz: TArray{TimestampTZ},
	oid.T_uuid:         UUID,
	oid.T__uuid:        TArray{UUID},
	oid.T_inet:         INet,
	oid.T__inet:        TArray{INet},
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
	oid.T_oidvector:  "oidvector",
	oid.T_text:       "text",
	oid.T_bytea:      "bytea",
	oid.T_varchar:    "varchar",
	oid.T_numeric:    "numeric",
	oid.T_record:     "record",
	oid.T_anyarray:   "anyarray",
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
	oid.T__time:        "_time",
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
	oid.T_anyelement:  oid.T_anyarray,
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
	oid.T_time:        oid.T__time,
	oid.T_timestamp:   oid.T__timestamp,
	oid.T_timestamptz: oid.T__timestamptz,
	oid.T_interval:    oid.T__interval,
	oid.T_numeric:     oid.T__numeric,
	oid.T_uuid:        oid.T__uuid,
}

// PGDisplayName returns the Postgres display name for a given type.
func PGDisplayName(typ T) string {
	if typname, ok := aliasedOidToName[typ.Oid()]; ok {
		return typname
	}
	return typ.String()
}

// TOid represents an alias to the Int type with a different Postgres OID.
type TOid struct {
	oidType oid.Oid
}

func (t TOid) String() string { return t.SQLName() }

// Equivalent implements the T interface.
func (t TOid) Equivalent(other T) bool { return t.FamilyEqual(other) || other == Any }

// FamilyEqual implements the T interface.
func (TOid) FamilyEqual(other T) bool { _, ok := UnwrapType(other).(TOid); return ok }

// Oid implements the T interface.
func (t TOid) Oid() oid.Oid { return t.oidType }

// SQLName implements the T interface.
func (t TOid) SQLName() string {
	switch t.oidType {
	case oid.T_oid:
		return "oid"
	case oid.T_regclass:
		return "regclass"
	case oid.T_regnamespace:
		return "regnamespace"
	case oid.T_regproc:
		return "regproc"
	case oid.T_regprocedure:
		return "regprocedure"
	case oid.T_regtype:
		return "regtype"
	default:
		panic(fmt.Sprintf("unexpected oidType: %v", t.oidType))
	}
}

// IsAmbiguous implements the T interface.
func (TOid) IsAmbiguous() bool { return false }

// TOidWrapper is a T implementation which is a wrapper around a T, allowing
// custom Oid values to be attached to the T. The T is used by DOidWrapper
// to permit type aliasing with custom Oids without needing to create new typing
// rules or define new Datum types.
type TOidWrapper struct {
	T
	oid oid.Oid
}

var customOidNames = map[oid.Oid]string{
	oid.T_name: "name",
}

func (t TOidWrapper) String() string {
	// Allow custom type names for specific Oids, but default to wrapped String.
	if s, ok := customOidNames[t.oid]; ok {
		return s
	}
	return t.T.String()
}

// Oid implements the T interface.
func (t TOidWrapper) Oid() oid.Oid { return t.oid }

// WrapTypeWithOid wraps a T with a custom Oid.
func WrapTypeWithOid(t T, oid oid.Oid) T {
	switch v := t.(type) {
	case tUnknown, tAny, TOidWrapper:
		panic(pgerror.NewErrorf(pgerror.CodeInternalError, "cannot wrap %T with an Oid", v))
	}
	return TOidWrapper{
		T:   t,
		oid: oid,
	}
}

// UnwrapType returns the base T type for a provided type, stripping
// a *TOidWrapper if present. This is useful for cases like type switches,
// where type aliases should be ignored.
func UnwrapType(t T) T {
	if w, ok := t.(TOidWrapper); ok {
		return w.T
	}
	return t
}
