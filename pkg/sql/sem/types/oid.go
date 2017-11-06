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
	// TypeOid is the type of an OID. Can be compared with ==.
	TypeOid = TOid{oid.T_oid}
	// TypeRegClass is the type of an regclass OID variant. Can be compared with ==.
	TypeRegClass = TOid{oid.T_regclass}
	// TypeRegNamespace is the type of an regnamespace OID variant. Can be compared with ==.
	TypeRegNamespace = TOid{oid.T_regnamespace}
	// TypeRegProc is the type of an regproc OID variant. Can be compared with ==.
	TypeRegProc = TOid{oid.T_regproc}
	// TypeRegProcedure is the type of an regprocedure OID variant. Can be compared with ==.
	TypeRegProcedure = TOid{oid.T_regprocedure}
	// TypeRegType is the type of an regtype OID variant. Can be compared with ==.
	TypeRegType = TOid{oid.T_regtype}

	// TypeName is a type-alias for TypeString with a different OID. Can be
	// compared with ==.
	TypeName = WrapTypeWithOid(TypeString, oid.T_name)
	// TypeIntVector is a type-alias for a TypeIntArray with a different OID. Can
	// be compared with ==.
	TypeIntVector = WrapTypeWithOid(TArray{TypeInt}, oid.T_int2vector)
	// TypeNameArray is the type family of a DArray containing the Name alias type.
	// Can be compared with ==.
	TypeNameArray Type = TArray{TypeName}
)

var (
	// Unexported wrapper types. These exist for Postgres type compatibility.
	typeInt2      = WrapTypeWithOid(TypeInt, oid.T_int2)
	typeInt4      = WrapTypeWithOid(TypeInt, oid.T_int4)
	typeFloat4    = WrapTypeWithOid(TypeFloat, oid.T_float4)
	typeVarChar   = WrapTypeWithOid(TypeString, oid.T_varchar)
	typeInt2Array = TArray{typeInt2}
	typeInt4Array = TArray{typeInt4}
)

// OidToType maps Postgres object IDs to CockroachDB types.  We export
// the map instead of a method so that other packages can iterate over
// the map directly.
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

// TOid represents one of the OID type variants.
type TOid struct {
	oidType oid.Oid
}

var oidTypeName = map[oid.Oid]string{
	oid.T_oid:          "oid",
	oid.T_regclass:     "regclass",
	oid.T_regnamespace: "regnamespace",
	oid.T_regproc:      "regproc",
	oid.T_regprocedure: "regprocedure",
	oid.T_regtype:      "regtype",
}

func (t TOid) String() string { return SQLName(t) }

// Equivalent implements the Type interface.
func (t TOid) Equivalent(other Type) bool { return t.FamilyEqual(other) || other == TypeAny }

// FamilyEqual implements the Type interface.
func (TOid) FamilyEqual(other Type) bool { _, ok := UnwrapType(other).(TOid); return ok }

// IsAmbiguous implements the Type interface.
func (TOid) IsAmbiguous() bool { return false }

// TOidWrapper is a Type implementation which is a wrapper around a Type, allowing
// custom Oid values to be attached to the Type. The Type is used by DOidWrapper
// to permit type aliasing with custom Oids without needing to create new typing
// rules or define new Datum types.
type TOidWrapper struct {
	Type
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
	return t.Type.String()
}

// WrapTypeWithOid wraps a Type with a custom Oid.
func WrapTypeWithOid(t Type, oid oid.Oid) Type {
	switch v := t.(type) {
	case tNull, tAny, TOidWrapper:
		panic(pgerror.NewErrorf(pgerror.CodeInternalError, "cannot wrap %T with an Oid", v))
	}
	return TOidWrapper{
		Type: t,
		oid:  oid,
	}
}

// UnwrapType returns the base Type type for a provided type, stripping
// a *TOidWrapper if present. This is useful for cases like type switches,
// where type aliases should be ignored.
func UnwrapType(t Type) Type {
	if w, ok := t.(TOidWrapper); ok {
		return w.Type
	}
	return t
}

var baseTypeOids = map[Type]oid.Oid{
	TypeNull:        oid.T_unknown,
	TypeBool:        oid.T_bool,
	TypeInt:         oid.T_int8,
	TypeFloat:       oid.T_float8,
	TypeDecimal:     oid.T_numeric,
	TypeString:      oid.T_text,
	TypeBytes:       oid.T_bytea,
	TypeDate:        oid.T_date,
	TypeTimestamp:   oid.T_timestamp,
	TypeTimestampTZ: oid.T_timestamptz,
	TypeInterval:    oid.T_interval,
	TypeJSON:        oid.T_jsonb,
	TypeUUID:        oid.T_uuid,
	TypeINet:        oid.T_inet,
	TypeAny:         oid.T_anyelement,
}

const noArrayType = 0

// ArrayOids is a set of all oids which correspond to an array type.
var ArrayOids = map[oid.Oid]struct{}{}

func init() {
	for _, v := range oidToArrayOid {
		ArrayOids[v] = struct{}{}
	}
}

// Oid returns the type's Postgres object ID.
func Oid(t Type) oid.Oid {
	// Compound types.
	switch ty := t.(type) {
	case TOid:
		return ty.oidType
	case TOidWrapper:
		return ty.oid
	case TCollatedString:
		return oid.T_unknown
	case TTuple:
		return oid.T_record
	case TTable:
		return oid.T_anyelement
	case TArray:
		if o, ok := oidToArrayOid[Oid(ty.Typ)]; ok {
			return o
		}
		// TODO(jordan,justin): should this not be T_unknown?
		return noArrayType
	}
	// Base types.
	if v, ok := baseTypeOids[t]; ok {
		return v
	}
	panic(fmt.Sprintf("no oid for type %v (%T)", t, t))
}

// PGDisplayName returns the Postgres display name for a given type.
func PGDisplayName(typ Type) string {
	if typname, ok := aliasedOidToName[Oid(typ)]; ok {
		return typname
	}
	return typ.String()
}
