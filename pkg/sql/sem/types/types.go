// Copyright 2015 The Cockroach Authors.
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
	"bytes"
	"fmt"

	"github.com/lib/pq/oid"
)

// T represents a SQL type.
type T interface {
	fmt.Stringer
	// Equivalent returns whether the receiver and the other type are equivalent.
	// We say that two type patterns are "equivalent" when they are structurally
	// equivalent given that a wildcard is equivalent to any type. When neither
	// Type is ambiguous (see IsAmbiguous), equivalency is the same as type equality.
	Equivalent(other T) bool
	// FamilyEqual returns whether the receiver and the other type have the same
	// constructor.
	FamilyEqual(other T) bool

	// Oid returns the type's Postgres object ID.
	Oid() oid.Oid
	// SQLName returns the type's SQL standard name. This can be looked up for a
	// type `t` in postgres by running `SELECT format_type(t::regtype, NULL)`.
	SQLName() string

	// IsAmbiguous returns whether the type is ambiguous or fully defined. This
	// is important for parameterized types to determine whether they are fully
	// concrete type specification or not.
	IsAmbiguous() bool
}

var (
	// Unknown is the type of an expression that statically evaluates to
	// NULL. Can be compared with ==.
	Unknown T = tUnknown{}
	// Bool is the type of a DBool. Can be compared with ==.
	Bool T = tBool{}
	// BitArray is the type of a DBitArray. Can be compared with ==.
	BitArray T = tBitArray{}
	// Int is the type of a DInt. Can be compared with ==.
	Int T = tInt{}
	// Float is the type of a DFloat. Can be compared with ==.
	Float T = tFloat{}
	// Decimal is the type of a DDecimal. Can be compared with ==.
	Decimal T = tDecimal{}
	// String is the type of a DString. Can be compared with ==.
	String T = tString{}
	// Bytes is the type of a DBytes. Can be compared with ==.
	Bytes T = tBytes{}
	// Date is the type of a DDate. Can be compared with ==.
	Date T = tDate{}
	// Time is the type of a DTime. Can be compared with ==.
	Time T = tTime{}
	// Timestamp is the type of a DTimestamp. Can be compared with ==.
	Timestamp T = tTimestamp{}
	// TimestampTZ is the type of a DTimestampTZ. Can be compared with ==.
	TimestampTZ T = tTimestampTZ{}
	// Interval is the type of a DInterval. Can be compared with ==.
	Interval T = tInterval{}
	// JSON is the type of a DJSON. Can be compared with ==.
	JSON T = tJSON{}
	// UUID is the type of a DUuid. Can be compared with ==.
	UUID T = tUUID{}
	// INet is the type of a DIPAddr. Can be compared with ==.
	INet T = tINet{}
	// AnyArray is the type of a DArray with a wildcard parameterized type.
	// Can be compared with ==.
	AnyArray T = TArray{Any}
	// Any can be any type. Can be compared with ==.
	Any T = tAny{}

	// AnyNonArray contains all non-array types.
	AnyNonArray = []T{
		Bool,
		BitArray,
		Int,
		Float,
		Decimal,
		String,
		Bytes,
		Date,
		Time,
		Timestamp,
		TimestampTZ,
		Interval,
		UUID,
		INet,
		JSON,
		Oid,
	}

	// FamCollatedString is the type family of a DString. CANNOT be
	// compared with ==.
	FamCollatedString T = TCollatedString{}
	// FamTuple is the type family of a DTuple. CANNOT be compared with ==.
	FamTuple T = TTuple{}
	// FamArray is the type family of a DArray. CANNOT be compared with ==.
	FamArray T = TArray{}
	// FamPlaceholder is the type family of a placeholder. CANNOT be compared
	// with ==.
	FamPlaceholder T = TPlaceholder{}
)

// Do not instantiate the tXxx types elsewhere. The variables above are intended
// to be singletons.
type tUnknown struct{}

func (tUnknown) String() string           { return "unknown" }
func (tUnknown) Equivalent(other T) bool  { return other == Unknown || other == Any }
func (tUnknown) FamilyEqual(other T) bool { return other == Unknown }
func (tUnknown) Oid() oid.Oid             { return oid.T_unknown }
func (tUnknown) SQLName() string          { return "unknown" }
func (tUnknown) IsAmbiguous() bool        { return true }

type tBool struct{}

func (tBool) String() string           { return "bool" }
func (tBool) Equivalent(other T) bool  { return UnwrapType(other) == Bool || other == Any }
func (tBool) FamilyEqual(other T) bool { return UnwrapType(other) == Bool }
func (tBool) Oid() oid.Oid             { return oid.T_bool }
func (tBool) SQLName() string          { return "boolean" }
func (tBool) IsAmbiguous() bool        { return false }

type tInt struct{}

func (tInt) String() string           { return "int" }
func (tInt) Equivalent(other T) bool  { return UnwrapType(other) == Int || other == Any }
func (tInt) FamilyEqual(other T) bool { return UnwrapType(other) == Int }
func (tInt) Oid() oid.Oid             { return oid.T_int8 }
func (tInt) SQLName() string          { return "bigint" }
func (tInt) IsAmbiguous() bool        { return false }

type tBitArray struct{}

func (tBitArray) String() string           { return "varbit" }
func (tBitArray) Equivalent(other T) bool  { return UnwrapType(other) == BitArray || other == Any }
func (tBitArray) FamilyEqual(other T) bool { return UnwrapType(other) == BitArray }
func (tBitArray) Oid() oid.Oid             { return oid.T_varbit }
func (tBitArray) SQLName() string          { return "bit varying" }
func (tBitArray) IsAmbiguous() bool        { return false }

type tFloat struct{}

func (tFloat) String() string           { return "float" }
func (tFloat) Equivalent(other T) bool  { return UnwrapType(other) == Float || other == Any }
func (tFloat) FamilyEqual(other T) bool { return UnwrapType(other) == Float }
func (tFloat) Oid() oid.Oid             { return oid.T_float8 }
func (tFloat) SQLName() string          { return "double precision" }
func (tFloat) IsAmbiguous() bool        { return false }

type tDecimal struct{}

func (tDecimal) String() string { return "decimal" }
func (tDecimal) Equivalent(other T) bool {
	return UnwrapType(other) == Decimal || other == Any
}

func (tDecimal) FamilyEqual(other T) bool { return UnwrapType(other) == Decimal }
func (tDecimal) Oid() oid.Oid             { return oid.T_numeric }
func (tDecimal) SQLName() string          { return "numeric" }
func (tDecimal) IsAmbiguous() bool        { return false }

type tString struct{}

func (tString) String() string           { return "string" }
func (tString) Equivalent(other T) bool  { return UnwrapType(other) == String || other == Any }
func (tString) FamilyEqual(other T) bool { return UnwrapType(other) == String }
func (tString) Oid() oid.Oid             { return oid.T_text }
func (tString) SQLName() string          { return "text" }
func (tString) IsAmbiguous() bool        { return false }

// TCollatedString is the type of strings with a locale.
type TCollatedString struct {
	Locale string
}

// String implements the fmt.Stringer interface.
func (t TCollatedString) String() string {
	return fmt.Sprintf("collatedstring{%s}", t.Locale)
}

// Equivalent implements the T interface.
func (t TCollatedString) Equivalent(other T) bool {
	if other == Any {
		return true
	}
	u, ok := UnwrapType(other).(TCollatedString)
	if ok {
		return t.Locale == "" || u.Locale == "" || t.Locale == u.Locale
	}
	return false
}

// FamilyEqual implements the T interface.
func (TCollatedString) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TCollatedString)
	return ok
}

// Oid implements the T interface.
func (TCollatedString) Oid() oid.Oid { return oid.T_text }

// SQLName implements the T interface.
func (TCollatedString) SQLName() string { return "text" }

// IsAmbiguous implements the T interface.
func (t TCollatedString) IsAmbiguous() bool {
	return t.Locale == ""
}

type tBytes struct{}

func (tBytes) String() string           { return "bytes" }
func (tBytes) Equivalent(other T) bool  { return UnwrapType(other) == Bytes || other == Any }
func (tBytes) FamilyEqual(other T) bool { return UnwrapType(other) == Bytes }
func (tBytes) Oid() oid.Oid             { return oid.T_bytea }
func (tBytes) SQLName() string          { return "bytea" }
func (tBytes) IsAmbiguous() bool        { return false }

type tDate struct{}

func (tDate) String() string           { return "date" }
func (tDate) Equivalent(other T) bool  { return UnwrapType(other) == Date || other == Any }
func (tDate) FamilyEqual(other T) bool { return UnwrapType(other) == Date }
func (tDate) Oid() oid.Oid             { return oid.T_date }
func (tDate) SQLName() string          { return "date" }
func (tDate) IsAmbiguous() bool        { return false }

type tTime struct{}

func (tTime) String() string           { return "time" }
func (tTime) Equivalent(other T) bool  { return UnwrapType(other) == Time || other == Any }
func (tTime) FamilyEqual(other T) bool { return UnwrapType(other) == Time }
func (tTime) Oid() oid.Oid             { return oid.T_time }
func (tTime) SQLName() string          { return "time" }
func (tTime) IsAmbiguous() bool        { return false }

type tTimestamp struct{}

func (tTimestamp) String() string { return "timestamp" }
func (tTimestamp) Equivalent(other T) bool {
	return UnwrapType(other) == Timestamp || other == Any
}

func (tTimestamp) FamilyEqual(other T) bool { return UnwrapType(other) == Timestamp }
func (tTimestamp) Oid() oid.Oid             { return oid.T_timestamp }
func (tTimestamp) SQLName() string          { return "timestamp without time zone" }
func (tTimestamp) IsAmbiguous() bool        { return false }

type tTimestampTZ struct{}

func (tTimestampTZ) String() string { return "timestamptz" }
func (tTimestampTZ) Equivalent(other T) bool {
	return UnwrapType(other) == TimestampTZ || other == Any
}

func (tTimestampTZ) FamilyEqual(other T) bool { return UnwrapType(other) == TimestampTZ }
func (tTimestampTZ) Oid() oid.Oid             { return oid.T_timestamptz }
func (tTimestampTZ) SQLName() string          { return "timestamp with time zone" }
func (tTimestampTZ) IsAmbiguous() bool        { return false }

type tInterval struct{}

func (tInterval) String() string { return "interval" }
func (tInterval) Equivalent(other T) bool {
	return UnwrapType(other) == Interval || other == Any
}

func (tInterval) FamilyEqual(other T) bool { return UnwrapType(other) == Interval }
func (tInterval) Oid() oid.Oid             { return oid.T_interval }
func (tInterval) SQLName() string          { return "interval" }
func (tInterval) IsAmbiguous() bool        { return false }

type tJSON struct{}

func (tJSON) String() string { return "jsonb" }
func (tJSON) Equivalent(other T) bool {
	return UnwrapType(other) == JSON || other == Any
}

func (tJSON) FamilyEqual(other T) bool { return UnwrapType(other) == JSON }
func (tJSON) Oid() oid.Oid             { return oid.T_jsonb }
func (tJSON) SQLName() string          { return "json" }
func (tJSON) IsAmbiguous() bool        { return false }

type tUUID struct{}

func (tUUID) String() string           { return "uuid" }
func (tUUID) Equivalent(other T) bool  { return UnwrapType(other) == UUID || other == Any }
func (tUUID) FamilyEqual(other T) bool { return UnwrapType(other) == UUID }
func (tUUID) Oid() oid.Oid             { return oid.T_uuid }
func (tUUID) SQLName() string          { return "uuid" }
func (tUUID) IsAmbiguous() bool        { return false }

type tINet struct{}

func (tINet) String() string           { return "inet" }
func (tINet) Equivalent(other T) bool  { return UnwrapType(other) == INet || other == Any }
func (tINet) FamilyEqual(other T) bool { return UnwrapType(other) == INet }
func (tINet) Oid() oid.Oid             { return oid.T_inet }
func (tINet) SQLName() string          { return "inet" }
func (tINet) IsAmbiguous() bool        { return false }

// TTuple is the type of a DTuple.
type TTuple struct {
	Types  []T
	Labels []string
}

// String implements the fmt.Stringer interface.
func (t TTuple) String() string {
	var buf bytes.Buffer
	buf.WriteString("tuple")
	if t.Types != nil {
		buf.WriteByte('{')
		for i, typ := range t.Types {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(typ.String())
			if t.Labels != nil {
				buf.WriteString(" AS ")
				buf.WriteString(t.Labels[i])
			}
		}
		buf.WriteByte('}')
	}
	return buf.String()
}

// Equivalent implements the T interface.
func (t TTuple) Equivalent(other T) bool {
	if other == Any {
		return true
	}
	u, ok := UnwrapType(other).(TTuple)
	if !ok {
		return false
	}
	if len(t.Types) == 0 || len(u.Types) == 0 {
		// Tuples that aren't fully specified (have a nil subtype list) are always
		// equivalent to other tuples, to allow overloads to specify that they take
		// an arbitrary tuple type.
		return true
	}
	if len(t.Types) != len(u.Types) {
		return false
	}
	for i, typ := range t.Types {
		if !typ.Equivalent(u.Types[i]) {
			return false
		}
	}
	return true
}

// FamilyEqual implements the T interface.
func (TTuple) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TTuple)
	return ok
}

// Oid implements the T interface.
func (TTuple) Oid() oid.Oid { return oid.T_record }

// SQLName implements the T interface.
func (TTuple) SQLName() string { return "record" }

// IsAmbiguous implements the T interface.
func (t TTuple) IsAmbiguous() bool {
	for _, typ := range t.Types {
		if typ == nil || typ.IsAmbiguous() {
			return true
		}
	}
	return len(t.Types) == 0
}

// TPlaceholder is the type of a placeholder.
type TPlaceholder struct {
	Name string
}

// String implements the fmt.Stringer interface.
func (t TPlaceholder) String() string { return fmt.Sprintf("placeholder{%s}", t.Name) }

// Equivalent implements the T interface.
func (t TPlaceholder) Equivalent(other T) bool {
	if other == Any {
		return true
	}
	if other.IsAmbiguous() {
		return true
	}
	u, ok := UnwrapType(other).(TPlaceholder)
	return ok && t.Name == u.Name
}

// FamilyEqual implements the T interface.
func (TPlaceholder) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TPlaceholder)
	return ok
}

// Oid implements the T interface.
func (TPlaceholder) Oid() oid.Oid { panic("TPlaceholder.Oid() is undefined") }

// SQLName implements the T interface.
func (TPlaceholder) SQLName() string { panic("TPlaceholder.SQLName() is undefined") }

// IsAmbiguous implements the T interface.
func (TPlaceholder) IsAmbiguous() bool { panic("TPlaceholder.IsAmbiguous() is undefined") }

// TArray is the type of a DArray.
type TArray struct{ Typ T }

func (a TArray) String() string { return a.Typ.String() + "[]" }

// Equivalent implements the T interface.
func (a TArray) Equivalent(other T) bool {
	if other == Any {
		return true
	}
	if u, ok := UnwrapType(other).(TArray); ok {
		return a.Typ.Equivalent(u.Typ)
	}
	return false
}

// FamilyEqual implements the T interface.
func (TArray) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TArray)
	return ok
}

const noArrayType = 0

// ArrayOids is a set of all oids which correspond to an array type.
var ArrayOids = map[oid.Oid]struct{}{}

func init() {
	for _, v := range oidToArrayOid {
		ArrayOids[v] = struct{}{}
	}
}

// Oid implements the T interface.
func (a TArray) Oid() oid.Oid {
	if o, ok := oidToArrayOid[a.Typ.Oid()]; ok {
		return o
	}
	return noArrayType
}

// SQLName implements the T interface.
func (a TArray) SQLName() string {
	return a.Typ.SQLName() + "[]"
}

// IsAmbiguous implements the T interface.
func (a TArray) IsAmbiguous() bool {
	return a.Typ == nil || a.Typ.IsAmbiguous()
}

type tAny struct{}

func (tAny) String() string           { return "anyelement" }
func (tAny) Equivalent(other T) bool  { return true }
func (tAny) FamilyEqual(other T) bool { return other == Any }
func (tAny) Oid() oid.Oid             { return oid.T_anyelement }
func (tAny) SQLName() string          { return "anyelement" }
func (tAny) IsAmbiguous() bool        { return true }

// IsStringType returns true iff t is String
// or a collated string type.
func IsStringType(t T) bool {
	switch t.(type) {
	case tString, TCollatedString:
		return true
	default:
		return false
	}
}

// IsValidArrayElementType returns true if the T
// can be used in TArray.
func IsValidArrayElementType(t T) bool {
	switch t {
	case JSON:
		return false
	default:
		return true
	}
}

// IsDateTimeType returns true if the T is
// date- or time-related type.
func IsDateTimeType(t T) bool {
	switch t {
	case Date:
		return true
	case Time:
		return true
	case Timestamp:
		return true
	case TimestampTZ:
		return true
	case Interval:
		return true
	default:
		return false
	}
}

// IsAdditiveType returns true if the T
// supports addition and subtraction.
func IsAdditiveType(t T) bool {
	switch t {
	case Int:
		return true
	case Float:
		return true
	case Decimal:
		return true
	default:
		return IsDateTimeType(t)
	}
}
