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
	"fmt"

	"github.com/lib/pq/oid"
)

// T represents a SQL type.
type T interface {
	fmt.Stringer

	// Equivalent returns whether the receiver and the other type are equivalent.
	// We say that two type patterns are "equivalent" when they are structurally
	// equivalent given that a wildcard is equivalent to any type. When neither
	// Type is ambiguous (see IsAmbiguous), equivalency is the same as type
	// equality.
	Equivalent(other T) bool

	// Identical return whether the receiver and the other type are identical
	// types. This function should be used in lieu of the == operator.
	Identical(other T) bool

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
	// NULL.
	Unknown T = tUnknown{}
	// Bool is the type of a DBool.
	Bool T = tBool{}
	// BitArray is the type of a DBitArray.
	BitArray T = tBitArray{}
	// Int is the type of a DInt.
	Int T = tInt{}
	// Float is the type of a DFloat.
	Float T = tFloat{}
	// Decimal is the type of a DDecimal.
	Decimal T = tDecimal{}
	// String is the type of a DString.
	String T = tString{}
	// Bytes is the type of a DBytes.
	Bytes T = tBytes{}
	// Date is the type of a DDate.
	Date T = tDate{}
	// Time is the type of a DTime.
	Time T = tTime{}
	// Timestamp is the type of a DTimestamp.
	Timestamp T = tTimestamp{}
	// TimestampTZ is the type of a DTimestampTZ.
	TimestampTZ T = tTimestampTZ{}
	// Interval is the type of a DInterval.
	Interval T = tInterval{}
	// JSON is the type of a DJSON.
	JSON T = tJSON{}
	// UUID is the type of a DUuid.
	UUID T = tUUID{}
	// INet is the type of a DIPAddr.
	INet T = tINet{}
	// AnyArray is the type of a DArray with a wildcard parameterized type.
	AnyArray T = makeTArray(Any)
	// Any can be any type.
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

	// FamCollatedString is the type family of a DString.
	FamCollatedString T = TCollatedString{}
	// FamTuple is the type family of a DTuple.
	FamTuple T = TTuple{}
	// FamArray is the type family of a DArray.
	FamArray T = TArray{}
	// FamPlaceholder is the type family of a placeholder.
	FamPlaceholder T = TPlaceholder{}
)

// Do not instantiate the tXxx types elsewhere. The variables above are intended
// to be singletons.
type tAny struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tAny) String() string             { return "anyelement" }
func (tAny) Identical(other T) bool     { _, ok := other.(tAny); return ok }
func (tAny) Equivalent(other T) bool    { return true }
func (t tAny) FamilyEqual(other T) bool { return t.Identical(other) }
func (tAny) Oid() oid.Oid               { return oid.T_anyelement }
func (tAny) SQLName() string            { return "anyelement" }
func (tAny) IsAmbiguous() bool          { return true }

type tUnknown struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tUnknown) String() string             { return "unknown" }
func (tUnknown) Identical(other T) bool     { _, ok := other.(tUnknown); return ok }
func (t tUnknown) FamilyEqual(other T) bool { return t.Identical(other) }
func (t tUnknown) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tUnknown) Oid() oid.Oid               { return oid.T_unknown }
func (tUnknown) SQLName() string            { return "unknown" }
func (tUnknown) IsAmbiguous() bool          { return true }

type tBool struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tBool) String() string             { return "bool" }
func (tBool) Identical(other T) bool     { _, ok := other.(tBool); return ok }
func (t tBool) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (t tBool) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tBool) Oid() oid.Oid               { return oid.T_bool }
func (tBool) SQLName() string            { return "boolean" }
func (tBool) IsAmbiguous() bool          { return false }

type tInt struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tInt) String() string             { return "int" }
func (tInt) Identical(other T) bool     { _, ok := other.(tInt); return ok }
func (t tInt) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (t tInt) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tInt) Oid() oid.Oid               { return oid.T_int8 }
func (tInt) SQLName() string            { return "bigint" }
func (tInt) IsAmbiguous() bool          { return false }

type tBitArray struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tBitArray) String() string             { return "varbit" }
func (tBitArray) Identical(other T) bool     { _, ok := other.(tBitArray); return ok }
func (t tBitArray) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (t tBitArray) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tBitArray) Oid() oid.Oid               { return oid.T_varbit }
func (tBitArray) SQLName() string            { return "bit varying" }
func (tBitArray) IsAmbiguous() bool          { return false }

type tFloat struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tFloat) String() string             { return "float" }
func (tFloat) Identical(other T) bool     { _, ok := other.(tFloat); return ok }
func (t tFloat) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (t tFloat) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tFloat) Oid() oid.Oid               { return oid.T_float8 }
func (tFloat) SQLName() string            { return "double precision" }
func (tFloat) IsAmbiguous() bool          { return false }

type tDecimal struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tDecimal) String() string             { return "decimal" }
func (tDecimal) Identical(other T) bool     { _, ok := other.(tDecimal); return ok }
func (t tDecimal) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (t tDecimal) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tDecimal) Oid() oid.Oid               { return oid.T_numeric }
func (tDecimal) SQLName() string            { return "numeric" }
func (tDecimal) IsAmbiguous() bool          { return false }

type tString struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tString) String() string             { return "string" }
func (tString) Identical(other T) bool     { _, ok := other.(tString); return ok }
func (t tString) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (t tString) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tString) Oid() oid.Oid               { return oid.T_text }
func (tString) SQLName() string            { return "text" }
func (tString) IsAmbiguous() bool          { return false }

type tBytes struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tBytes) String() string             { return "bytes" }
func (tBytes) Identical(other T) bool     { _, ok := other.(tBytes); return ok }
func (t tBytes) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (t tBytes) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tBytes) Oid() oid.Oid               { return oid.T_bytea }
func (tBytes) SQLName() string            { return "bytea" }
func (tBytes) IsAmbiguous() bool          { return false }

type tDate struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tDate) String() string             { return "date" }
func (tDate) Identical(other T) bool     { _, ok := other.(tDate); return ok }
func (t tDate) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (t tDate) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tDate) Oid() oid.Oid               { return oid.T_date }
func (tDate) SQLName() string            { return "date" }
func (tDate) IsAmbiguous() bool          { return false }

type tTime struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tTime) String() string             { return "time" }
func (tTime) Identical(other T) bool     { _, ok := other.(tTime); return ok }
func (t tTime) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (t tTime) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tTime) Oid() oid.Oid               { return oid.T_time }
func (tTime) SQLName() string            { return "time" }
func (tTime) IsAmbiguous() bool          { return false }

type tTimestamp struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tTimestamp) String() string             { return "timestamp" }
func (tTimestamp) Identical(other T) bool     { _, ok := other.(tTimestamp); return ok }
func (t tTimestamp) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (t tTimestamp) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tTimestamp) Oid() oid.Oid               { return oid.T_timestamp }
func (tTimestamp) SQLName() string            { return "timestamp without time zone" }
func (tTimestamp) IsAmbiguous() bool          { return false }

type tTimestampTZ struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tTimestampTZ) String() string             { return "timestamptz" }
func (tTimestampTZ) Identical(other T) bool     { _, ok := other.(tTimestampTZ); return ok }
func (t tTimestampTZ) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (t tTimestampTZ) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tTimestampTZ) Oid() oid.Oid               { return oid.T_timestamptz }
func (tTimestampTZ) SQLName() string            { return "timestamp with time zone" }
func (tTimestampTZ) IsAmbiguous() bool          { return false }

type tInterval struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tInterval) String() string             { return "interval" }
func (tInterval) Identical(other T) bool     { _, ok := other.(tInterval); return ok }
func (t tInterval) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (tInterval) Oid() oid.Oid               { return oid.T_interval }
func (t tInterval) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tInterval) SQLName() string            { return "interval" }
func (tInterval) IsAmbiguous() bool          { return false }

type tJSON struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tJSON) String() string             { return "jsonb" }
func (tJSON) Identical(other T) bool     { _, ok := other.(tJSON); return ok }
func (t tJSON) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (t tJSON) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tJSON) Oid() oid.Oid               { return oid.T_jsonb }
func (tJSON) SQLName() string            { return "json" }
func (tJSON) IsAmbiguous() bool          { return false }

type tUUID struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tUUID) String() string             { return "uuid" }
func (tUUID) Identical(other T) bool     { _, ok := other.(tUUID); return ok }
func (t tUUID) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (t tUUID) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tUUID) Oid() oid.Oid               { return oid.T_uuid }
func (tUUID) SQLName() string            { return "uuid" }
func (tUUID) IsAmbiguous() bool          { return false }

type tINet struct {
	_ [0][]byte // Prevents use of the == operator.
}

func (tINet) String() string             { return "inet" }
func (tINet) Identical(other T) bool     { _, ok := other.(tINet); return ok }
func (t tINet) FamilyEqual(other T) bool { return t.Identical(UnwrapType(other)) }
func (t tINet) Equivalent(other T) bool  { return t.FamilyEqual(other) || Any.Identical(other) }
func (tINet) Oid() oid.Oid               { return oid.T_inet }
func (tINet) SQLName() string            { return "inet" }
func (tINet) IsAmbiguous() bool          { return false }

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
	return !JSON.Identical(t)
}

// IsDateTimeType returns true if the T is
// date- or time-related type.
func IsDateTimeType(t T) bool {
	switch t.(type) {
	case tDate:
		return true
	case tTime:
		return true
	case tTimestamp:
		return true
	case tTimestampTZ:
		return true
	case tInterval:
		return true
	default:
		return false
	}
}

// IsAdditiveType returns true if the T
// supports addition and subtraction.
func IsAdditiveType(t T) bool {
	switch t.(type) {
	case tInt:
		return true
	case tFloat:
		return true
	case tDecimal:
		return true
	default:
		return IsDateTimeType(t)
	}
}
