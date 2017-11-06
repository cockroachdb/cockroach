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

	// IsAmbiguous returns whether the type is ambiguous or fully defined. This
	// is important for parameterized types to determine whether they are fully
	// concrete type specification or not.
	IsAmbiguous() bool
}

var (
	// TypeNull is the type of a DNull. Can be compared with ==.
	TypeNull T = tNull{}
	// TypeBool is the type of a DBool. Can be compared with ==.
	TypeBool T = tBool{}
	// TypeInt is the type of a DInt. Can be compared with ==.
	TypeInt T = tInt{}
	// TypeFloat is the type of a DFloat. Can be compared with ==.
	TypeFloat T = tFloat{}
	// TypeDecimal is the type of a DDecimal. Can be compared with ==.
	TypeDecimal T = tDecimal{}
	// TypeString is the type of a DString. Can be compared with ==.
	TypeString T = tString{}
	// TypeBytes is the type of a DBytes. Can be compared with ==.
	TypeBytes T = tBytes{}
	// TypeDate is the type of a DDate. Can be compared with ==.
	TypeDate T = tDate{}
	// TypeTimestamp is the type of a DTimestamp. Can be compared with ==.
	TypeTimestamp T = tTimestamp{}
	// TypeTimestampTZ is the type of a DTimestampTZ. Can be compared with ==.
	TypeTimestampTZ T = tTimestampTZ{}
	// TypeInterval is the type of a DInterval. Can be compared with ==.
	TypeInterval T = tInterval{}
	// TypeJSON is the type of a DJSON. Can be compared with ==.
	TypeJSON T = tJSON{}
	// TypeUUID is the type of a DUuid. Can be compared with ==.
	TypeUUID T = tUUID{}
	// TypeINet is the type of a DIPAddr. Can be compared with ==.
	TypeINet T = tINet{}
	// TypeAnyArray is the type of a DArray with a wildcard parameterized type.
	// Can be compared with ==.
	TypeAnyArray T = TArray{TypeAny}
	// TypeAny can be any type. Can be compared with ==.
	TypeAny T = tAny{}

	// TypesAnyNonArray contains all non-array types.
	TypesAnyNonArray = []T{
		TypeBool,
		TypeInt,
		TypeFloat,
		TypeDecimal,
		TypeString,
		TypeBytes,
		TypeDate,
		TypeTimestamp,
		TypeTimestampTZ,
		TypeInterval,
		TypeUUID,
		TypeINet,
		TypeJSON,
		TypeOid,
	}

	// FamCollatedString is the type family of a DString. CANNOT be
	// compared with ==.
	FamCollatedString T = TCollatedString{}
	// FamTuple is the type family of a DTuple. CANNOT be compared with ==.
	FamTuple T = TTuple(nil)
	// FamArray is the type family of a DArray. CANNOT be compared with ==.
	FamArray T = TArray{}
	// FamTable is the type family of a DTable. CANNOT be compared with ==.
	FamTable T = TTable{}
	// FamPlaceholder is the type family of a placeholder. CANNOT be compared
	// with ==.
	FamPlaceholder T = TPlaceholder{}
)

// Do not instantiate the tXxx types elsewhere. The variables above are intended
// to be singletons.
type tNull struct{}

func (tNull) String() string           { return "NULL" }
func (tNull) Equivalent(other T) bool  { return other == TypeNull || other == TypeAny }
func (tNull) FamilyEqual(other T) bool { return other == TypeNull }
func (tNull) IsAmbiguous() bool        { return true }

type tBool struct{}

func (tBool) String() string           { return "bool" }
func (tBool) Equivalent(other T) bool  { return UnwrapType(other) == TypeBool || other == TypeAny }
func (tBool) FamilyEqual(other T) bool { return UnwrapType(other) == TypeBool }
func (tBool) IsAmbiguous() bool        { return false }

type tInt struct{}

func (tInt) String() string           { return "int" }
func (tInt) Equivalent(other T) bool  { return UnwrapType(other) == TypeInt || other == TypeAny }
func (tInt) FamilyEqual(other T) bool { return UnwrapType(other) == TypeInt }
func (tInt) IsAmbiguous() bool        { return false }

type tFloat struct{}

func (tFloat) String() string           { return "float" }
func (tFloat) Equivalent(other T) bool  { return UnwrapType(other) == TypeFloat || other == TypeAny }
func (tFloat) FamilyEqual(other T) bool { return UnwrapType(other) == TypeFloat }
func (tFloat) IsAmbiguous() bool        { return false }

type tDecimal struct{}

func (tDecimal) String() string { return "decimal" }
func (tDecimal) Equivalent(other T) bool {
	return UnwrapType(other) == TypeDecimal || other == TypeAny
}
func (tDecimal) FamilyEqual(other T) bool { return UnwrapType(other) == TypeDecimal }
func (tDecimal) IsAmbiguous() bool        { return false }

type tString struct{}

func (tString) String() string           { return "string" }
func (tString) Equivalent(other T) bool  { return UnwrapType(other) == TypeString || other == TypeAny }
func (tString) FamilyEqual(other T) bool { return UnwrapType(other) == TypeString }
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
	if other == TypeAny {
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

// IsAmbiguous implements the T interface.
func (t TCollatedString) IsAmbiguous() bool {
	return t.Locale == ""
}

type tBytes struct{}

func (tBytes) String() string           { return "bytes" }
func (tBytes) Equivalent(other T) bool  { return UnwrapType(other) == TypeBytes || other == TypeAny }
func (tBytes) FamilyEqual(other T) bool { return UnwrapType(other) == TypeBytes }
func (tBytes) IsAmbiguous() bool        { return false }

type tDate struct{}

func (tDate) String() string           { return "date" }
func (tDate) Equivalent(other T) bool  { return UnwrapType(other) == TypeDate || other == TypeAny }
func (tDate) FamilyEqual(other T) bool { return UnwrapType(other) == TypeDate }
func (tDate) IsAmbiguous() bool        { return false }

type tTimestamp struct{}

func (tTimestamp) String() string { return "timestamp" }
func (tTimestamp) Equivalent(other T) bool {
	return UnwrapType(other) == TypeTimestamp || other == TypeAny
}
func (tTimestamp) FamilyEqual(other T) bool { return UnwrapType(other) == TypeTimestamp }
func (tTimestamp) IsAmbiguous() bool        { return false }

type tTimestampTZ struct{}

func (tTimestampTZ) String() string { return "timestamptz" }
func (tTimestampTZ) Equivalent(other T) bool {
	return UnwrapType(other) == TypeTimestampTZ || other == TypeAny
}
func (tTimestampTZ) FamilyEqual(other T) bool { return UnwrapType(other) == TypeTimestampTZ }
func (tTimestampTZ) IsAmbiguous() bool        { return false }

type tInterval struct{}

func (tInterval) String() string { return "interval" }
func (tInterval) Equivalent(other T) bool {
	return UnwrapType(other) == TypeInterval || other == TypeAny
}
func (tInterval) FamilyEqual(other T) bool { return UnwrapType(other) == TypeInterval }
func (tInterval) IsAmbiguous() bool        { return false }

type tJSON struct{}

func (tJSON) String() string { return "jsonb" }
func (tJSON) Equivalent(other T) bool {
	return UnwrapType(other) == TypeJSON || other == TypeAny
}
func (tJSON) FamilyEqual(other T) bool { return UnwrapType(other) == TypeJSON }
func (tJSON) IsAmbiguous() bool        { return false }

type tUUID struct{}

func (tUUID) String() string           { return "uuid" }
func (tUUID) Equivalent(other T) bool  { return UnwrapType(other) == TypeUUID || other == TypeAny }
func (tUUID) FamilyEqual(other T) bool { return UnwrapType(other) == TypeUUID }
func (tUUID) IsAmbiguous() bool        { return false }

type tINet struct{}

func (tINet) String() string           { return "inet" }
func (tINet) Equivalent(other T) bool  { return UnwrapType(other) == TypeINet || other == TypeAny }
func (tINet) FamilyEqual(other T) bool { return UnwrapType(other) == TypeINet }
func (tINet) IsAmbiguous() bool        { return false }

// TTuple is the type of a DTuple.
type TTuple []T

// String implements the fmt.Stringer interface.
func (t TTuple) String() string {
	var buf bytes.Buffer
	buf.WriteString("tuple")
	if t != nil {
		buf.WriteByte('{')
		for i, typ := range t {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(typ.String())
		}
		buf.WriteByte('}')
	}
	return buf.String()
}

// Equivalent implements the T interface.
func (t TTuple) Equivalent(other T) bool {
	if other == TypeAny {
		return true
	}
	u, ok := UnwrapType(other).(TTuple)
	if !ok || len(t) != len(u) {
		return false
	}
	for i, typ := range t {
		if !typ.Equivalent(u[i]) {
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

// IsAmbiguous implements the T interface.
func (t TTuple) IsAmbiguous() bool {
	for _, typ := range t {
		if typ == nil || typ.IsAmbiguous() {
			return true
		}
	}
	return false
}

// TPlaceholder is the type of a placeholder.
type TPlaceholder struct {
	Name string
}

// String implements the fmt.Stringer interface.
func (t TPlaceholder) String() string { return fmt.Sprintf("placeholder{%s}", t.Name) }

// Equivalent implements the T interface.
func (t TPlaceholder) Equivalent(other T) bool {
	if other == TypeAny {
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

// IsAmbiguous implements the T interface.
func (TPlaceholder) IsAmbiguous() bool { panic("TPlaceholder.IsAmbiguous() is undefined") }

// TArray is the type of a DArray.
type TArray struct{ Typ T }

func (a TArray) String() string { return a.Typ.String() + "[]" }

// Equivalent implements the T interface.
func (a TArray) Equivalent(other T) bool {
	if other == TypeAny {
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

// IsAmbiguous implements the T interface.
func (a TArray) IsAmbiguous() bool {
	return a.Typ == nil || a.Typ.IsAmbiguous()
}

// TTable is the type of a DTable.
// See the comments at the start of generator_builtins.go for details.
type TTable struct {
	Cols   TTuple
	Labels []string
}

func (a TTable) String() string { return "setof " + a.Cols.String() }

// Equivalent implements the T interface.
func (a TTable) Equivalent(other T) bool {
	if u, ok := UnwrapType(other).(TTable); ok {
		return a.Cols.Equivalent(u.Cols)
	}
	return false
}

// FamilyEqual implements the T interface.
func (TTable) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TTable)
	return ok
}

// IsAmbiguous implements the T interface.
func (a TTable) IsAmbiguous() bool {
	return a.Cols == nil || a.Cols.IsAmbiguous()
}

type tAny struct{}

func (tAny) String() string           { return "anyelement" }
func (tAny) Equivalent(other T) bool  { return true }
func (tAny) FamilyEqual(other T) bool { return other == TypeAny }
func (tAny) IsAmbiguous() bool        { return true }

// IsStringType returns true iff t is TypeString
// or a collated string type.
func IsStringType(t T) bool {
	switch t.(type) {
	case tString, TCollatedString:
		return true
	default:
		return false
	}
}
