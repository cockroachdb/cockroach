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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package parser

import (
	"bytes"
	"fmt"
	"unsafe"
)

// Type represents a SQL type.
type Type interface {
	fmt.Stringer
	// Equal returns whether the receiver and the other type are the same. Prefer
	// == for determining equality with a TypeXxx constant except TypeTuple and
	// TypePlaceholder.
	Equal(other Type) bool
	// FamilyEqual returns whether the receiver and the other type have the same
	// constructor.
	FamilyEqual(other Type) bool

	// Size returns a lower bound on the total size of a Datum whose type is the
	// receiver in bytes, including memory that is pointed at (even if shared
	// between Datum instances) but excluding allocation overhead.
	//
	// The second argument indicates whether data of this type have different
	// sizes.
	//
	// It holds for every Datum d that d.Size() >= d.ResolvedType().Size().
	Size() (uintptr, bool)

	// IsAmbiguous returns whether the type is ambiguous or fully defined. This
	// is important for parameterized types to determine whether they are fully
	// concrete type specification or not.
	IsAmbiguous() bool
}

const (
	fixedSize    = false
	variableSize = true
)

var (
	// TypeNull is the type of a DNull. Can be compared with ==.
	TypeNull Type = tNull{}
	// TypeBool is the type of a DBool. Can be compared with ==.
	TypeBool Type = tBool{}
	// TypeInt is the type of a DInt. Can be compared with ==.
	TypeInt Type = tInt{}
	// TypeFloat is the type of a DFloat. Can be compared with ==.
	TypeFloat Type = tFloat{}
	// TypeDecimal is the type of a DDecimal. Can be compared with ==.
	TypeDecimal Type = tDecimal{}
	// TypeString is the type of a DString. Can be compared with ==.
	TypeString Type = tString{}
	// TypeCollatedString is the type family of a DString. CANNOT be compared with
	// ==.
	TypeCollatedString Type = TCollatedString{}
	// TypeBytes is the type of a DBytes. Can be compared with ==.
	TypeBytes Type = tBytes{}
	// TypeDate is the type of a DDate. Can be compared with ==.
	TypeDate Type = tDate{}
	// TypeTimestamp is the type of a DTimestamp. Can be compared with ==.
	TypeTimestamp Type = tTimestamp{}
	// TypeTimestampTZ is the type of a DTimestampTZ. Can be compared with ==.
	TypeTimestampTZ Type = tTimestampTZ{}
	// TypeInterval is the type of a DInterval. Can be compared with ==.
	TypeInterval Type = tInterval{}
	// TypeTuple is the type family of a DTuple. CANNOT be compared with ==.
	TypeTuple Type = TTuple(nil)
	// TypePlaceholder is the type family of a placeholder. CANNOT be compared
	// with ==.
	TypePlaceholder Type = TPlaceholder{}
	// TypeStringArray is the type family of a DArray containing strings. Can be
	// compared with ==.
	TypeStringArray Type = tArray{TypeString}
	// TypeIntArray is the type family of a DArray containing ints. Can be
	// compared with ==.
	TypeIntArray Type = tArray{TypeInt}
	// TypeAnyArray is the type of a DArray with a wildcard parameterized type.
	// Can be compared with ==.
	TypeAnyArray Type = tArray{TypeAny}
	// TypeAny can be any type. Can be compared with ==.
	TypeAny Type = tAny{}
)

// Do not instantiate the tXxx types elsewhere. The variables above are intended
// to be singletons.
type tNull struct{}

func (tNull) String() string              { return "NULL" }
func (tNull) Equal(other Type) bool       { return other == TypeNull || other == TypeAny }
func (tNull) FamilyEqual(other Type) bool { return other == TypeNull }
func (tNull) Size() (uintptr, bool)       { return unsafe.Sizeof(dNull{}), fixedSize }
func (tNull) IsAmbiguous() bool           { return true }

type tBool struct{}

func (tBool) String() string              { return "bool" }
func (tBool) Equal(other Type) bool       { return other == TypeBool || other == TypeAny }
func (tBool) FamilyEqual(other Type) bool { return other == TypeBool }
func (tBool) Size() (uintptr, bool)       { return unsafe.Sizeof(DBool(false)), fixedSize }
func (tBool) IsAmbiguous() bool           { return false }

type tInt struct{}

func (tInt) String() string              { return "int" }
func (tInt) Equal(other Type) bool       { return other == TypeInt || other == TypeAny }
func (tInt) FamilyEqual(other Type) bool { return other == TypeInt }
func (tInt) Size() (uintptr, bool)       { return unsafe.Sizeof(DInt(0)), fixedSize }
func (tInt) IsAmbiguous() bool           { return false }

type tFloat struct{}

func (tFloat) String() string              { return "float" }
func (tFloat) Equal(other Type) bool       { return other == TypeFloat || other == TypeAny }
func (tFloat) FamilyEqual(other Type) bool { return other == TypeFloat }
func (tFloat) Size() (uintptr, bool)       { return unsafe.Sizeof(DFloat(0.0)), fixedSize }
func (tFloat) IsAmbiguous() bool           { return false }

type tDecimal struct{}

func (tDecimal) String() string              { return "decimal" }
func (tDecimal) Equal(other Type) bool       { return other == TypeDecimal || other == TypeAny }
func (tDecimal) FamilyEqual(other Type) bool { return other == TypeDecimal }
func (tDecimal) Size() (uintptr, bool)       { return unsafe.Sizeof(DDecimal{}), variableSize }
func (tDecimal) IsAmbiguous() bool           { return false }

type tString struct{}

func (tString) String() string              { return "string" }
func (tString) Equal(other Type) bool       { return other == TypeString || other == TypeAny }
func (tString) FamilyEqual(other Type) bool { return other == TypeString }
func (tString) Size() (uintptr, bool)       { return unsafe.Sizeof(DString("")), variableSize }
func (tString) IsAmbiguous() bool           { return false }

// TCollatedString is the type of strings with a locale.
type TCollatedString struct {
	Locale string
}

// String implements the fmt.Stringer interface.
func (t TCollatedString) String() string {
	return fmt.Sprintf("collatedstring{%s}", t.Locale)
}

// Equal implements the Type interface.
func (t TCollatedString) Equal(other Type) bool {
	if other == TypeAny {
		return true
	}
	u, ok := other.(TCollatedString)
	if ok {
		return t.Locale == "" || u.Locale == "" || t.Locale == u.Locale
	}
	return false
}

// FamilyEqual implements the Type interface.
func (TCollatedString) FamilyEqual(other Type) bool {
	_, ok := other.(TCollatedString)
	return ok
}

// Size implements the Type interface.
func (TCollatedString) Size() (uintptr, bool) {
	return unsafe.Sizeof(DCollatedString{"", "", nil}), variableSize
}

// IsAmbiguous implements the Type interface.
func (t TCollatedString) IsAmbiguous() bool {
	return t.Locale == ""
}

type tBytes struct{}

func (tBytes) String() string              { return "bytes" }
func (tBytes) Equal(other Type) bool       { return other == TypeBytes || other == TypeAny }
func (tBytes) FamilyEqual(other Type) bool { return other == TypeBytes }
func (tBytes) Size() (uintptr, bool)       { return unsafe.Sizeof(DBytes("")), variableSize }
func (tBytes) IsAmbiguous() bool           { return false }

type tDate struct{}

func (tDate) String() string              { return "date" }
func (tDate) Equal(other Type) bool       { return other == TypeDate || other == TypeAny }
func (tDate) FamilyEqual(other Type) bool { return other == TypeDate }
func (tDate) Size() (uintptr, bool)       { return unsafe.Sizeof(DDate(0)), fixedSize }
func (tDate) IsAmbiguous() bool           { return false }

type tTimestamp struct{}

func (tTimestamp) String() string              { return "timestamp" }
func (tTimestamp) Equal(other Type) bool       { return other == TypeTimestamp || other == TypeAny }
func (tTimestamp) FamilyEqual(other Type) bool { return other == TypeTimestamp }
func (tTimestamp) Size() (uintptr, bool)       { return unsafe.Sizeof(DTimestamp{}), fixedSize }
func (tTimestamp) IsAmbiguous() bool           { return false }

type tTimestampTZ struct{}

func (tTimestampTZ) String() string              { return "timestamptz" }
func (tTimestampTZ) Equal(other Type) bool       { return other == TypeTimestampTZ || other == TypeAny }
func (tTimestampTZ) FamilyEqual(other Type) bool { return other == TypeTimestampTZ }
func (tTimestampTZ) Size() (uintptr, bool)       { return unsafe.Sizeof(DTimestampTZ{}), fixedSize }
func (tTimestampTZ) IsAmbiguous() bool           { return false }

type tInterval struct{}

func (tInterval) String() string              { return "interval" }
func (tInterval) Equal(other Type) bool       { return other == TypeInterval || other == TypeAny }
func (tInterval) FamilyEqual(other Type) bool { return other == TypeInterval }
func (tInterval) Size() (uintptr, bool)       { return unsafe.Sizeof(DInterval{}), fixedSize }
func (tInterval) IsAmbiguous() bool           { return false }

// TTuple is the type of a DTuple.
type TTuple []Type

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

// Equal implements the Type interface.
func (t TTuple) Equal(other Type) bool {
	if other == TypeAny {
		return true
	}
	u, ok := other.(TTuple)
	if !ok || len(t) != len(u) {
		return false
	}
	for i, typ := range t {
		if !typ.Equal(u[i]) {
			return false
		}
	}
	return true
}

// FamilyEqual implements the Type interface.
func (TTuple) FamilyEqual(other Type) bool {
	_, ok := other.(TTuple)
	return ok
}

// Size implements the Type interface.
func (t TTuple) Size() (uintptr, bool) {
	sz := uintptr(0)
	variable := false
	for _, typ := range t {
		typsz, typvariable := typ.Size()
		sz += typsz
		variable = variable || typvariable
	}
	return sz, variable
}

// IsAmbiguous implements the Type interface.
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

// Equal implements the Type interface.
func (t TPlaceholder) Equal(other Type) bool {
	if other == TypeAny {
		return true
	}
	u, ok := other.(TPlaceholder)
	return ok && t.Name == u.Name
}

// FamilyEqual implements the Type interface.
func (TPlaceholder) FamilyEqual(other Type) bool {
	_, ok := other.(TPlaceholder)
	return ok
}

// Size implements the Type interface.
func (TPlaceholder) Size() (uintptr, bool) { panic("TPlaceholder.Size() is undefined") }

// IsAmbiguous implements the Type interface.
func (TPlaceholder) IsAmbiguous() bool { panic("TPlaceholder.IsAmbiguous() is undefined") }

// TArray is the type of a DArray.
type tArray struct{ Typ Type }

func (a tArray) String() string { return a.Typ.String() + "[]" }

// Equal implements the Type interface.
func (a tArray) Equal(other Type) bool {
	if other == TypeAny {
		return true
	}
	if u, ok := other.(tArray); ok {
		return a.Typ.Equal(u.Typ)
	}
	return false
}

// FamilyEqual implements the Type interface.
func (tArray) FamilyEqual(other Type) bool {
	_, ok := other.(tArray)
	return ok
}

// Size implements the Type interface.
func (tArray) Size() (uintptr, bool) {
	return unsafe.Sizeof(DString("")), variableSize
}

// IsAmbiguous implements the Type interface.
func (a tArray) IsAmbiguous() bool {
	return a.Typ == nil || a.Typ.IsAmbiguous()
}

type tAny struct{}

func (tAny) String() string              { return "anyelement" }
func (tAny) Equal(other Type) bool       { return true }
func (tAny) FamilyEqual(other Type) bool { return other == TypeAny }
func (tAny) Size() (uintptr, bool)       { return unsafe.Sizeof(DString("")), variableSize }
func (tAny) IsAmbiguous() bool           { return true }
