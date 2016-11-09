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
	// TypeAny can be any type. Can be compared with ==.
	TypeAny Type = tAny{}
)

// Do not instantiate the tXxx types elsewhere. The variables above are intended
// to be singletons.
type tNull struct{}

func (tNull) String() string              { return "NULL" }
func (tNull) Equal(other Type) bool       { return other == TypeNull }
func (tNull) FamilyEqual(other Type) bool { return other == TypeNull }
func (tNull) Size() (uintptr, bool)       { return unsafe.Sizeof(dNull{}), fixedSize }

type tBool struct{}

func (tBool) String() string              { return "bool" }
func (tBool) Equal(other Type) bool       { return other == TypeBool }
func (tBool) FamilyEqual(other Type) bool { return other == TypeBool }
func (tBool) Size() (uintptr, bool)       { return unsafe.Sizeof(DBool(false)), fixedSize }

type tInt struct{}

func (tInt) String() string              { return "int" }
func (tInt) Equal(other Type) bool       { return other == TypeInt }
func (tInt) FamilyEqual(other Type) bool { return other == TypeInt }
func (tInt) Size() (uintptr, bool)       { return unsafe.Sizeof(DInt(0)), fixedSize }

type tFloat struct{}

func (tFloat) String() string              { return "float" }
func (tFloat) Equal(other Type) bool       { return other == TypeFloat }
func (tFloat) FamilyEqual(other Type) bool { return other == TypeFloat }
func (tFloat) Size() (uintptr, bool)       { return unsafe.Sizeof(DFloat(0.0)), fixedSize }

type tDecimal struct{}

func (tDecimal) String() string              { return "decimal" }
func (tDecimal) Equal(other Type) bool       { return other == TypeDecimal }
func (tDecimal) FamilyEqual(other Type) bool { return other == TypeDecimal }
func (tDecimal) Size() (uintptr, bool)       { return unsafe.Sizeof(DDecimal{}), variableSize }

type tString struct{}

func (tString) String() string              { return "string" }
func (tString) Equal(other Type) bool       { return other == TypeString }
func (tString) FamilyEqual(other Type) bool { return other == TypeString }
func (tString) Size() (uintptr, bool)       { return unsafe.Sizeof(DString("")), variableSize }

type tBytes struct{}

func (tBytes) String() string              { return "bytes" }
func (tBytes) Equal(other Type) bool       { return other == TypeBytes }
func (tBytes) FamilyEqual(other Type) bool { return other == TypeBytes }
func (tBytes) Size() (uintptr, bool)       { return unsafe.Sizeof(DBytes("")), variableSize }

type tDate struct{}

func (tDate) String() string              { return "date" }
func (tDate) Equal(other Type) bool       { return other == TypeDate }
func (tDate) FamilyEqual(other Type) bool { return other == TypeDate }
func (tDate) Size() (uintptr, bool)       { return unsafe.Sizeof(DDate(0)), fixedSize }

type tTimestamp struct{}

func (tTimestamp) String() string              { return "timestamp" }
func (tTimestamp) Equal(other Type) bool       { return other == TypeTimestamp }
func (tTimestamp) FamilyEqual(other Type) bool { return other == TypeTimestamp }
func (tTimestamp) Size() (uintptr, bool)       { return unsafe.Sizeof(DTimestamp{}), fixedSize }

type tTimestampTZ struct{}

func (tTimestampTZ) String() string              { return "timestamptz" }
func (tTimestampTZ) Equal(other Type) bool       { return other == TypeTimestampTZ }
func (tTimestampTZ) FamilyEqual(other Type) bool { return other == TypeTimestampTZ }
func (tTimestampTZ) Size() (uintptr, bool)       { return unsafe.Sizeof(DTimestampTZ{}), fixedSize }

type tInterval struct{}

func (tInterval) String() string              { return "interval" }
func (tInterval) Equal(other Type) bool       { return other == TypeInterval }
func (tInterval) FamilyEqual(other Type) bool { return other == TypeInterval }
func (tInterval) Size() (uintptr, bool)       { return unsafe.Sizeof(DInterval{}), fixedSize }

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

// TPlaceholder is the type of a placeholder.
type TPlaceholder struct {
	Name string
}

// String implements the fmt.Stringer interface.
func (t TPlaceholder) String() string { return fmt.Sprintf("placeholder{%s}", t.Name) }

// Equal implements the Type interface.
func (t TPlaceholder) Equal(other Type) bool {
	u, ok := other.(*TPlaceholder)
	return ok && t.Name == u.Name
}

// FamilyEqual implements the Type interface.
func (TPlaceholder) FamilyEqual(other Type) bool {
	_, ok := other.(*TPlaceholder)
	return ok
}

// Size implements the Type interface.
func (t TPlaceholder) Size() (uintptr, bool) { panic("TPlaceholder.Size() is undefined") }

// TArray is the type of a DArray.
type tArray struct{ Typ Type }

func (a tArray) String() string { return a.Typ.String() + "[]" }

// Equal implements the Type interface.
func (a tArray) Equal(other Type) bool {
	oa, ok := other.(tArray)
	if !ok {
		return false
	}
	return a.Typ == oa.Typ
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

type tAny struct{}

func (tAny) String() string              { return "anyelement" }
func (tAny) Equal(other Type) bool       { return other == TypeAny }
func (tAny) FamilyEqual(other Type) bool { return other == TypeAny }
func (tAny) Size() (uintptr, bool)       { return unsafe.Sizeof(DString("")), variableSize }
