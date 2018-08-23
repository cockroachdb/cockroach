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

package coltypes

import (
	"strings"

	"github.com/lib/pq/oid"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

var (
	// Bool is an immutable T instance.
	Bool = &TBool{}

	// Int is an immutable T instance.
	Int = &TInt{Name: "INT"}
	// Int2 is an immutable T instance.
	Int2 = &TInt{Name: "INT2", Width: 16, ImplicitWidth: true}
	// Int4 is an immutable T instance.
	Int4 = &TInt{Name: "INT4", Width: 32, ImplicitWidth: true}
	// Int8 is an immutable T instance.
	Int8 = &TInt{Name: "INT8"}
	// Int64 is an immutable T instance.
	Int64 = &TInt{Name: "INT64"}
	// Integer is an immutable T instance.
	Integer = &TInt{Name: "INTEGER"}
	// SmallInt is an immutable T instance.
	SmallInt = &TInt{Name: "SMALLINT", Width: 16, ImplicitWidth: true}
	// BigInt is an immutable T instance.
	BigInt = &TInt{Name: "BIGINT"}
	// Serial is an immutable T instance.
	Serial = &TSerial{IntType: Int}
	// Serial2 is an immutable T instance.
	Serial2 = &TSerial{IntType: Int2}
	// Serial4 is an immutable T instance.
	Serial4 = &TSerial{IntType: Int4}
	// Serial8 is an immutable T instance.
	Serial8 = &TSerial{IntType: Int8}

	// Float4 is an immutable T instance.
	Float4 = &TFloat{Short: true}
	// Float8 is an immutable T instance.
	Float8 = &TFloat{}

	// Decimal is an immutable T instance.
	Decimal = &TDecimal{}

	// Date is an immutable T instance.
	Date = &TDate{}

	// Time is an immutable T instance.
	Time = &TTime{}

	// Timestamp is an immutable T instance.
	Timestamp = &TTimestamp{}
	// TimestampWithTZ is an immutable T instance.
	TimestampWithTZ = &TTimestampTZ{}

	// Interval is an immutable T instance.
	Interval = &TInterval{}

	// Char is an immutable T instance. See strings.go for details.
	Char = &TString{Variant: TStringVariantCHAR, N: 1}
	// VarChar is an immutable T instance. See strings.go for details.
	VarChar = &TString{Variant: TStringVariantVARCHAR}
	// String is an immutable T instance. See strings.go for details.
	String = &TString{Variant: TStringVariantSTRING}
	// QChar is an immutable T instance. See strings.go for details.
	QChar = &TString{Variant: TStringVariantQCHAR}

	// Name is an immutable T instance.
	Name = &TName{}

	// Bytes is an immutable T instance.
	Bytes = &TBytes{}

	// Int2vector is an immutable T instance.
	Int2vector = &TVector{Name: "INT2VECTOR", ParamType: Int}

	// UUID is an immutable T instance.
	UUID = &TUUID{}

	// INet is an immutable T instance.
	INet = &TIPAddr{}

	// JSON is an immutable T instance.
	JSON = &TJSON{}

	// Oid is an immutable T instance.
	Oid = &TOid{Name: "OID"}
	// RegClass is an immutable T instance.
	RegClass = &TOid{Name: "REGCLASS"}
	// RegNamespace is an immutable T instance.
	RegNamespace = &TOid{Name: "REGNAMESPACE"}
	// RegProc is an immutable T instance.
	RegProc = &TOid{Name: "REGPROC"}
	// RegProcedure is an immutable T instance.
	RegProcedure = &TOid{Name: "REGPROCEDURE"}
	// RegType is an immutable T instance.
	RegType = &TOid{Name: "REGTYPE"}

	// OidVector is an immutable T instance.
	OidVector = &TVector{Name: "OIDVECTOR", ParamType: Oid}
)

var errFloatPrecAtLeast1 = pgerror.NewError(pgerror.CodeInvalidParameterValueError,
	"precision for type float must be at least 1 bit")
var errFloatPrecMax54 = pgerror.NewError(pgerror.CodeInvalidParameterValueError,
	"precision for type float must be less than 54 bits")

// NewFloat creates a type alias for FLOAT with the given precision.
func NewFloat(prec int64) (*TFloat, error) {
	if prec < 1 {
		return nil, errFloatPrecAtLeast1
	}
	if prec <= 24 {
		return Float4, nil
	}
	if prec <= 54 {
		return Float8, nil
	}
	return nil, errFloatPrecMax54
}

// ArrayOf creates a type alias for an array of the given element type and fixed bounds.
func ArrayOf(colType T, bounds []int32) (T, error) {
	if !canBeInArrayColType(colType) {
		return nil, pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError, "arrays of %s not allowed", colType)
	}
	return &TArray{ParamType: colType, Bounds: bounds}, nil
}

var typNameLiterals map[string]T

func init() {
	typNameLiterals = make(map[string]T)
	for o, t := range types.OidToType {
		name := strings.ToLower(oid.TypeName[o])
		if _, ok := typNameLiterals[name]; !ok {
			colTyp, err := DatumTypeToColumnType(t)
			if err != nil {
				continue
			}
			typNameLiterals[name] = colTyp
		}
	}
}

// TypeForNonKeywordTypeName returns the column type for the string name of a
// type, if one exists.
func TypeForNonKeywordTypeName(name string) (T, error) {
	if typ, ok := typNameLiterals[name]; ok {
		return typ, nil
	}
	return nil, pgerror.NewError(pgerror.CodeUndefinedObjectError, "type does not exist")
}
