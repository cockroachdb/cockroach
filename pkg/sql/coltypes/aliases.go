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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

var (
	// Bool is an immutable T instance.
	Bool = &TBool{Name: "BOOL"}
	// Boolean is an immutable T instance.
	Boolean = &TBool{Name: "BOOLEAN"}

	// Bit is an immutable T instance.
	Bit = &TInt{Name: "BIT", Width: 1, ImplicitWidth: true}
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
	Serial = &TInt{Name: "SERIAL"}
	// Serial2 is an immutable T instance.
	Serial2 = &TInt{Name: "SERIAL2"}
	// Serial4 is an immutable T instance.
	Serial4 = &TInt{Name: "SERIAL4"}
	// Serial8 is an immutable T instance.
	Serial8 = &TInt{Name: "SERIAL8"}
	// SmallSerial is an immutable T instance.
	SmallSerial = &TInt{Name: "SMALLSERIAL"}
	// BigSerial is an immutable T instance.
	BigSerial = &TInt{Name: "BIGSERIAL"}

	// Real is an immutable T instance.
	Real = &TFloat{Name: "REAL", Width: 32}
	// Float is an immutable T instance.
	Float = &TFloat{Name: "FLOAT", Width: 64}
	// Float4 is an immutable T instance.
	Float4 = &TFloat{Name: "FLOAT4", Width: 32}
	// Float8 is an immutable T instance.
	Float8 = &TFloat{Name: "FLOAT8", Width: 64}
	// Double is an immutable T instance.
	Double = &TFloat{Name: "DOUBLE PRECISION", Width: 64}

	// Dec is an immutable T instance.
	Dec = &TDecimal{Name: "DEC"}
	// Decimal is an immutable T instance.
	Decimal = &TDecimal{Name: "DECIMAL"}
	// Numeric is an immutable T instance.
	Numeric = &TDecimal{Name: "NUMERIC"}

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

	// Char is an immutable T instance.
	Char = &TString{Name: "CHAR"}
	// VarChar is an immutable T instance.
	VarChar = &TString{Name: "VARCHAR"}
	// String is an immutable T instance.
	String = &TString{Name: "STRING"}
	// Text is an immutable T instance.
	Text = &TString{Name: "TEXT"}

	// Name is an immutable T instance.
	Name = &TName{}

	// Blob is an immutable T instance.
	Blob = &TBytes{Name: "BLOB"}
	// Bytes is an immutable T instance.
	Bytes = &TBytes{Name: "BYTES"}
	// Bytea is an immutable T instance.
	Bytea = &TBytes{Name: "BYTEA"}

	// Int2vector is an immutable T instance.
	Int2vector = &TVector{Name: "INT2VECTOR", ParamType: Int}

	// UUID is an immutable T instance.
	UUID = &TUUID{}

	// INet is an immutable T instance.
	INet = &TIPAddr{Name: "INET"}

	// JSON is an immutable T instance.
	JSON = &TJSON{Name: "JSON"}
	// JSONB is an immutable T instance.
	JSONB = &TJSON{Name: "JSONB"}

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

var errBitLengthNotPositive = pgerror.NewError(pgerror.CodeInvalidParameterValueError, "length for type bit must be at least 1")

// NewIntBitType creates a type alias for INT named BIT with the given bit width.
func NewIntBitType(width int) (*TInt, error) {
	if width < 1 {
		return nil, errBitLengthNotPositive
	}
	return &TInt{Name: "BIT", Width: width}, nil
}

// NewFloat creates a type alias for FLOAT with the given precision.
func NewFloat(prec int, precSpecified bool) *TFloat {
	if prec == 0 && !precSpecified {
		return Float
	}
	return &TFloat{Name: "FLOAT", Width: 64, Prec: prec, PrecSpecified: precSpecified}
}

// ArrayOf creates a type alias for an array of the given element type and fixed bounds.
func ArrayOf(colType T, bounds []int32) (T, error) {
	if !canBeInArrayColType(colType) {
		return nil, pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError, "arrays of %s not allowed", colType)
	}
	return &TArray{Name: colType.String() + "[]", ParamType: colType, Bounds: bounds}, nil
}

var typNameLiterals map[string]T

func init() {
	typNameLiterals = make(map[string]T)
	for _, t := range types.OidToType {
		name := types.PGDisplayName(t)
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
