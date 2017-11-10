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

import "github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"

var (
	// BoolColTypeBool is an immutable ColumnType instance.
	BoolColTypeBool = &BoolColType{Name: "BOOL"}
	// BoolColTypeBoolean is an immutable ColumnType instance.
	BoolColTypeBoolean = &BoolColType{Name: "BOOLEAN"}
	// IntColTypeBit is an immutable ColumnType instance.
	IntColTypeBit = &IntColType{Name: "BIT", Width: 1, ImplicitWidth: true}
	// IntColTypeInt is an immutable ColumnType instance.
	IntColTypeInt = &IntColType{Name: "INT"}
	// IntColTypeInt2 is an immutable ColumnType instance.
	IntColTypeInt2 = &IntColType{Name: "INT2", Width: 16, ImplicitWidth: true}
	// IntColTypeInt4 is an immutable ColumnType instance.
	IntColTypeInt4 = &IntColType{Name: "INT4", Width: 32, ImplicitWidth: true}
	// IntColTypeInt8 is an immutable ColumnType instance.
	IntColTypeInt8 = &IntColType{Name: "INT8"}
	// IntColTypeInt64 is an immutable ColumnType instance.
	IntColTypeInt64 = &IntColType{Name: "INT64"}
	// IntColTypeInteger is an immutable ColumnType instance.
	IntColTypeInteger = &IntColType{Name: "INTEGER"}
	// IntColTypeSmallInt is an immutable ColumnType instance.
	IntColTypeSmallInt = &IntColType{Name: "SMALLINT", Width: 16, ImplicitWidth: true}
	// IntColTypeBigInt is an immutable ColumnType instance.
	IntColTypeBigInt = &IntColType{Name: "BIGINT"}
	// IntColTypeSerial is an immutable ColumnType instance.
	IntColTypeSerial = &IntColType{Name: "SERIAL"}
	// IntColTypeSmallSerial is an immutable ColumnType instance.
	IntColTypeSmallSerial = &IntColType{Name: "SMALLSERIAL"}
	// IntColTypeBigSerial is an immutable ColumnType instance.
	IntColTypeBigSerial = &IntColType{Name: "BIGSERIAL"}

	// FloatColTypeReal is an immutable ColumnType instance.
	FloatColTypeReal = &FloatColType{Name: "REAL", Width: 32}
	// FloatColTypeFloat is an immutable ColumnType instance.
	FloatColTypeFloat = &FloatColType{Name: "FLOAT", Width: 64}
	// FloatColTypeFloat4 is an immutable ColumnType instance.
	FloatColTypeFloat4 = &FloatColType{Name: "FLOAT4", Width: 32}
	// FloatColTypeFloat8 is an immutable ColumnType instance.
	FloatColTypeFloat8 = &FloatColType{Name: "FLOAT8", Width: 64}
	// FloatColTypeDouble is an immutable ColumnType instance.
	FloatColTypeDouble = &FloatColType{Name: "DOUBLE PRECISION", Width: 64}

	// DecimalColTypeDec is an immutable ColumnType instance.
	DecimalColTypeDec = &DecimalColType{Name: "DEC"}
	// DecimalColTypeDecimal is an immutable ColumnType instance.
	DecimalColTypeDecimal = &DecimalColType{Name: "DECIMAL"}
	// DecimalColTypeNumeric is an immutable ColumnType instance.
	DecimalColTypeNumeric = &DecimalColType{Name: "NUMERIC"}

	// DateColTypeDate is an immutable ColumnType instance.
	DateColTypeDate = &DateColType{}

	// TimestampColTypeTimestamp is an immutable ColumnType instance.
	TimestampColTypeTimestamp = &TimestampColType{}
	// TimestampTzColTypeTimestampWithTZ is an immutable ColumnType instance.
	TimestampTzColTypeTimestampWithTZ = &TimestampTZColType{}

	// IntervalColTypeInterval is an immutable ColumnType instance.
	IntervalColTypeInterval = &IntervalColType{}

	// StringColTypeChar is an immutable ColumnType instance.
	StringColTypeChar = &StringColType{Name: "CHAR"}
	// StringColTypeVarChar is an immutable ColumnType instance.
	StringColTypeVarChar = &StringColType{Name: "VARCHAR"}
	// StringColTypeString is an immutable ColumnType instance.
	StringColTypeString = &StringColType{Name: "STRING"}
	// StringColTypeText is an immutable ColumnType instance.
	StringColTypeText = &StringColType{Name: "TEXT"}

	// NameColTypeName is an immutable ColumnType instance.
	NameColTypeName = &NameColType{}

	// BytesColTypeBlob is an immutable ColumnType instance.
	BytesColTypeBlob = &BytesColType{Name: "BLOB"}
	// BytesColTypeBytes is an immutable ColumnType instance.
	BytesColTypeBytes = &BytesColType{Name: "BYTES"}
	// BytesColTypeBytea is an immutable ColumnType instance.
	BytesColTypeBytea = &BytesColType{Name: "BYTEA"}

	// Int2vectorColType is an immutable ColumnType instance.
	Int2vectorColType = &VectorColType{Name: "INT2VECTOR", ParamType: IntColTypeInt}

	// UuidColTypeUUID is an immutable ColumnType instance.
	UuidColTypeUUID = &UUIDColType{}

	// IpnetColTypeINet is an immutable ColumnType instance.
	IpnetColTypeINet = &IPAddrColType{Name: "INET"}

	// JsonColType is an immutable ColumnType instance.
	JsonColType = &JSONColType{Name: "JSON"}
	// JsonbColType is an immutable ColumnType instance.
	JsonbColType = &JSONColType{Name: "JSONB"}

	// OidColTypeOid is an immutable ColumnType instance.
	OidColTypeOid = &OidColType{Name: "OID"}
	// OidColTypeRegClass is an immutable ColumnType instance.
	OidColTypeRegClass = &OidColType{Name: "REGCLASS"}
	// OidColTypeRegNamespace is an immutable ColumnType instance.
	OidColTypeRegNamespace = &OidColType{Name: "REGNAMESPACE"}
	// OidColTypeRegProc is an immutable ColumnType instance.
	OidColTypeRegProc = &OidColType{Name: "REGPROC"}
	// OidColTypeRegProcedure is an immutable ColumnType instance.
	OidColTypeRegProcedure = &OidColType{Name: "REGPROCEDURE"}
	// OidColTypeRegType is an immutable ColumnType instance.
	OidColTypeRegType = &OidColType{Name: "REGTYPE"}
)

var errBitLengthNotPositive = pgerror.NewError(pgerror.CodeInvalidParameterValueError, "length for type bit must be at least 1")

// NewIntBitType creates a type alias for INT named BIT with the given bit width.
func NewIntBitType(width int) (*IntColType, error) {
	if width < 1 {
		return nil, errBitLengthNotPositive
	}
	return &IntColType{Name: "BIT", Width: width}, nil
}

// NewFloatColType creates a type alias for FLOAT with the given precision.
func NewFloatColType(prec int, precSpecified bool) *FloatColType {
	if prec == 0 && !precSpecified {
		return FloatColTypeFloat
	}
	return &FloatColType{Name: "FLOAT", Width: 64, Prec: prec, PrecSpecified: precSpecified}
}

// ArrayOf creates a type alias for an array of the given element type and fixed bounds.
func ArrayOf(colType ColumnType, bounds []int32) (ColumnType, error) {
	if !canBeInArrayColType(colType) {
		return nil, pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError, "arrays of %s not allowed", colType)
	}
	return &ArrayColType{Name: colType.String() + "[]", ParamType: colType, Bounds: bounds}, nil
}
