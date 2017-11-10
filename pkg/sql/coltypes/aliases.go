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
	// Pre-allocated immutable boolean column types.
	boolColTypeBool    = &BoolColType{Name: "BOOL"}
	boolColTypeBoolean = &BoolColType{Name: "BOOLEAN"}

	// Pre-allocated immutable integer column types.
	intColTypeBit         = &IntColType{Name: "BIT", Width: 1, ImplicitWidth: true}
	intColTypeInt         = &IntColType{Name: "INT"}
	intColTypeInt2        = &IntColType{Name: "INT2", Width: 16, ImplicitWidth: true}
	intColTypeInt4        = &IntColType{Name: "INT4", Width: 32, ImplicitWidth: true}
	intColTypeInt8        = &IntColType{Name: "INT8"}
	intColTypeInt64       = &IntColType{Name: "INT64"}
	intColTypeInteger     = &IntColType{Name: "INTEGER"}
	intColTypeSmallInt    = &IntColType{Name: "SMALLINT", Width: 16, ImplicitWidth: true}
	intColTypeBigInt      = &IntColType{Name: "BIGINT"}
	intColTypeSerial      = &IntColType{Name: "SERIAL"}
	intColTypeSmallSerial = &IntColType{Name: "SMALLSERIAL"}
	intColTypeBigSerial   = &IntColType{Name: "BIGSERIAL"}

	// Pre-allocated immutable float column types.
	floatColTypeReal   = &FloatColType{Name: "REAL", Width: 32}
	floatColTypeFloat  = &FloatColType{Name: "FLOAT", Width: 64}
	floatColTypeFloat4 = &FloatColType{Name: "FLOAT4", Width: 32}
	floatColTypeFloat8 = &FloatColType{Name: "FLOAT8", Width: 64}
	floatColTypeDouble = &FloatColType{Name: "DOUBLE PRECISION", Width: 64}

	// Pre-allocated immutable decimal column types.
	decimalColTypeDec     = &DecimalColType{Name: "DEC"}
	decimalColTypeDecimal = &DecimalColType{Name: "DECIMAL"}
	decimalColTypeNumeric = &DecimalColType{Name: "NUMERIC"}

	// Pre-allocated immutable date column type.
	dateColTypeDate = &DateColType{}

	// Pre-allocated immutable timestamp column type.
	timestampColTypeTimestamp = &TimestampColType{}

	// Pre-allocated immutable timestamp with time zone column type.
	timestampTzColTypeTimestampWithTZ = &TimestampTZColType{}

	// Pre-allocated immutable interval column type.
	intervalColTypeInterval = &IntervalColType{}

	// Pre-allocated immutable string column types.
	stringColTypeChar    = &StringColType{Name: "CHAR"}
	stringColTypeVarChar = &StringColType{Name: "VARCHAR"}
	stringColTypeString  = &StringColType{Name: "STRING"}
	stringColTypeText    = &StringColType{Name: "TEXT"}

	// Pre-allocated immutable name column type.
	nameColTypeName = &NameColType{}

	// Pre-allocated immutable bytes column types.
	bytesColTypeBlob  = &BytesColType{Name: "BLOB"}
	bytesColTypeBytes = &BytesColType{Name: "BYTES"}
	bytesColTypeBytea = &BytesColType{Name: "BYTEA"}

	// Int2VectorColType represents an INT2VECTOR column type.
	int2vectorColType = &VectorColType{Name: "INT2VECTOR", ParamType: intColTypeInt}

	// Pre-allocated immutable uuid column type.
	uuidColTypeUUID = &UUIDColType{}

	// Pre-allocated immutable IPAddr column type.
	ipnetColTypeINet = &IPAddrColType{Name: "INET"}

	// Pre-allocated immutable JSON column type.
	jsonColType  = &JSONColType{Name: "JSON"}
	jsonbColType = &JSONColType{Name: "JSONB"}

	// Pre-allocated immutable Postgres oid column types.
	oidColTypeOid          = &OidColType{Name: "OID"}
	oidColTypeRegClass     = &OidColType{Name: "REGCLASS"}
	oidColTypeRegNamespace = &OidColType{Name: "REGNAMESPACE"}
	oidColTypeRegProc      = &OidColType{Name: "REGPROC"}
	oidColTypeRegProcedure = &OidColType{Name: "REGPROCEDURE"}
	oidColTypeRegType      = &OidColType{Name: "REGTYPE"}
)

var errBitLengthNotPositive = pgerror.NewError(pgerror.CodeInvalidParameterValueError, "length for type bit must be at least 1")

// newIntBitType creates a type alias for INT named BIT with the given bit width.
func newIntBitType(width int) (*IntColType, error) {
	if width < 1 {
		return nil, errBitLengthNotPositive
	}
	return &IntColType{Name: "BIT", Width: width}, nil
}

// NewFloatColType creates a type alias for FLOAT with the given precision.
func NewFloatColType(prec int, precSpecified bool) *FloatColType {
	if prec == 0 && !precSpecified {
		return floatColTypeFloat
	}
	return &FloatColType{Name: "FLOAT", Width: 64, Prec: prec, PrecSpecified: precSpecified}
}

// arrayOf creates a type alias for an array of the given element type and fixed bounds.
func arrayOf(colType ColumnType, bounds []int32) (ColumnType, error) {
	if !canBeInArrayColType(colType) {
		return nil, pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError, "arrays of %s not allowed", colType)
	}
	return &ArrayColType{Name: colType.String() + "[]", ParamType: colType, Bounds: bounds}, nil
}
