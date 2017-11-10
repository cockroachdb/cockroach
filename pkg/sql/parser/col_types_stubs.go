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

package parser

import "github.com/cockroachdb/cockroach/pkg/sql/coltypes"

// ColumnType is a temporary alias.
type ColumnType = coltypes.ColumnType

// CastTargetType is a temporary alias.
type CastTargetType = coltypes.CastTargetType

// ArrayColType is a temporary alias.
type ArrayColType = coltypes.ArrayColType

// BoolColType is a temporary alias.
type BoolColType = coltypes.BoolColType

// BytesColType is a temporary alias.
type BytesColType = coltypes.BytesColType

// ColTypeFormatter is a temporary alias.
type ColTypeFormatter = coltypes.ColTypeFormatter

// CollatedStringColType is a temporary alias.
type CollatedStringColType = coltypes.CollatedStringColType

// DateColType is a temporary alias.
type DateColType = coltypes.DateColType

// DecimalColType is a temporary alias.
type DecimalColType = coltypes.DecimalColType

// FloatColType is a temporary alias.
type FloatColType = coltypes.FloatColType

// IPAddrColType is a temporary alias.
type IPAddrColType = coltypes.IPAddrColType

// IntColType is a temporary alias.
type IntColType = coltypes.IntColType

// IntervalColType is a temporary alias.
type IntervalColType = coltypes.IntervalColType

// JSONColType is a temporary alias.
type JSONColType = coltypes.JSONColType

// NameColType is a temporary alias.
type NameColType = coltypes.NameColType

// OidColType is a temporary alias.
type OidColType = coltypes.OidColType

// StringColType is a temporary alias.
type StringColType = coltypes.StringColType

// TimestampColType is a temporary alias.
type TimestampColType = coltypes.TimestampColType

// TimestampTZColType is a temporary alias.
type TimestampTZColType = coltypes.TimestampTZColType

// UUIDColType is a temporary alias.
type UUIDColType = coltypes.UUIDColType

// VectorColType is a temporary alias.
type VectorColType = coltypes.VectorColType

var (
	CastTargetToDatumType             = coltypes.CastTargetToDatumType
	DatumTypeToColumnType             = coltypes.DatumTypeToColumnType
	NewFloatColType                   = coltypes.NewFloatColType
	boolColTypeBool                   = coltypes.BoolColTypeBool
	boolColTypeBoolean                = coltypes.BoolColTypeBoolean
	intColTypeBit                     = coltypes.IntColTypeBit
	intColTypeInt                     = coltypes.IntColTypeInt
	intColTypeInt2                    = coltypes.IntColTypeInt2
	intColTypeInt4                    = coltypes.IntColTypeInt4
	intColTypeInt8                    = coltypes.IntColTypeInt8
	intColTypeInt64                   = coltypes.IntColTypeInt64
	intColTypeInteger                 = coltypes.IntColTypeInteger
	intColTypeSmallInt                = coltypes.IntColTypeSmallInt
	intColTypeBigInt                  = coltypes.IntColTypeBigInt
	intColTypeSerial                  = coltypes.IntColTypeSerial
	intColTypeSmallSerial             = coltypes.IntColTypeSmallSerial
	intColTypeBigSerial               = coltypes.IntColTypeBigSerial
	floatColTypeReal                  = coltypes.FloatColTypeReal
	floatColTypeFloat                 = coltypes.FloatColTypeFloat
	floatColTypeFloat4                = coltypes.FloatColTypeFloat4
	floatColTypeFloat8                = coltypes.FloatColTypeFloat8
	floatColTypeDouble                = coltypes.FloatColTypeDouble
	decimalColTypeDec                 = coltypes.DecimalColTypeDec
	decimalColTypeDecimal             = coltypes.DecimalColTypeDecimal
	decimalColTypeNumeric             = coltypes.DecimalColTypeNumeric
	dateColTypeDate                   = coltypes.DateColTypeDate
	timestampColTypeTimestamp         = coltypes.TimestampColTypeTimestamp
	timestampTzColTypeTimestampWithTZ = coltypes.TimestampTzColTypeTimestampWithTZ
	intervalColTypeInterval           = coltypes.IntervalColTypeInterval
	stringColTypeChar                 = coltypes.StringColTypeChar
	stringColTypeVarChar              = coltypes.StringColTypeVarChar
	stringColTypeString               = coltypes.StringColTypeString
	stringColTypeText                 = coltypes.StringColTypeText
	nameColTypeName                   = coltypes.NameColTypeName
	bytesColTypeBlob                  = coltypes.BytesColTypeBlob
	bytesColTypeBytes                 = coltypes.BytesColTypeBytes
	bytesColTypeBytea                 = coltypes.BytesColTypeBytea
	int2vectorColType                 = coltypes.Int2vectorColType
	uuidColTypeUUID                   = coltypes.UuidColTypeUUID
	ipnetColTypeINet                  = coltypes.IpnetColTypeINet
	jsonColType                       = coltypes.JsonColType
	jsonbColType                      = coltypes.JsonbColType
	oidColTypeOid                     = coltypes.OidColTypeOid
	oidColTypeRegClass                = coltypes.OidColTypeRegClass
	oidColTypeRegNamespace            = coltypes.OidColTypeRegNamespace
	oidColTypeRegProc                 = coltypes.OidColTypeRegProc
	oidColTypeRegProcedure            = coltypes.OidColTypeRegProcedure
	oidColTypeRegType                 = coltypes.OidColTypeRegType
	arrayOf                           = coltypes.ArrayOf
	canBeInArrayColType               = coltypes.CanBeInArrayColType
	newIntBitType                     = coltypes.NewIntBitType
	oidTypeToColType                  = coltypes.OidTypeToColType
	oidColTypeToType                  = coltypes.OidColTypeToType
)
